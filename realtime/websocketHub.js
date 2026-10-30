const WebSocket = require('ws');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const authMiddleware = require('../middleware/auth');
const { isTenantWideRole } = require('../utils/accessControl');
const { buildScopedFilter: buildCrmScopedFilter } = require('../utils/crmAccess');
const {
  publishRealtimeEvent,
  subscribeRealtimeEvents
} = require('./realtimeBus');
const {
  subscribeBroadcastEvents
} = require('./broadcastEventBus');
const {
  setUserPresence,
  setTypingState,
  clearTypingState
} = require('./presenceStore');

const buildConversationRealtimeFilter = (context, conversationId) => {
  const filter = { _id: conversationId };
  if (context.companyId) filter.companyId = context.companyId;
  else if (!isTenantWideRole(context.normalizedRole)) return null;
  if (!isTenantWideRole(context.normalizedRole)) {
    const userId = String(context.userId || '');
    filter.$or = [
      { broadcastOwnerId: userId },
      {
        $and: [
          { $or: [{ broadcastOwnerId: null }, { broadcastOwnerId: { $exists: false } }] },
          { $or: [{ userId }, { createdBy: userId }, { assignedTo: userId }, { assignedToId: userId }, { assignedAgent: userId }] }
        ]
      }
    ];
  }
  return filter;
};

const createWebSocketHub = ({
  wss,
  conversationExists = (filter) => Conversation.exists(filter),
  contactExists = (filter) => Contact.exists(filter),
  publishRealtimeEventFn = publishRealtimeEvent,
  subscribeRealtimeEventsFn = subscribeRealtimeEvents,
  subscribeBroadcastEventsFn = subscribeBroadcastEvents,
  setUserPresenceFn = setUserPresence,
  setTypingStateFn = setTypingState,
  clearTypingStateFn = clearTypingState,
  sessionRevalidationIntervalMs = Math.min(
    5 * 60_000,
    Math.max(30_000, Number(process.env.WS_AUTH_REVALIDATION_INTERVAL_MS || 60_000))
  ),
  authenticateToken = authMiddleware.authenticateBearerToken,
  authorizeConversation = async (context, conversationId) => {
    const filter = buildConversationRealtimeFilter(context, conversationId);
    if (!filter) return false;
    return Boolean(await conversationExists(filter));
  }
}) => {
  const clients = new Map();
  const roomMembers = new Map();
  const clientMeta = new Map();
  const crmSubscriptions = new Map();
  const lastPresenceBroadcastByUser = new Map();
  const lastTypingBroadcastByConversationUser = new Map();
  const outboundBatchQueues = new Map();
  const outboundBatchTimers = new Map();
  const OUTBOUND_BATCH_WINDOW_MS = Number(process.env.WS_OUTBOUND_BATCH_WINDOW_MS || 30);
  const OUTBOUND_BATCH_MAX_SIZE = Number(process.env.WS_OUTBOUND_BATCH_MAX_SIZE || 40);

  const getRoomKey = (kind, value) => `${kind}:${String(value || '').trim()}`;
  const getClientKey = (ws, fallbackUserId = '') =>
    ws.__clientKey || String(fallbackUserId || 'anonymous').trim() || 'anonymous';
  const resolveDataCompanyId = (data = {}) =>
    String(
      data?.companyId ||
        data?.conversation?.companyId ||
        data?.message?.companyId ||
        data?.payload?.companyId ||
        ''
    ).trim();
  const resolveDataConversationId = (data = {}) =>
    String(
      data?.conversationId ||
        data?.conversation?._id ||
        data?.message?.conversationId ||
        data?.payload?.conversationId ||
        ''
    ).trim();
  const resolveDataConversationIds = (data = {}, fallbackId = '') =>
    Array.from(
      new Set(
        [
          fallbackId,
          data?.conversationId,
          data?.conversation?._id,
          data?.conversation?.id,
          data?.message?.conversationId,
          data?.payload?.conversationId,
          ...(Array.isArray(data?.relatedConversationIds) ? data.relatedConversationIds : []),
          ...(Array.isArray(data?.message?.relatedConversationIds) ? data.message.relatedConversationIds : []),
          ...(Array.isArray(data?.conversation?.relatedConversationIds)
            ? data.conversation.relatedConversationIds
            : [])
        ]
          .map((id) => String(id || '').trim())
          .filter(Boolean)
      )
    );

  const getSocketSet = (userId) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return null;

    let socketSet = clients.get(normalizedUserId);
    if (!socketSet) {
      socketSet = new Set();
      clients.set(normalizedUserId, socketSet);
    }
    return socketSet;
  };

  const removeSocket = (userId, ws) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId || !ws) return;

    const socketSet = clients.get(normalizedUserId);
    if (!socketSet) return;

    socketSet.delete(ws);
    if (socketSet.size === 0) {
      clients.delete(normalizedUserId);
    }

    const joinedRooms = ws.joinedRooms instanceof Set ? Array.from(ws.joinedRooms) : [];
    joinedRooms.forEach((roomId) => {
      const roomSet = roomMembers.get(roomId);
      if (!roomSet) return;
      roomSet.delete(ws);
      if (roomSet.size === 0) {
        roomMembers.delete(roomId);
        outboundBatchQueues.delete(roomId);
        const timer = outboundBatchTimers.get(roomId);
        if (timer) {
          clearTimeout(timer);
          outboundBatchTimers.delete(roomId);
        }
      }
    });
    if (ws.joinedRooms instanceof Set) {
      ws.joinedRooms.clear();
    }

    lastPresenceBroadcastByUser.delete(normalizedUserId);
    Array.from(lastTypingBroadcastByConversationUser.keys()).forEach((key) => {
      if (String(key || '').endsWith(`:${normalizedUserId}`)) {
        lastTypingBroadcastByConversationUser.delete(key);
      }
    });
  };

  const getRoomSet = (roomId) => {
    const normalizedRoomId = String(roomId || '').trim();
    if (!normalizedRoomId) return null;

    let socketSet = roomMembers.get(normalizedRoomId);
    if (!socketSet) {
      socketSet = new Set();
      roomMembers.set(normalizedRoomId, socketSet);
    }
    return socketSet;
  };

  const joinRoom = (roomId, ws) => {
    const normalizedRoomId = String(roomId || '').trim();
    if (!normalizedRoomId || !ws) return false;

    const socketSet = getRoomSet(normalizedRoomId);
    if (!socketSet) return false;

    socketSet.add(ws);
    if (!(ws.joinedRooms instanceof Set)) {
      ws.joinedRooms = new Set();
    }
    ws.joinedRooms.add(normalizedRoomId);
    return true;
  };

  const leaveRoom = (roomId, ws) => {
    const normalizedRoomId = String(roomId || '').trim();
    if (!normalizedRoomId || !ws) return false;

    const socketSet = roomMembers.get(normalizedRoomId);
    if (!socketSet) return false;

    socketSet.delete(ws);
    if (ws.joinedRooms instanceof Set) {
      ws.joinedRooms.delete(normalizedRoomId);
    }
    if (socketSet.size === 0) {
      roomMembers.delete(normalizedRoomId);
      outboundBatchQueues.delete(normalizedRoomId);
      const timer = outboundBatchTimers.get(normalizedRoomId);
      if (timer) {
        clearTimeout(timer);
        outboundBatchTimers.delete(normalizedRoomId);
      }
    }
    return true;
  };

  const flushOutboundBatch = async (roomId) => {
    const normalizedRoomId = String(roomId || '').trim();
    if (!normalizedRoomId) return false;

    const timer = outboundBatchTimers.get(normalizedRoomId);
    if (timer) {
      clearTimeout(timer);
      outboundBatchTimers.delete(normalizedRoomId);
    }

    const socketSet = roomMembers.get(normalizedRoomId);
    const queue = outboundBatchQueues.get(normalizedRoomId);
    if (!socketSet || socketSet.size === 0 || !queue || queue.length === 0) {
      outboundBatchQueues.delete(normalizedRoomId);
      return false;
    }

    const events = queue.splice(0, queue.length);
    const payload =
      events.length === 1
        ? events[0]
        : {
            type: 'realtime_batch',
            room: normalizedRoomId,
            events,
            timestamp: new Date().toISOString()
          };
    const message = JSON.stringify(payload);

    socketSet.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });

    if (queue.length === 0) {
      outboundBatchQueues.delete(normalizedRoomId);
    }
    return true;
  };

  const sendToLocalRoom = (roomId, data) => {
    const normalizedRoomId = String(roomId || '').trim();
    const socketSet = roomMembers.get(normalizedRoomId);
    if (!socketSet || socketSet.size === 0) return;

    let queue = outboundBatchQueues.get(normalizedRoomId);
    if (!queue) {
      queue = [];
      outboundBatchQueues.set(normalizedRoomId, queue);
    }

    queue.push(data);
    if (queue.length >= OUTBOUND_BATCH_MAX_SIZE) {
      void flushOutboundBatch(normalizedRoomId).catch((error) => {
        console.error('Failed to flush outbound websocket batch:', error?.message || error);
      });
      return;
    }

    if (!outboundBatchTimers.has(normalizedRoomId)) {
      const timer = setTimeout(() => {
        outboundBatchTimers.delete(normalizedRoomId);
        void flushOutboundBatch(normalizedRoomId).catch((error) => {
          console.error('Failed to flush outbound websocket batch:', error?.message || error);
        });
      }, OUTBOUND_BATCH_WINDOW_MS);

      if (typeof timer.unref === 'function') {
        timer.unref();
      }

      outboundBatchTimers.set(normalizedRoomId, timer);
    }
  };

  const sendJson = (ws, data) => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  };

  const sendToLocalUser = (userId, data) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;
    sendToLocalRoom(getRoomKey('user', normalizedUserId), data);
  };

  const syncConversationRoom = (ws, nextConversationId = '') => {
    if (!(ws.joinedRooms instanceof Set)) {
      ws.joinedRooms = new Set();
    }

    const normalizedNextConversationId = String(nextConversationId || '').trim();
    const currentConversationId = String(ws.activeConversationId || '').trim();
    if (currentConversationId && currentConversationId !== normalizedNextConversationId) {
      leaveRoom(getRoomKey('conversation', currentConversationId), ws);
    }

    ws.activeConversationId = normalizedNextConversationId;
    if (normalizedNextConversationId) {
      joinRoom(getRoomKey('conversation', normalizedNextConversationId), ws);
    }
  };

  const syncCompanyRoom = (ws, companyId = '') => {
    if (!(ws.joinedRooms instanceof Set)) {
      ws.joinedRooms = new Set();
    }

    const normalizedCompanyId = String(companyId || '').trim();
    const currentCompanyId = String(ws.companyId || '').trim();
    if (currentCompanyId && currentCompanyId !== normalizedCompanyId) {
      leaveRoom(getRoomKey('company', currentCompanyId), ws);
    }

    ws.companyId = normalizedCompanyId;
    if (normalizedCompanyId) {
      joinRoom(getRoomKey('company', normalizedCompanyId), ws);
    }
  };

  const deliverEvent = (event = {}) => {
    const scope = String(event?.scope || '').trim().toLowerCase();
    if (scope === 'broadcast') {
      sendToLocalRoom('global-admins', event.data || {});
      return;
    }

    if (scope === 'user') {
      const targetUserId = String(event?.userId || '').trim();
      if (targetUserId) {
        sendToLocalUser(targetUserId, event.data || {});
      }
      resolveDataConversationIds(event?.data || {}, event?.conversationId).forEach((conversationId) => {
        sendToLocalRoom(getRoomKey('conversation', conversationId), event.data || {});
      });
      return;
    }

    if (scope === 'company') {
      const targetCompanyId = String(event?.companyId || resolveDataCompanyId(event?.data || {})).trim();
      const data = event?.data || {};
      const targetedUserId = String(event?.userId || data?.userId || '').trim();
      if (targetedUserId) {
        sendToLocalUser(targetedUserId, data);
      } else if (
        targetCompanyId &&
        (data.type === 'user_list' ||
          (data.type === 'team_inbox_cache_invalidated' && !data.conversationId && !data.conversationIds?.length))
      ) {
        sendToLocalRoom(getRoomKey('company', targetCompanyId), event.data || {});
      }
      return;
    }

    if (scope === 'conversation') {
      resolveDataConversationIds(event?.data || {}, event?.conversationId).forEach((conversationId) => {
        sendToLocalRoom(getRoomKey('conversation', conversationId), event.data || {});
      });
      return;
    }

    if (scope === 'room') {
      const targetRoom = String(event?.room || '').trim();
      if (targetRoom) {
        sendToLocalRoom(targetRoom, event.data || {});
      }
    }
  };

  subscribeRealtimeEventsFn((event) => {
    deliverEvent(event);
  });

  subscribeBroadcastEventsFn((event) => {
    const targetUserId = String(event?.userId || '').trim();
    const payload = event?.payload || {};

    if (targetUserId) {
      void sendToUser(targetUserId, payload).catch((error) => {
        console.error('Failed to deliver broadcast event to user:', error?.message || error);
      });
      return;
    }

    void broadcast(payload).catch((error) => {
      console.error('Failed to broadcast global broadcast-event payload:', error?.message || error);
    });
  });

  const broadcast = async (data) => {
    if (String(process.env.WS_ENABLE_GLOBAL_BROADCASTS || '').toLowerCase() !== 'true') {
      throw new Error('Global WebSocket broadcasts are disabled');
    }
    const payload = {
      scope: 'room',
      room: 'global-admins',
      data
    };
    sendToLocalRoom('global-admins', data);
    await publishRealtimeEventFn(payload);
  };

  const sendToUser = async (userId, data) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;

    const companyId = resolveDataCompanyId(data || {});
    const conversationId = resolveDataConversationId(data || {});
    const payload = {
      scope: 'user',
      userId: normalizedUserId,
      companyId: companyId || null,
      conversationId: conversationId || null,
      data
    };

    sendToLocalUser(normalizedUserId, data);
    resolveDataConversationIds(data || {}, conversationId).forEach((targetConversationId) => {
      sendToLocalRoom(getRoomKey('conversation', targetConversationId), data);
    });
    await publishRealtimeEventFn(payload);
  };

  const emitRealtimeEvent = async (userId, data) => {
    if (userId) {
      await sendToUser(String(userId), data);
      return;
    }
    await broadcast(data);
  };

  const broadcastUserList = () => {
    const seen = new Map();

    clientMeta.forEach((meta, clientKey) => {
      const userId = String(meta?.userId || clientKey || '').trim();
      if (!userId) return;

      const previous = seen.get(userId) || {};
      const displayName = String(meta?.displayName || previous.displayName || '').trim();
      const email = String(meta?.email || previous.email || '').trim();
      const companyId = String(meta?.companyId || previous.companyId || '').trim();

      seen.set(userId, {
        userId,
        id: userId,
        _id: userId,
        displayName: displayName || previous.displayName || '',
        name: displayName || previous.name || '',
        email,
        companyId,
        connected: true,
        lastSeenAt: meta?.lastSeenAt || previous.lastSeenAt || null
      });
    });

    const usersByCompany = new Map();
    seen.forEach((user) => {
      if (!user.companyId) return;
      if (!usersByCompany.has(user.companyId)) usersByCompany.set(user.companyId, []);
      usersByCompany.get(user.companyId).push(user);
    });
    usersByCompany.forEach((users, companyId) => {
      sendToLocalRoom(getRoomKey('company', companyId), { type: 'user_list', users });
    });
    const globalAdmins = Array.from(seen.values()).filter((user) => !user.companyId);
    if (globalAdmins.length) {
      clientMeta.forEach((meta) => {
        if (!meta.companyId && isTenantWideRole(meta.role)) {
          sendJson(meta.ws, { type: 'user_list', users: Array.from(seen.values()) });
        }
      });
    }
  };

  const broadcastCrmChannel = (channel, data, exceptKey = '') => {
    const subscribers = crmSubscriptions.get(channel);
    if (!subscribers || subscribers.size === 0) return;

    subscribers.forEach((clientKey) => {
      if (exceptKey && clientKey === exceptKey) return;
      const meta = clientMeta.get(clientKey);
      sendJson(meta?.ws, data);
    });
  };

  const removeClientFromCrmChannels = (clientKey) => {
    crmSubscriptions.forEach((subscribers, channel) => {
      if (!subscribers.delete(clientKey)) return;
      if (subscribers.size === 0) {
        crmSubscriptions.delete(channel);
        return;
      }

      broadcastCrmChannel(
        channel,
        {
          type: 'crm_presence_leave',
          scope: 'crm',
          channel,
          userId: clientMeta.get(clientKey)?.userId || clientKey,
          timestamp: new Date().toISOString()
        },
        clientKey
      );
    });
  };

  const publishPresenceUpdate = async ({
    userId,
    companyId,
    online,
    socketCount,
    lastSeen,
    activeConversationId,
    role
  }) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;

    if (!companyId && !isTenantWideRole(role)) return false;
    const payload = {
      scope: companyId ? 'company' : 'room',
      ...(!companyId ? { room: 'global-admins' } : {}),
      companyId: String(companyId || '').trim() || null,
      data: {
        type: 'presence:update',
        userId: normalizedUserId,
        online: Boolean(online),
        socketCount: Math.max(0, Number(socketCount) || 0),
        lastSeen: lastSeen || new Date().toISOString(),
        activeConversationId: String(activeConversationId || '').trim() || null
      }
    };

    const signature = JSON.stringify({
      companyId: payload.companyId || '',
      online: payload.data.online,
      socketCount: payload.data.socketCount,
      activeConversationId: payload.data.activeConversationId || ''
    });
    const lastPresence = lastPresenceBroadcastByUser.get(normalizedUserId);
    if (lastPresence && lastPresence.signature === signature) {
      return payload;
    }
    lastPresenceBroadcastByUser.set(normalizedUserId, {
      signature,
      updatedAt: Date.now()
    });

    if (payload.companyId) {
      sendToLocalRoom(getRoomKey('company', payload.companyId), payload.data);
    } else {
      sendToLocalRoom('global-admins', payload.data);
    }
    await publishRealtimeEventFn(payload);
  };

  const publishTypingUpdate = async ({
    userId,
    conversationId,
    companyId,
    isTyping,
    displayName
  }) => {
    const normalizedUserId = String(userId || '').trim();
    const normalizedConversationId = String(conversationId || '').trim();
    if (!normalizedUserId || !normalizedConversationId) return;

    const payload = {
      scope: 'conversation',
      companyId: String(companyId || '').trim() || null,
      conversationId: normalizedConversationId,
      data: {
        type: 'typing:update',
        userId: normalizedUserId,
        conversationId: normalizedConversationId,
        isTyping: Boolean(isTyping),
        displayName: String(displayName || '').trim() || null
      }
    };

    const signature = JSON.stringify({
      companyId: payload.companyId || '',
      isTyping: payload.data.isTyping,
      displayName: payload.data.displayName || ''
    });
    const typingKey = `${normalizedConversationId}:${normalizedUserId}`;
    const lastTyping = lastTypingBroadcastByConversationUser.get(typingKey);
    if (lastTyping && lastTyping.signature === signature) {
      return payload;
    }
    lastTypingBroadcastByConversationUser.set(typingKey, {
      signature,
      updatedAt: Date.now()
    });

    if (payload.conversationId) {
      sendToLocalRoom(getRoomKey('conversation', payload.conversationId), payload.data);
    }
    await publishRealtimeEventFn(payload);
  };

  const rejectSocket = (ws, { code = 1008, reason = 'Authentication failed', type = 'auth_error', error = 'Authentication required' } = {}) => {
    if (ws.authState === 'rejected') return;
    ws.authState = 'rejected';
    ['authTimeoutTimer', 'authExpiryTimer', 'authRevalidationTimer'].forEach((key) => {
      if (ws[key]) clearTimeout(ws[key]);
      if (ws[key]) clearInterval(ws[key]);
      ws[key] = null;
    });
    if (ws.readyState === WebSocket.OPEN) sendJson(ws, { type, error });
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close(code, reason);
  };

  const scheduleTokenExpiry = (ws, exp) => {
    if (ws.authExpiryTimer) clearTimeout(ws.authExpiryTimer);
    if (!exp) return;
    const expiresInMs = exp * 1000 - Date.now();
    if (expiresInMs <= 0) {
      rejectSocket(ws, { reason: 'Authentication expired', error: 'Invalid or expired session' });
      return;
    }
    ws.authExpiryTimer = setTimeout(() => {
      rejectSocket(ws, { reason: 'Authentication expired', error: 'Invalid or expired session' });
    }, expiresInMs);
    ws.authExpiryTimer.unref?.();
  };

  const startSessionRevalidation = (ws) => {
    ws.authRevalidationTimer = setInterval(async () => {
      if (ws.authState !== 'authenticated' || ws.authRevalidationInProgress) return;
      ws.authRevalidationInProgress = true;
      try {
        const latest = await authenticateToken(ws.authToken);
        if (ws.authState !== 'authenticated') return;
        if (
          String(latest.userId || '') !== String(ws.authContext.userId || '') ||
          String(latest.companyId || '') !== String(ws.authContext.companyId || '')
        ) {
          rejectSocket(ws, { reason: 'Session identity changed', error: 'Session is no longer valid' });
          return;
        }
        ws.authContext = latest;
        scheduleTokenExpiry(ws, latest.exp);
      } catch (error) {
        if (error?.authFailure === 'transient') {
          rejectSocket(ws, {
            code: 1013,
            reason: 'Authentication service temporarily unavailable',
            type: 'auth_unavailable',
            error: 'Authentication service temporarily unavailable'
          });
        } else {
          rejectSocket(ws, { reason: 'Session revoked or expired', error: 'Session is no longer valid' });
        }
      } finally {
        ws.authRevalidationInProgress = false;
      }
    }, sessionRevalidationIntervalMs);
    ws.authRevalidationTimer.unref?.();
  };

  const handleWebSocketMessage = async (data, userId, ws) => {
    if (ws.authState !== 'authenticated') {
      if (ws.authState !== 'unauthenticated') {
        rejectSocket(ws, { error: 'Duplicate or premature authentication event' });
        return;
      }
      if (data?.type !== 'authenticate' || typeof data.token !== 'string') {
        rejectSocket(ws, { error: 'Authentication required before application events' });
        return;
      }
      if (data.token.length > 16_384) {
        rejectSocket(ws, { code: 1009, reason: 'Authentication frame too large', error: 'Authentication frame too large' });
        return;
      }
      ws.authState = 'authenticating';
      try {
        const context = await authenticateToken(data.token);
        if (ws.readyState !== WebSocket.OPEN || ws.authState !== 'authenticating') return;
        if (!context?.userId) throw new Error('Invalid auth context');
        const companyId = String(context.companyId || '').trim();
        if (!companyId && !isTenantWideRole(context.normalizedRole)) {
          throw new Error('Authenticated tenant scope is missing');
        }
        scheduleTokenExpiry(ws, context.exp);
        if (ws.authState !== 'authenticating') return;
        userId = String(context.userId);
        ws.lastSeenAt = new Date();
        await setUserPresenceFn({ userId, online: true, socketCount: (clients.get(userId)?.size || 0) + 1, lastSeen: ws.lastSeenAt, companyId });
        await publishPresenceUpdate({ userId, companyId, online: true, socketCount: (clients.get(userId)?.size || 0) + 1, lastSeen: ws.lastSeenAt.toISOString(), role: context.normalizedRole });
        if (ws.readyState !== WebSocket.OPEN || ws.authState !== 'authenticating') return;

        if (ws.authTimeoutTimer) clearTimeout(ws.authTimeoutTimer);
        ws.authContext = context;
        ws.authToken = data.token;
        ws.userId = userId;
        ws.companyId = companyId;
        ws.__clientKey = `${userId}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
        ws.authState = 'authenticated';
        const nextSocketSet = getSocketSet(userId);
        nextSocketSet?.add(ws);
        clientMeta.set(getClientKey(ws, userId), {
          ws, userId, companyId,
          displayName: context.username || null,
          email: context.email || null,
          role: context.normalizedRole || context.role || null,
          lastSeenAt: ws.lastSeenAt.toISOString()
        });
        joinRoom(getRoomKey('user', userId), ws);
        if (companyId) syncCompanyRoom(ws, companyId);
        else if (isTenantWideRole(context.normalizedRole)) joinRoom('global-admins', ws);
        startSessionRevalidation(ws);
        sendJson(ws, { type: 'authenticated', userId, companyId: companyId || null });
        broadcastUserList();
      } catch (error) {
        if (ws.authState !== 'authenticating') return;
        console.warn('WebSocket authentication rejected', { reason: error?.authFailure || error?.name || 'AuthError' });
        if (error?.authFailure === 'transient') {
          rejectSocket(ws, {
            code: 1013,
            reason: 'Authentication service temporarily unavailable',
            type: 'auth_unavailable',
            error: 'Authentication service temporarily unavailable'
          });
        } else {
          rejectSocket(ws, { error: 'Invalid or expired session' });
        }
      }
      return;
    }
    if (data?.type === 'authenticate' || data?.type === 'identify') {
      rejectSocket(ws, { error: 'Identity is established by the authenticated session' });
      return;
    }
    userId = ws.authContext.userId;
    switch (data.type) {
      case 'identify': {
        break;
      }
      case 'presence:ping': {
        const activeConversationId = String(data.conversationId || data.activeConversationId || '').trim();
        const companyId = String(ws.authContext.companyId || '').trim();
        ws.lastSeenAt = new Date();
        if (activeConversationId) {
          if (await authorizeConversation(ws.authContext, activeConversationId)) syncConversationRoom(ws, activeConversationId);
        }

        const socketSet = clients.get(String(userId || '').trim());
        const socketCount = socketSet ? socketSet.size : 1;
        await setUserPresenceFn({
          userId,
          online: true,
          socketCount,
          lastSeen: ws.lastSeenAt,
          activeConversationId: ws.activeConversationId || ''
        });
        await publishPresenceUpdate({
          userId,
          companyId: ws.companyId || '',
          online: true,
          socketCount,
          lastSeen: ws.lastSeenAt.toISOString(),
          activeConversationId: ws.activeConversationId || ''
        });
        try {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
              type: 'pong',
              timestamp: Date.now(),
              userId: String(userId || '').trim() || null
            }));
          }
        } catch (error) {
          console.error('Failed to send pong response:', error.message);
        }
        break;
      }
      case 'ping': {
        await handleWebSocketMessage({ ...data, type: 'presence:ping' }, userId, ws);
        break;
      }
      case 'conversation:subscribe': {
        const conversationId = String(data.conversationId || '').trim();
        if (conversationId && await authorizeConversation(ws.authContext, conversationId)) {
          syncConversationRoom(ws, conversationId);
          sendJson(ws, { type: 'conversation:subscribed', conversationId });
        } else {
          sendJson(ws, { type: 'conversation:subscription_rejected', conversationId });
        }
        break;
      }
      case 'conversation:unsubscribe': {
        const conversationId = String(data.conversationId || '').trim();
        if (conversationId && String(ws.activeConversationId || '').trim() === conversationId) {
          leaveRoom(getRoomKey('conversation', conversationId), ws);
          ws.activeConversationId = '';
        }
        break;
      }
      case 'crm_subscribe': {
        const channel = String(data.channel || data.contactId || 'crm').trim() || 'crm';
        if (channel !== 'crm') {
          const contactId = channel.startsWith('contact:') ? channel.slice('contact:'.length) : channel;
          if (data.contactId && String(data.contactId).trim() !== contactId) {
            sendJson(ws, { type: 'crm_subscription_rejected', channel });
            break;
          }
          const contactFilter = buildCrmScopedFilter({
            user: {
              id: ws.authContext.userId,
              role: ws.authContext.role,
              companyRole: ws.authContext.companyRole,
              normalizedRole: ws.authContext.normalizedRole
            },
            companyId: ws.authContext.companyId
          }, { _id: contactId });
          if (!(await contactExists(contactFilter))) {
            sendJson(ws, { type: 'crm_subscription_rejected', channel });
            break;
          }
        } else if (ws.authContext.companyId || !isTenantWideRole(ws.authContext.normalizedRole)) {
          sendJson(ws, { type: 'crm_subscription_rejected', channel });
          break;
        }
        if (!crmSubscriptions.has(channel)) crmSubscriptions.set(channel, new Set());
        crmSubscriptions.get(channel).add(getClientKey(ws, userId));
        sendJson(ws, {
          type: 'crm_subscribed',
          scope: 'crm',
          channel,
          timestamp: new Date().toISOString()
        });
        break;
      }
      case 'crm_unsubscribe': {
        const channel = String(data.channel || data.contactId || 'crm').trim() || 'crm';
        const subscribers = crmSubscriptions.get(channel);
        if (subscribers) {
          subscribers.delete(getClientKey(ws, userId));
          if (subscribers.size === 0) crmSubscriptions.delete(channel);
        }
        sendJson(ws, {
          type: 'crm_unsubscribed',
          scope: 'crm',
          channel,
          timestamp: new Date().toISOString()
        });
        break;
      }
      case 'crm_presence_viewing':
      case 'crm_presence_editing':
      case 'crm_presence_leave': {
        const channel = String(data.channel || data.contactId || 'crm').trim() || 'crm';
        const clientKey = getClientKey(ws, userId);
        const subscribers = crmSubscriptions.get(channel);
        if (!subscribers?.has(clientKey)) break;

        if (data.type === 'crm_presence_leave') {
          if (subscribers) {
            subscribers.delete(clientKey);
            if (subscribers.size === 0) crmSubscriptions.delete(channel);
          }
        } else {
          if (!crmSubscriptions.has(channel)) crmSubscriptions.set(channel, new Set());
          crmSubscriptions.get(channel).add(clientKey);
        }

        broadcastCrmChannel(
          channel,
          {
            ...data,
            scope: 'crm',
            channel,
            userId: clientMeta.get(clientKey)?.userId || userId,
            timestamp: new Date().toISOString()
          },
          clientKey
        );
        sendJson(ws, {
          type: 'crm_mutation_ack',
          scope: 'crm',
          channel,
          requestId: data.requestId || '',
          timestamp: new Date().toISOString()
        });
        break;
      }
      case 'typing':
      case 'typing:start':
      case 'typing:stop': {
        const conversationId = String(data.conversationId || '').trim();
        const isTyping =
          data.type === 'typing:start' ? true : data.type === 'typing:stop' ? false : Boolean(data.isTyping);
        const displayName = String(clientMeta.get(getClientKey(ws, userId))?.displayName || '').trim();
        const companyId = String(ws.authContext.companyId || '').trim();

        ws.lastSeenAt = new Date();
        if (!conversationId || !(await authorizeConversation(ws.authContext, conversationId))) break;

        await setTypingStateFn({
          userId,
          conversationId,
          isTyping,
          displayName
        });
        await publishTypingUpdate({
          userId,
          conversationId,
          companyId,
          isTyping,
          displayName
        });
        break;
      }
      default:
        console.log(`Unknown message type from ${userId}:`, data.type);
    }
  };

  wss.on('connection', (ws) => {
    console.log('WebSocket connection established');
    let userId = '';
    ws.__clientKey = `pending:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    ws.lastSeenAt = new Date();
    ws.authState = 'unauthenticated';
    ws.activeConversationId = '';
    ws.joinedRooms = ws.joinedRooms instanceof Set ? ws.joinedRooms : new Set();
    ws.authTimeoutTimer = setTimeout(() => {
      rejectSocket(ws, { reason: 'Authentication timeout', error: 'Authentication required' });
    }, 30_000);
    ws.authTimeoutTimer.unref?.();

    ws.on('message', (message) => {
      const messageBytes = Buffer.isBuffer(message) ? message.length : Buffer.byteLength(String(message || ''));
      if (messageBytes > 256 * 1024) {
        rejectSocket(ws, { code: 1009, reason: 'WebSocket frame too large', error: 'WebSocket frame too large' });
        return;
      }
      try {
        const data = JSON.parse(message);
        handleWebSocketMessage(data, userId, ws).then(() => {
          if (ws.authContext) userId = ws.authContext.userId;
        }).catch((error) => {
          console.error('WebSocket message handler failed:', error);
        });
      } catch (error) {
        rejectSocket(ws, { code: 1007, reason: 'Malformed WebSocket frame', error: 'Malformed WebSocket frame' });
      }
    });

    ws.on('close', () => {
        if (ws.authTimeoutTimer) clearTimeout(ws.authTimeoutTimer);
        if (ws.authExpiryTimer) clearTimeout(ws.authExpiryTimer);
        if (ws.authRevalidationTimer) clearInterval(ws.authRevalidationTimer);
        if (!ws.authContext) return;
        const authenticatedContext = ws.authContext;
        const authenticatedCompanyId = String(authenticatedContext.companyId || '').trim();
        removeSocket(userId, ws);
        const clientKey = getClientKey(ws, userId);
        removeClientFromCrmChannels(clientKey);
        clientMeta.delete(clientKey);
        const socketCount = clients.get(userId)?.size || 0;
        const lastSeenAt = ws.lastSeenAt || new Date();

        setUserPresenceFn({
          userId,
          online: socketCount > 0,
          socketCount,
          lastSeen: lastSeenAt,
          activeConversationId: ws.activeConversationId || '',
          companyId: authenticatedCompanyId
        }).catch((error) => {
          console.error('Failed to update presence on close:', error.message);
        });

      if (socketCount === 0) {
        clearTypingStateFn({
          userId,
          conversationId: ws.activeConversationId || ''
        }).catch((error) => {
          console.error('Failed to clear typing state on close:', error.message);
        });
      }

      console.log(`Client disconnected: ${userId}`);
      broadcastUserList();
      publishPresenceUpdate({
        userId,
        online: socketCount > 0,
        socketCount,
        lastSeen: lastSeenAt.toISOString(),
        activeConversationId: ws.activeConversationId || '',
        companyId: authenticatedCompanyId,
        role: authenticatedContext.normalizedRole
      }).catch((error) => {
        console.error('Failed to publish presence close event:', error.message);
      });
    });
  });

  return {
    clients,
    broadcast,
    sendToUser,
    emitRealtimeEvent
  };
};

module.exports = {
  createWebSocketHub,
  buildConversationRealtimeFilter
};
