const WebSocket = require('ws');
const {
  publishRealtimeEvent,
  subscribeRealtimeEvents
} = require('./realtimeBus');
const {
  subscribeBroadcastEvents
} = require('./broadcastEventBus');
const {
  setUserPresence,
  clearUserPresence,
  setTypingState,
  clearTypingState
} = require('./presenceStore');

const createWebSocketHub = ({ wss }) => {
  const clients = new Map();
  const roomMembers = new Map();
  const clientMeta = new Map();
  const crmSubscriptions = new Map();
  const lastPresenceBroadcastByUser = new Map();
  const lastTypingBroadcastByConversationUser = new Map();

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
    }
    return true;
  };

  const sendToLocalRoom = (roomId, data) => {
    const socketSet = roomMembers.get(String(roomId || '').trim());
    if (!socketSet || socketSet.size === 0) return;

    const message = JSON.stringify(data);
    socketSet.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
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

  const broadcastLocal = (data) => {
    const message = JSON.stringify(data);
    clients.forEach((socketSet) => {
      socketSet.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(message);
        }
      });
    });
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
      broadcastLocal(event.data || {});
      return;
    }

    if (scope === 'user') {
      const targetUserId = String(event?.userId || '').trim();
      if (targetUserId) {
        sendToLocalUser(targetUserId, event.data || {});
      }
      const targetCompanyId = resolveDataCompanyId(event?.data || {});
      if (targetCompanyId) {
        sendToLocalRoom(getRoomKey('company', targetCompanyId), event.data || {});
      }
      const targetConversationId = resolveDataConversationId(event?.data || {});
      if (targetConversationId) {
        sendToLocalRoom(getRoomKey('conversation', targetConversationId), event.data || {});
      }
      return;
    }

    if (scope === 'company') {
      const targetCompanyId = String(event?.companyId || resolveDataCompanyId(event?.data || {})).trim();
      if (targetCompanyId) {
        sendToLocalRoom(getRoomKey('company', targetCompanyId), event.data || {});
      }
      return;
    }

    if (scope === 'conversation') {
      const targetConversationId = String(event?.conversationId || resolveDataConversationId(event?.data || {})).trim();
      if (targetConversationId) {
        sendToLocalRoom(getRoomKey('conversation', targetConversationId), event.data || {});
      }
      return;
    }

    if (scope === 'room') {
      const targetRoom = String(event?.room || '').trim();
      if (targetRoom) {
        sendToLocalRoom(targetRoom, event.data || {});
      }
    }
  };

  subscribeRealtimeEvents((event) => {
    deliverEvent(event);
  });

  subscribeBroadcastEvents((event) => {
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
    const payload = {
      scope: 'broadcast',
      data
    };
    broadcastLocal(data);
    await publishRealtimeEvent(payload);
  };

  const sendToUser = async (userId, data) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;

    const companyId = resolveDataCompanyId(data || {});
    if (companyId) {
      const payload = {
        scope: 'company',
        companyId,
        data
      };
      sendToLocalRoom(getRoomKey('company', companyId), data);
      await publishRealtimeEvent(payload);
      return;
    }

    const payload = {
      scope: 'user',
      userId: normalizedUserId,
      data
    };
    sendToLocalUser(normalizedUserId, data);
    await publishRealtimeEvent(payload);
  };

  const emitRealtimeEvent = async (userId, data) => {
    if (userId) {
      await sendToUser(String(userId), data);
      return;
    }
    await broadcast(data);
  };

  const broadcastUserList = () => {
    const userList = Array.from(clients.keys());
    broadcastLocal({ type: 'user_list', users: userList });
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
    activeConversationId
  }) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;

    const payload = {
      scope: companyId ? 'company' : 'broadcast',
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
      broadcastLocal(payload.data);
    }
    await publishRealtimeEvent(payload);
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
      scope: companyId ? 'conversation' : 'broadcast',
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
    await publishRealtimeEvent(payload);
  };

  const handleWebSocketMessage = async (data, userId, ws) => {
    switch (data.type) {
      case 'identify': {
        if (!data.userId) return;
        removeSocket(userId, ws);
        const nextUserId = String(data.userId || '').trim();
        const nextSocketSet = getSocketSet(nextUserId);
        if (!nextSocketSet) return;

        nextSocketSet.add(ws);
        ws.userId = nextUserId;
        clientMeta.set(getClientKey(ws, nextUserId), {
          ws,
          userId: nextUserId,
          companyId: String(data.companyId || ws.companyId || '').trim()
        });
        joinRoom(getRoomKey('user', nextUserId), ws);
        syncCompanyRoom(ws, data.companyId || ws.companyId || '');
        syncConversationRoom(ws, data.activeConversationId || ws.activeConversationId || '');
        ws.lastSeenAt = new Date();
        console.log(`Client identified: ${nextUserId}`);

        const socketCount = nextSocketSet.size;
        await setUserPresence({
          userId: nextUserId,
          online: true,
          socketCount,
          lastSeen: ws.lastSeenAt,
          activeConversationId: ws.activeConversationId || '',
          companyId: ws.companyId || ''
        });
        await publishPresenceUpdate({
          userId: nextUserId,
          companyId: ws.companyId || '',
          online: true,
          socketCount,
          lastSeen: ws.lastSeenAt.toISOString(),
          activeConversationId: ws.activeConversationId || ''
        });
        broadcastUserList();
        break;
      }
      case 'presence:ping': {
        const activeConversationId = String(data.conversationId || data.activeConversationId || '').trim();
        const companyId = String(data.companyId || ws.companyId || '').trim();
        ws.lastSeenAt = new Date();
        if (companyId) {
          syncCompanyRoom(ws, companyId);
        }
        if (activeConversationId) {
          syncConversationRoom(ws, activeConversationId);
        }

        const socketSet = clients.get(String(userId || '').trim());
        const socketCount = socketSet ? socketSet.size : 1;
        await setUserPresence({
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
        if (conversationId) {
          syncConversationRoom(ws, conversationId);
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
        const displayName = String(data.displayName || data.userName || '').trim();
        const companyId = String(data.companyId || ws.companyId || '').trim();

        ws.lastSeenAt = new Date();
        if (conversationId) {
          syncConversationRoom(ws, conversationId);
        }

        await setTypingState({
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

  wss.on('connection', (ws, req) => {
    console.log('WebSocket connection established');
    const initialUserId = String(req.headers['user-id'] || 'anonymous').trim() || 'anonymous';
    let userId = initialUserId;
    ws.__clientKey = `${initialUserId}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    ws.lastSeenAt = new Date();
    ws.activeConversationId = '';

    console.log('User ID:', userId);
    getSocketSet(userId)?.add(ws);
    ws.joinedRooms = ws.joinedRooms instanceof Set ? ws.joinedRooms : new Set();
    joinRoom(getRoomKey('user', userId), ws);
    clientMeta.set(getClientKey(ws, userId), { ws, userId, companyId: '' });

    console.log('Total connected users:', clients.size);

    broadcastUserList();

    setUserPresence({
        userId,
        online: true,
        socketCount: clients.get(userId)?.size || 1,
        lastSeen: ws.lastSeenAt,
        activeConversationId: '',
        companyId: ws.companyId || ''
      }).catch((error) => {
        console.error('Failed to mark presence online:', error.message);
      });

    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        console.log('WebSocket message received:', data);
        handleWebSocketMessage(data, userId, ws).catch((error) => {
          console.error('WebSocket message handler failed:', error);
        });
        if (ws.userId) {
          userId = ws.userId;
        }
      } catch (error) {
        console.error('WebSocket message parse error:', error);
      }
    });

    ws.on('close', () => {
        removeSocket(userId, ws);
        const clientKey = getClientKey(ws, userId);
        removeClientFromCrmChannels(clientKey);
        clientMeta.delete(clientKey);
        const socketCount = clients.get(userId)?.size || 0;
        const lastSeenAt = ws.lastSeenAt || new Date();

        setUserPresence({
          userId,
          online: socketCount > 0,
          socketCount,
          lastSeen: lastSeenAt,
          activeConversationId: ws.activeConversationId || ''
        }).catch((error) => {
          console.error('Failed to update presence on close:', error.message);
        });

      if (socketCount === 0) {
        clearTypingState({
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
        activeConversationId: ws.activeConversationId || ''
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
  createWebSocketHub
};
