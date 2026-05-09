const WebSocket = require('ws');
const {
  publishRealtimeEvent,
  subscribeRealtimeEvents
} = require('./realtimeBus');
const {
  setUserPresence,
  clearUserPresence,
  setTypingState,
  clearTypingState
} = require('./presenceStore');

const createWebSocketHub = ({ wss }) => {
  const clients = new Map();

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
  };

  const sendToLocalUser = (userId, data) => {
    const socketSet = clients.get(String(userId || '').trim());
    if (!socketSet || socketSet.size === 0) return;

    const message = JSON.stringify(data);
    socketSet.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
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
    }
  };

  subscribeRealtimeEvents((event) => {
    deliverEvent(event);
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

  const publishPresenceUpdate = async ({ userId, online, socketCount, lastSeen, activeConversationId }) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;

    const payload = {
      scope: 'broadcast',
      data: {
        type: 'presence:update',
        userId: normalizedUserId,
        online: Boolean(online),
        socketCount: Math.max(0, Number(socketCount) || 0),
        lastSeen: lastSeen || new Date().toISOString(),
        activeConversationId: String(activeConversationId || '').trim() || null
      }
    };
    broadcastLocal(payload.data);
    await publishRealtimeEvent(payload);
  };

  const publishTypingUpdate = async ({
    userId,
    conversationId,
    isTyping,
    displayName
  }) => {
    const normalizedUserId = String(userId || '').trim();
    const normalizedConversationId = String(conversationId || '').trim();
    if (!normalizedUserId || !normalizedConversationId) return;

    const payload = {
      scope: 'broadcast',
      data: {
        type: 'typing:update',
        userId: normalizedUserId,
        conversationId: normalizedConversationId,
        isTyping: Boolean(isTyping),
        displayName: String(displayName || '').trim() || null
      }
    };
    broadcastLocal(payload.data);
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
        ws.lastSeenAt = new Date();
        console.log(`Client identified: ${nextUserId}`);

        const socketCount = nextSocketSet.size;
        await setUserPresence({
          userId: nextUserId,
          online: true,
          socketCount,
          lastSeen: ws.lastSeenAt,
          activeConversationId: ws.activeConversationId || ''
        });
        await publishPresenceUpdate({
          userId: nextUserId,
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
        ws.lastSeenAt = new Date();
        if (activeConversationId) {
          ws.activeConversationId = activeConversationId;
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
          online: true,
          socketCount,
          lastSeen: ws.lastSeenAt.toISOString(),
          activeConversationId: ws.activeConversationId || ''
        });
        break;
      }
      case 'ping': {
        await handleWebSocketMessage({ ...data, type: 'presence:ping' }, userId, ws);
        break;
      }
      case 'typing':
      case 'typing:start':
      case 'typing:stop': {
        const conversationId = String(data.conversationId || '').trim();
        const isTyping =
          data.type === 'typing:start' ? true : data.type === 'typing:stop' ? false : Boolean(data.isTyping);
        const displayName = String(data.displayName || data.userName || '').trim();

        ws.lastSeenAt = new Date();
        if (conversationId) {
          ws.activeConversationId = conversationId;
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
    ws.lastSeenAt = new Date();
    ws.activeConversationId = '';

    console.log('User ID:', userId);
    getSocketSet(userId)?.add(ws);

    console.log('Total connected users:', clients.size);

    broadcastUserList();

    setUserPresence({
      userId,
      online: true,
      socketCount: clients.get(userId)?.size || 1,
      lastSeen: ws.lastSeenAt,
      activeConversationId: ''
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
