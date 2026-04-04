const WebSocket = require('ws');

const createWebSocketHub = ({ wss }) => {
  const clients = new Map();

  const broadcast = (data) => {
    const message = JSON.stringify(data);
    clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  };

  const sendToUser = (userId, data) => {
    const client = clients.get(String(userId || ''));
    if (client && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  };

  const emitRealtimeEvent = (userId, data) => {
    if (userId) {
      sendToUser(String(userId), data);
      return;
    }
    broadcast(data);
  };

  const broadcastUserList = () => {
    const userList = Array.from(clients.keys());
    broadcast({ type: 'user_list', users: userList });
  };

  const handleWebSocketMessage = (data, userId, ws) => {
    switch (data.type) {
      case 'identify':
        if (data.userId) {
          clients.delete(userId);
          clients.set(data.userId, ws);
          console.log(`Client identified: ${data.userId}`);
          broadcastUserList();
        }
        break;
      default:
        console.log(`Unknown message type from ${userId}:`, data.type);
    }
  };

  wss.on('connection', (ws, req) => {
    console.log('WebSocket connection established');
    const userId = req.headers['user-id'] || 'anonymous';
    console.log('User ID:', userId);
    clients.set(userId, ws);

    console.log('Total connected clients:', clients.size);

    broadcastUserList();

    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        console.log('WebSocket message received:', data);
        handleWebSocketMessage(data, userId, ws);
      } catch (error) {
        console.error('WebSocket message parse error:', error);
      }
    });

    ws.on('close', () => {
      clients.delete(userId);
      console.log(`Client disconnected: ${userId}`);
      broadcastUserList();
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
