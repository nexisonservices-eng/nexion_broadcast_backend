const { createRedisConnection } = require('../config/redis');

const redis = createRedisConnection();

redis.on('error', (error) => {
  const message = String(error?.message || '').trim();
  if (
    error?.code === 'ECONNREFUSED' ||
    message.includes('ECONNREFUSED') ||
    message.includes('connect ECONNREFUSED')
  ) {
    return;
  }
  console.error('Presence store Redis error:', message || error);
});

const PRESENCE_PREFIX = 'nexion:presence:user';
const TYPING_PREFIX = 'nexion:typing:conversation';
const PRESENCE_TTL_SECONDS = 120;
const OFFLINE_TTL_SECONDS = 60 * 60 * 24;
const TYPING_TTL_SECONDS = 8;

const toCleanString = (value) => String(value || '').trim();

const buildPresenceKey = (userId) => `${PRESENCE_PREFIX}:${toCleanString(userId)}`;
const buildTypingKey = (conversationId, userId) =>
  `${TYPING_PREFIX}:${toCleanString(conversationId)}:user:${toCleanString(userId)}`;

const serialize = (value = {}) => JSON.stringify(value);

const setUserPresence = async ({
  userId,
  online = true,
  socketCount = 1,
  lastSeen = new Date(),
  activeConversationId = ''
} = {}) => {
  const normalizedUserId = toCleanString(userId);
  if (!normalizedUserId) return false;

  const payload = {
    userId: normalizedUserId,
    online: Boolean(online),
    socketCount: Math.max(0, Number(socketCount) || 0),
    lastSeen: new Date(lastSeen || Date.now()).toISOString(),
    activeConversationId: toCleanString(activeConversationId) || null,
    updatedAt: new Date().toISOString()
  };

  const ttlSeconds = payload.online ? PRESENCE_TTL_SECONDS : OFFLINE_TTL_SECONDS;
  await redis.set(buildPresenceKey(normalizedUserId), serialize(payload), 'EX', ttlSeconds);
  return payload;
};

const clearUserPresence = async ({ userId } = {}) => {
  const normalizedUserId = toCleanString(userId);
  if (!normalizedUserId) return false;
  await redis.del(buildPresenceKey(normalizedUserId));
  return true;
};

const setTypingState = async ({
  userId,
  conversationId,
  isTyping = true,
  displayName = ''
} = {}) => {
  const normalizedUserId = toCleanString(userId);
  const normalizedConversationId = toCleanString(conversationId);
  if (!normalizedUserId || !normalizedConversationId) return false;

  const payload = {
    userId: normalizedUserId,
    conversationId: normalizedConversationId,
    isTyping: Boolean(isTyping),
    displayName: toCleanString(displayName) || null,
    updatedAt: new Date().toISOString()
  };

  if (!payload.isTyping) {
    await redis.del(buildTypingKey(normalizedConversationId, normalizedUserId));
    return payload;
  }

  await redis.set(
    buildTypingKey(normalizedConversationId, normalizedUserId),
    serialize(payload),
    'EX',
    TYPING_TTL_SECONDS
  );
  return payload;
};

const clearTypingState = async ({ userId, conversationId } = {}) => {
  const normalizedUserId = toCleanString(userId);
  const normalizedConversationId = toCleanString(conversationId);
  if (!normalizedUserId || !normalizedConversationId) return false;
  await redis.del(buildTypingKey(normalizedConversationId, normalizedUserId));
  return true;
};

module.exports = {
  PRESENCE_TTL_SECONDS,
  OFFLINE_TTL_SECONDS,
  TYPING_TTL_SECONDS,
  buildPresenceKey,
  buildTypingKey,
  setUserPresence,
  clearUserPresence,
  setTypingState,
  clearTypingState
};
