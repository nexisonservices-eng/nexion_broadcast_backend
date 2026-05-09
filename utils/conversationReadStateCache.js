const { createRedisConnection } = require('../config/redis');

const READ_STATE_NAMESPACE = 'nexion:conversation-read';
const READ_STATE_TTL_SECONDS = 60 * 60 * 24 * 7;

let redisClient = null;

const getRedisClient = () => {
  if (!redisClient) {
    redisClient = createRedisConnection();
    redisClient.on('error', (error) => {
      const message = String(error?.message || '').trim();
      if (
        error?.code === 'ECONNREFUSED' ||
        message.includes('ECONNREFUSED') ||
        message.includes('connect ECONNREFUSED')
      ) {
        return;
      }
      console.error('Read state cache Redis error:', message || error);
    });
  }
  return redisClient;
};

const toCleanString = (value) => String(value || '').trim();

const buildReadStateKey = ({ companyId = '', userId = '', conversationId = '' } = {}) =>
  [
    READ_STATE_NAMESPACE,
    toCleanString(companyId) || 'global',
    toCleanString(userId) || 'anonymous',
    toCleanString(conversationId)
  ].join(':');

const normalizeReadState = (state = {}) => {
  if (!state || typeof state !== 'object') return null;

  const unreadCount = Math.max(0, Number(state.unreadCount || 0) || 0);
  const lastInboundMessageId = toCleanString(state.lastInboundMessageId);
  const lastReadMessageId = toCleanString(state.lastReadMessageId);
  const lastInboundAt = toCleanString(state.lastInboundAt);
  const lastReadAt = toCleanString(state.lastReadAt);
  const updatedAt = toCleanString(state.updatedAt);

  return {
    unreadCount,
    lastInboundMessageId: lastInboundMessageId || null,
    lastReadMessageId: lastReadMessageId || null,
    lastInboundAt: lastInboundAt || null,
    lastReadAt: lastReadAt || null,
    updatedAt: updatedAt || null
  };
};

const getConversationReadState = async (scope = {}) => {
  const key = buildReadStateKey(scope);
  try {
    const client = getRedisClient();
    const raw = await client.get(key);
    if (!raw) return null;
    return normalizeReadState(JSON.parse(raw));
  } catch {
    return null;
  }
};

const setConversationReadState = async (scope = {}, nextState = {}) => {
  const key = buildReadStateKey(scope);
  const value = normalizeReadState({
    ...nextState,
    updatedAt: new Date().toISOString()
  });

  if (!value) return null;

  try {
    const client = getRedisClient();
    await client.set(key, JSON.stringify(value), 'EX', READ_STATE_TTL_SECONDS);
    return value;
  } catch {
    return null;
  }
};

const recordConversationInboundUnread = async ({
  companyId = '',
  userId = '',
  conversationId = '',
  messageId = '',
  messageAt = new Date(),
  unreadCount = 1
} = {}) => {
  const current = (await getConversationReadState({ companyId, userId, conversationId })) || {};
  const nextUnreadCount = Math.max(
    0,
    Number.isFinite(Number(unreadCount)) ? Number(unreadCount) : 0
  );
  return setConversationReadState(
    { companyId, userId, conversationId },
    {
      unreadCount: nextUnreadCount > 0 ? nextUnreadCount : Math.max(1, Number(current.unreadCount || 0) + 1),
      lastInboundMessageId: toCleanString(messageId) || current.lastInboundMessageId || null,
      lastInboundAt: new Date(messageAt || Date.now()).toISOString(),
      lastReadMessageId: current.lastReadMessageId || null,
      lastReadAt: current.lastReadAt || null
    }
  );
};

const recordConversationRead = async ({
  companyId = '',
  userId = '',
  conversationId = '',
  latestInboundMessageId = '',
  latestInboundAt = new Date()
} = {}) => {
  const current = (await getConversationReadState({ companyId, userId, conversationId })) || {};
  const normalizedLatestInboundMessageId = toCleanString(latestInboundMessageId) || current.lastInboundMessageId || null;

  return setConversationReadState(
    { companyId, userId, conversationId },
    {
      unreadCount: 0,
      lastInboundMessageId: normalizedLatestInboundMessageId,
      lastInboundAt: current.lastInboundAt || new Date(latestInboundAt || Date.now()).toISOString(),
      lastReadMessageId: normalizedLatestInboundMessageId,
      lastReadAt: new Date().toISOString()
    }
  );
};

const shouldSkipConversationReadUpdate = (state = {}, latestInboundMessageId = '') => {
  const normalizedState = normalizeReadState(state) || {};
  const normalizedLatestInboundMessageId = toCleanString(latestInboundMessageId);

  if (normalizedState.unreadCount > 0) return false;
  if (!normalizedLatestInboundMessageId) return normalizedState.unreadCount === 0;
  return normalizedState.lastReadMessageId === normalizedLatestInboundMessageId;
};

module.exports = {
  READ_STATE_NAMESPACE,
  READ_STATE_TTL_SECONDS,
  buildReadStateKey,
  getConversationReadState,
  setConversationReadState,
  recordConversationInboundUnread,
  recordConversationRead,
  shouldSkipConversationReadUpdate,
  normalizeReadState
};
