const crypto = require('crypto');
const { createRedisConnection } = require('../config/redis');

const CACHE_NAMESPACE = 'team-inbox';
const CACHE_TTL_SECONDS = {
  conversations: 30,
  messages: 45
};
const VERSION_TTL_SECONDS = 60 * 60 * 24 * 30;

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
      console.error('Team inbox cache Redis error:', message || error);
    });
  }
  return redisClient;
};

const toCleanString = (value) => String(value || '').trim();

const toScopeId = ({ companyId = '', userId = '' } = {}) => {
  const normalizedCompanyId = toCleanString(companyId);
  const normalizedUserId = toCleanString(userId);

  if (normalizedCompanyId && normalizedUserId) {
    return `company:${normalizedCompanyId}:user:${normalizedUserId}`;
  }
  if (normalizedCompanyId) {
    return `company:${normalizedCompanyId}`;
  }
  if (normalizedUserId) {
    return `user:${normalizedUserId}`;
  }
  return '';
};

const getInboxScopeVariants = ({ companyId = '', userId = '' } = {}) => {
  const scopes = [];
  const companyScope = toScopeId({ companyId });
  const userScope = toScopeId({ companyId, userId });

  if (companyScope) scopes.push(companyScope);
  if (userScope && userScope !== companyScope) scopes.push(userScope);
  return scopes;
};

const hashParts = (...parts) =>
  crypto
    .createHash('sha1')
    .update(parts.map((part) => toCleanString(part)).join('::'))
    .digest('hex');

const getVersionKey = (namespace, scope, versionGroup) =>
  `${CACHE_NAMESPACE}:${namespace}:${versionGroup}:${scope}:version`;

const getCacheKey = ({ namespace, scope, versionGroup, version, keyParts = [] }) =>
  `${CACHE_NAMESPACE}:${namespace}:${versionGroup}:${scope}:v${version}:${hashParts(...keyParts)}`;

const getVersion = async (namespace, scope, versionGroup) => {
  if (!scope) return 1;
  try {
    const client = getRedisClient();
    const raw = await client.get(getVersionKey(namespace, scope, versionGroup));
    const parsed = Number(raw || 0);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
  } catch {
    return 1;
  }
};

const bumpVersion = async (namespace, scope, versionGroup) => {
  if (!scope) return 1;
  try {
    const client = getRedisClient();
    const next = await client.incr(getVersionKey(namespace, scope, versionGroup));
    await client.expire(getVersionKey(namespace, scope, versionGroup), VERSION_TTL_SECONDS);
    return next;
  } catch {
    return 1;
  }
};

const getCachedJson = async (key) => {
  if (!key) return null;
  try {
    const client = getRedisClient();
    const raw = await client.get(key);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
};

const setCachedJson = async (key, value, ttlSeconds) => {
  if (!key || !Number.isFinite(Number(ttlSeconds)) || Number(ttlSeconds) <= 0) return false;
  try {
    const client = getRedisClient();
    await client.set(key, JSON.stringify(value), 'EX', Math.max(1, Math.trunc(ttlSeconds)));
    return true;
  } catch {
    return false;
  }
};

const getOrSetCachedJson = async ({
  namespace,
  scope,
  versionGroup,
  keyParts = [],
  ttlSeconds,
  loader
}) => {
  if (typeof loader !== 'function' || !scope) {
    return loader ? loader() : null;
  }

  const version = await getVersion(namespace, scope, versionGroup);
  const cacheKey = getCacheKey({
    namespace,
    scope,
    versionGroup,
    version,
    keyParts
  });
  const cached = await getCachedJson(cacheKey);
  if (cached !== null) {
    return cached;
  }

  const value = await loader();
  await setCachedJson(cacheKey, value, ttlSeconds);
  return value;
};

const invalidateInboxScope = async ({ companyId = '', userId = '', versionGroup = 'list' } = {}) => {
  const scopes = getInboxScopeVariants({ companyId, userId });
  await Promise.all(
    scopes.map((scope) => bumpVersion('conversations', scope, versionGroup))
  );
};

const invalidateInboxConversation = async ({
  companyId = '',
  userId = '',
  conversationId = ''
} = {}) => {
  const scopes = getInboxScopeVariants({ companyId, userId });
  const normalizedConversationId = toCleanString(conversationId);
  if (!normalizedConversationId) {
    await Promise.all(scopes.map((scope) => bumpVersion('conversations', scope, 'list')));
    return;
  }

  await Promise.all(
    scopes.flatMap((scope) => [
      bumpVersion('conversations', scope, 'list'),
      bumpVersion('messages', `${scope}:${normalizedConversationId}`, 'thread')
    ])
  );
};

module.exports = {
  CACHE_TTL_SECONDS,
  getInboxScopeVariants,
  getOrSetCachedJson,
  invalidateInboxConversation,
  invalidateInboxScope,
  getCacheKey,
  getVersion,
  bumpVersion,
  toCleanString
};
