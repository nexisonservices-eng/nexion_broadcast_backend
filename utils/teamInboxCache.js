const crypto = require('crypto');
const { createRedisConnection } = require('../config/redis');
const { publishRealtimeEvent } = require('../realtime/realtimeBus');
const { enqueueRealtimeOutboxEvent } = require('../services/realtimeOutboxService');

const CACHE_NAMESPACE = 'team-inbox';
const INVALIDATION_CHANNEL = 'nexion:team-inbox:cache-invalidation';
const CACHE_TTL_SECONDS = {
  conversations: 30,
  summaryPages: 20,
  messages: 45
};
const VERSION_TTL_SECONDS = 60 * 60 * 24 * 30;
// Bump the cache groups whenever the inbox pagination contract changes.
// This avoids reusing stale first-page responses that can incorrectly report
// hasMore=false after a pagination fix ships.
const CONVERSATION_CACHE_VERSION_GROUPS = [
  'list-v2',
  'summaryPages-v2',
  'hydratedPages-v1',
  'unreadCount-v1',
  'overviewSnapshot-v1',
  'overviewCounts-v1',
  'overviewUnread-v1'
];

let redisClient = null;
let invalidationPublisher = null;
let invalidationSubscriber = null;
let invalidationSubscribed = false;
const processInstanceId = crypto.randomUUID();

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

const getInvalidationPublisher = () => {
  if (!invalidationPublisher) {
    invalidationPublisher = createRedisConnection();
    invalidationPublisher.on('error', (error) => {
      const message = String(error?.message || '').trim();
      if (
        error?.code === 'ECONNREFUSED' ||
        message.includes('ECONNREFUSED') ||
        message.includes('connect ECONNREFUSED')
      ) {
        return;
      }
      console.error('Team inbox invalidation Redis publisher error:', message || error);
    });
  }
  return invalidationPublisher;
};

const getInvalidationSubscriber = () => {
  if (!invalidationSubscriber) {
    invalidationSubscriber = createRedisConnection();
    invalidationSubscriber.on('error', (error) => {
      const message = String(error?.message || '').trim();
      if (
        error?.code === 'ECONNREFUSED' ||
        message.includes('ECONNREFUSED') ||
        message.includes('connect ECONNREFUSED')
      ) {
        return;
      }
      console.error('Team inbox invalidation Redis subscriber error:', message || error);
    });
  }
  return invalidationSubscriber;
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

const bumpConversationCacheGroups = async ({
  companyId = '',
  userId = '',
  versionGroups = CONVERSATION_CACHE_VERSION_GROUPS
} = {}) => {
  const scopes = getInboxScopeVariants({ companyId, userId });
  const normalizedGroups = Array.from(
    new Set(
      (Array.isArray(versionGroups) ? versionGroups : [versionGroups])
        .map((versionGroup) => toCleanString(versionGroup))
        .filter(Boolean)
    )
  );

  if (!scopes.length || !normalizedGroups.length) {
    return;
  }

  await Promise.all(
    scopes.flatMap((scope) =>
      normalizedGroups.map((versionGroup) => bumpVersion('conversations', scope, versionGroup))
    )
  );
};

const publishInboxInvalidation = async (payload = {}) => {
  try {
    const publisher = getInvalidationPublisher();
    await publisher.publish(
      INVALIDATION_CHANNEL,
      JSON.stringify({
        originId: processInstanceId,
        payload
      })
    );
  } catch (error) {
    const message = String(error?.message || '').trim();
    if (
      error?.code === 'ECONNREFUSED' ||
      message.includes('ECONNREFUSED') ||
      message.includes('connect ECONNREFUSED')
    ) {
      return;
    }
    console.error('Failed to publish team inbox invalidation:', message || error);
  }
};

const ensureInboxInvalidationSubscriber = () => {
  if (invalidationSubscribed) return;
  invalidationSubscribed = true;

  const subscriber = getInvalidationSubscriber();
  subscriber.subscribe(INVALIDATION_CHANNEL).catch((error) => {
    console.error('Failed to subscribe team inbox invalidation channel:', error?.message || error);
  });

  subscriber.on('message', async (channel, message) => {
    if (channel !== INVALIDATION_CHANNEL) return;
    try {
      const parsed = JSON.parse(message);
      if (parsed?.originId === processInstanceId) return;
      const payload = parsed?.payload || {};
      const companyId = toCleanString(payload?.companyId);
      const userId = toCleanString(payload?.userId);
      const conversationIds = Array.isArray(payload?.conversationIds)
        ? payload.conversationIds.map((value) => toCleanString(value)).filter(Boolean)
        : [];
      const conversationId = toCleanString(payload?.conversationId);
      const invalidatedConversationIds = Array.from(
        new Set([conversationId, ...conversationIds].filter(Boolean))
      );
      const versionGroups = Array.isArray(payload?.versionGroups)
        ? payload.versionGroups
        : payload?.versionGroup
          ? [payload.versionGroup]
          : [];

      if (invalidatedConversationIds.length > 0) {
        await bumpConversationCacheGroups({
          companyId,
          userId,
          versionGroups: versionGroups.length ? versionGroups : CONVERSATION_CACHE_VERSION_GROUPS
        });
        await Promise.all(
          getInboxScopeVariants({ companyId, userId }).flatMap((scope) => [
            ...invalidatedConversationIds.map((normalizedConversationId) =>
              bumpVersion('messages', `${scope}:${normalizedConversationId}`, 'thread')
            )
          ])
        );
        return;
      }

      await bumpConversationCacheGroups({
        companyId,
        userId,
        versionGroups: versionGroups.length ? versionGroups : CONVERSATION_CACHE_VERSION_GROUPS
      });
    } catch (error) {
      console.error('Failed to process team inbox invalidation:', error?.message || error);
    }
  });
};

ensureInboxInvalidationSubscriber();

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
  const normalizedVersionGroup = toCleanString(versionGroup) || 'list';
  const versionGroups =
    normalizedVersionGroup === 'list'
      ? CONVERSATION_CACHE_VERSION_GROUPS
      : [normalizedVersionGroup];

  await bumpConversationCacheGroups({
    companyId,
    userId,
    versionGroups
  });
  void publishInboxInvalidation({
    companyId,
    userId,
    versionGroups
  });
  const realtimePayload = {
    scope: 'company',
    companyId: toCleanString(companyId) || null,
    data: {
      type: 'team_inbox_cache_invalidated',
      companyId: toCleanString(companyId) || null,
      userId: toCleanString(userId) || null,
      versionGroups,
      updatedAt: new Date().toISOString()
    }
  };
  try {
    await enqueueRealtimeOutboxEvent({
      eventType: 'team_inbox_cache_invalidated',
      scope: 'company',
      companyId,
      userId,
      payload: realtimePayload,
      dedupeKey: `team-inbox-cache:${toCleanString(companyId) || 'no-company'}:${toCleanString(userId) || 'no-user'}:${versionGroups.join(',')}`
    });
  } catch (error) {
    console.error('Failed to enqueue team inbox scope invalidation event:', error?.message || error);
    void publishRealtimeEvent(realtimePayload);
  }
};

const invalidateInboxConversation = async ({
  companyId = '',
  userId = '',
  conversationId = '',
  conversationIds = []
} = {}) => {
  const scopes = getInboxScopeVariants({ companyId, userId });
  const normalizedConversationIds = Array.from(
    new Set(
      [conversationId, ...(Array.isArray(conversationIds) ? conversationIds : [])]
        .map((value) => toCleanString(value))
        .filter(Boolean)
    )
  );
  if (!normalizedConversationIds.length) {
    await bumpConversationCacheGroups({
      companyId,
      userId
    });
    return;
  }

  await bumpConversationCacheGroups({
    companyId,
    userId
  });
  await Promise.all(
    scopes.flatMap((scope) =>
      normalizedConversationIds.map((normalizedConversationId) =>
        bumpVersion('messages', `${scope}:${normalizedConversationId}`, 'thread')
      )
    )
  );
  void publishInboxInvalidation({
    companyId,
    userId,
    conversationId: normalizedConversationIds[0],
    conversationIds: normalizedConversationIds
  });
  const realtimePayload = {
    scope: 'company',
    companyId: toCleanString(companyId) || null,
    conversationId: normalizedConversationIds[0],
    data: {
      type: 'team_inbox_cache_invalidated',
      companyId: toCleanString(companyId) || null,
      userId: toCleanString(userId) || null,
      conversationId: normalizedConversationIds[0],
      conversationIds: normalizedConversationIds,
      updatedAt: new Date().toISOString()
    }
  };
  try {
    await enqueueRealtimeOutboxEvent({
      eventType: 'team_inbox_cache_invalidated',
      scope: 'company',
      companyId,
      userId,
      conversationId: normalizedConversationIds[0],
      payload: realtimePayload,
      dedupeKey: `team-inbox-conversation:${toCleanString(companyId) || 'no-company'}:${toCleanString(userId) || 'no-user'}:${normalizedConversationIds.join(',')}`
    });
  } catch (error) {
    console.error('Failed to enqueue team inbox conversation invalidation event:', error?.message || error);
    void publishRealtimeEvent(realtimePayload);
  }
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
