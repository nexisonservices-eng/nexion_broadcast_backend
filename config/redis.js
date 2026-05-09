const IORedis = require('ioredis');

const hasExplicitRedisConfig = Boolean(
  String(process.env.REDIS_URL || '').trim() ||
    String(process.env.REDIS_HOST || '').trim() ||
    String(process.env.REDIS_PORT || '').trim() ||
    String(process.env.DISABLE_REDIS || '').trim() ||
    String(process.env.REDIS_DISABLED || '').trim()
);

const isRedisDisabled =
  String(process.env.DISABLE_REDIS || process.env.REDIS_DISABLED || '').trim().toLowerCase() ===
    'true' || !hasExplicitRedisConfig;

const redisUrl =
  String(process.env.REDIS_URL || '').trim() ||
  `redis://${String(process.env.REDIS_HOST || '127.0.0.1').trim()}:${String(
    process.env.REDIS_PORT || '6379'
  ).trim()}`;

const redisOptions = {
  lazyConnect: true,
  connectTimeout: 1000,
  maxRetriesPerRequest: 1,
  enableReadyCheck: false,
  enableOfflineQueue: false,
  retryStrategy: (times) => {
    if (times > 3) return null;
    return Math.min(times * 100, 1000);
  }
};

const shouldSilenceRedisErrors =
  String(process.env.NODE_ENV || '').trim().toLowerCase() !== 'production';

const createNoopMulti = () => ({
  set: () => createNoopMulti(),
  del: () => createNoopMulti(),
  hset: () => createNoopMulti(),
  expire: () => createNoopMulti(),
  exec: async () => []
});

const createNoopRedisConnection = () => {
  const noop = {
    status: 'ready',
    connected: false,
    on: () => noop,
    off: () => noop,
    once: () => noop,
    connect: async () => noop,
    quit: async () => 'OK',
    end: async () => 'OK',
    disconnect: () => undefined,
    publish: async () => 0,
    subscribe: async () => 0,
    unsubscribe: async () => 0,
    set: async () => 'OK',
    get: async () => null,
    del: async () => 0,
    hset: async () => 1,
    hgetall: async () => ({}),
    expire: async () => 0,
    multi: () => createNoopMulti(),
    duplicate: () => createNoopRedisConnection(),
    ping: async () => 'PONG',
    keys: async () => []
  };

  return noop;
};

const createRedisConnection = (overrides = {}) => {
  if (isRedisDisabled) {
    return createNoopRedisConnection();
  }

  const client = new IORedis(redisUrl, {
    ...redisOptions,
    ...overrides
  });

  client.on('error', (error) => {
    const message = String(error?.message || '').trim();
    if (
      shouldSilenceRedisErrors &&
      (error?.code === 'ECONNREFUSED' ||
        message.includes('ECONNREFUSED') ||
        message.includes('connect ECONNREFUSED'))
    ) {
      return;
    }

    console.error('Redis client error:', message || error);
  });

  return client;
};

module.exports = {
  redisUrl,
  redisOptions,
  hasExplicitRedisConfig,
  isRedisDisabled,
  createRedisConnection,
  createNoopRedisConnection
};
