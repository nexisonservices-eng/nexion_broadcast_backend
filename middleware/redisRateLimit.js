const { createRedisConnection, isRedisDisabled } = require('../config/redis');

const memoryBuckets = new Map();
let redisClient = null;

const toCleanString = (value) => String(value || '').trim();

const getRedisClient = () => {
  if (isRedisDisabled) return null;
  if (!redisClient) {
    redisClient = createRedisConnection();
  }
  return redisClient;
};

const buildKey = (req, namespace) => {
  const companyId = toCleanString(req?.companyId || req?.user?.companyId || 'global');
  const userId = toCleanString(req?.user?.id || req?.user?._id || req?.ip || 'anonymous');
  return ['nexion', 'rate-limit', namespace, companyId, userId].join(':');
};

const memoryCheck = (key, windowMs, max) => {
  const now = Date.now();
  const bucket = memoryBuckets.get(key);
  if (!bucket || bucket.resetAt <= now) {
    const nextBucket = {
      count: 1,
      resetAt: now + windowMs
    };
    memoryBuckets.set(key, nextBucket);
    return {
      allowed: true,
      remaining: Math.max(0, max - 1),
      resetAt: nextBucket.resetAt
    };
  }

  bucket.count += 1;
  memoryBuckets.set(key, bucket);
  return {
    allowed: bucket.count <= max,
    remaining: Math.max(0, max - bucket.count),
    resetAt: bucket.resetAt
  };
};

const redisCheck = async (key, windowMs, max) => {
  const client = getRedisClient();
  if (!client) return memoryCheck(key, windowMs, max);

  try {
    const script = `
      local current = redis.call('INCR', KEYS[1])
      if current == 1 then
        redis.call('PEXPIRE', KEYS[1], ARGV[1])
      end
      return current
    `;
    const current = Number(await client.eval(script, 1, key, String(windowMs)) || 0);
    const ttl = Number(await client.pttl(key));
    return {
      allowed: current <= max,
      remaining: Math.max(0, max - current),
      resetAt: ttl > 0 ? Date.now() + ttl : Date.now() + windowMs
    };
  } catch (error) {
    const message = String(error?.message || '').trim();
    if (message) {
      console.warn('Redis rate limit fallback:', message);
    }
    return memoryCheck(key, windowMs, max);
  }
};

const createRedisRateLimiter = ({
  namespace,
  windowMs = 60_000,
  max = 120,
  message = 'Rate limit exceeded. Please try again shortly.'
}) => async (req, res, next) => {
  try {
    const key = buildKey(req, namespace);
    const result = await redisCheck(key, windowMs, max);

    res.setHeader('X-RateLimit-Limit', String(max));
    res.setHeader('X-RateLimit-Remaining', String(result.remaining));
    res.setHeader('X-RateLimit-Reset', String(Math.ceil(result.resetAt / 1000)));

    if (!result.allowed) {
      res.setHeader('Retry-After', String(Math.max(1, Math.ceil((result.resetAt - Date.now()) / 1000))));
      return res.status(429).json({
        success: false,
        error: message
      });
    }

    return next();
  } catch (error) {
    console.error('Rate limiter failure:', error?.message || error);
    return next();
  }
};

module.exports = {
  createRedisRateLimiter
};
