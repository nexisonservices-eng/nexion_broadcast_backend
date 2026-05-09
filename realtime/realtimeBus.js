const crypto = require('crypto');
const { createRedisConnection } = require('../config/redis');

const CHANNEL = 'nexion:realtime:events';

const publisher = createRedisConnection();
const subscriber = createRedisConnection();
const instanceId = crypto.randomUUID();

const ignoreRedisError = (error) => {
  const message = String(error?.message || '').trim();
  if (
    error?.code === 'ECONNREFUSED' ||
    message.includes('ECONNREFUSED') ||
    message.includes('connect ECONNREFUSED')
  ) {
    return;
  }
  console.error('Realtime bus Redis error:', message || error);
};

publisher.on('error', ignoreRedisError);
subscriber.on('error', ignoreRedisError);

const publishRealtimeEvent = async (payload = {}) => {
  if (!payload) return false;
  await publisher.publish(
    CHANNEL,
    JSON.stringify({
      originId: instanceId,
      payload
    })
  );
  return true;
};

const subscribeRealtimeEvents = (handler) => {
  if (typeof handler !== 'function') return () => {};

  const onMessage = (channel, message) => {
    if (channel !== CHANNEL) return;
    try {
      const parsed = JSON.parse(message);
      if (parsed?.originId === instanceId) return;
      handler(parsed?.payload || {});
    } catch (error) {
      console.error('Realtime bus message parse error:', error);
    }
  };

  subscriber.subscribe(CHANNEL).catch((error) => {
    console.error('Failed to subscribe to realtime bus:', error.message);
  });
  subscriber.on('message', onMessage);

  return async () => {
    subscriber.off('message', onMessage);
    await Promise.allSettled([publisher.quit(), subscriber.quit()]);
  };
};

module.exports = {
  CHANNEL,
  instanceId,
  publishRealtimeEvent,
  subscribeRealtimeEvents
};
