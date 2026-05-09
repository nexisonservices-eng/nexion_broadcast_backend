const { EventEmitter } = require('events');
const { createRedisConnection } = require('../config/redis');

const CHANNEL = 'nexion:broadcast:realtime';

const publisher = createRedisConnection();
const subscriber = createRedisConnection();
const emitter = new EventEmitter();

const ignoreRedisError = (error) => {
  const message = String(error?.message || '').trim();
  if (
    error?.code === 'ECONNREFUSED' ||
    message.includes('ECONNREFUSED') ||
    message.includes('connect ECONNREFUSED')
  ) {
    return;
  }
  console.error('Broadcast event bus Redis error:', message || error);
};

publisher.on('error', ignoreRedisError);
subscriber.on('error', ignoreRedisError);

const publishBroadcastEvent = async ({ userId = null, payload }) => {
  if (!payload) return;
  await publisher.publish(
    CHANNEL,
    JSON.stringify({
      userId: userId ? String(userId) : null,
      payload
    })
  );
};

const subscribeBroadcastEvents = async (handler) => {
  if (typeof handler !== 'function') return;
  await subscriber.subscribe(CHANNEL);
  subscriber.on('message', (channel, message) => {
    if (channel !== CHANNEL) return;
    try {
      const parsed = JSON.parse(message);
      emitter.emit('event', parsed);
      handler(parsed);
    } catch (error) {
      emitter.emit('error', error);
    }
  });
};

const closeBroadcastEventBus = async () => {
  await Promise.allSettled([publisher.quit(), subscriber.quit()]);
};

module.exports = {
  CHANNEL,
  emitter,
  publishBroadcastEvent,
  subscribeBroadcastEvents,
  closeBroadcastEventBus
};
