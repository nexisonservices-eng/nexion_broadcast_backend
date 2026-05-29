const crypto = require('crypto');
const { createRedisConnection } = require('../config/redis');

const CHANNEL = 'nexion:realtime:events';
const BATCH_WINDOW_MS = Number(process.env.REALTIME_BATCH_WINDOW_MS || 25);
const BATCH_MAX_SIZE = Number(process.env.REALTIME_BATCH_MAX_SIZE || 50);

const publisher = createRedisConnection();
const subscriber = createRedisConnection();
const instanceId = crypto.randomUUID();
const pendingEvents = [];
let publishTimer = null;

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

const clearPublishTimer = () => {
  if (publishTimer) {
    clearTimeout(publishTimer);
    publishTimer = null;
  }
};

const flushPendingEvents = async () => {
  clearPublishTimer();
  if (pendingEvents.length === 0) return false;

  const events = pendingEvents.splice(0, pendingEvents.length);
  const payload =
    events.length === 1
      ? events[0]
      : {
          type: 'realtime_batch',
          events,
          timestamp: new Date().toISOString()
        };

  await publisher.publish(
    CHANNEL,
    JSON.stringify({
      originId: instanceId,
      payload
    })
  );
  return true;
};

const scheduleFlush = () => {
  if (publishTimer) return;
  publishTimer = setTimeout(() => {
    flushPendingEvents().catch((error) => {
      console.error('Failed to flush realtime batch:', error?.message || error);
    });
  }, BATCH_WINDOW_MS);
  if (typeof publishTimer.unref === 'function') {
    publishTimer.unref();
  }
};

publisher.on('error', ignoreRedisError);
subscriber.on('error', ignoreRedisError);

const publishRealtimeEvent = async (payload = {}) => {
  if (!payload) return false;

  pendingEvents.push(payload);
  if (pendingEvents.length >= BATCH_MAX_SIZE) {
    await flushPendingEvents();
    return true;
  }

  scheduleFlush();
  return true;
};

const subscribeRealtimeEvents = (handler) => {
  if (typeof handler !== 'function') return () => {};

  const onMessage = (channel, message) => {
    if (channel !== CHANNEL) return;
    try {
      const parsed = JSON.parse(message);
      if (parsed?.originId === instanceId) return;
      const payload = parsed?.payload || {};
      if (payload?.type === 'realtime_batch' && Array.isArray(payload.events)) {
        payload.events.forEach((event) => {
          handler(event || {});
        });
        return;
      }
      handler(payload);
    } catch (error) {
      console.error('Realtime bus message parse error:', error);
    }
  };

  subscriber.subscribe(CHANNEL).catch((error) => {
    console.error('Failed to subscribe to realtime bus:', error.message);
  });
  subscriber.on('message', onMessage);

  return async () => {
    clearPublishTimer();
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
