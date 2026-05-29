require('dotenv').config();

const { publishRealtimeEvent } = require('../realtime/realtimeBus');
const connectDB = require('../config/database');
const {
  claimRealtimeOutboxEvents,
  markRealtimeOutboxEventPublished,
  purgePublishedRealtimeOutboxEvents,
  rescheduleRealtimeOutboxEvent
} = require('../services/realtimeOutboxService');

const WORKER_ID = `realtime-outbox-${process.pid}-${Math.random().toString(16).slice(2, 8)}`;
const POLL_INTERVAL_MS = Math.max(100, Number(process.env.REALTIME_OUTBOX_POLL_MS || 250));
const CLAIM_BATCH_SIZE = Math.max(1, Math.min(100, Number(process.env.REALTIME_OUTBOX_BATCH_SIZE || 25)));
const CLEANUP_INTERVAL_MS = Math.max(60_000, Number(process.env.REALTIME_OUTBOX_CLEANUP_MS || 15 * 60_000));
const CLEANUP_RETENTION_DAYS = Math.max(1, Number(process.env.REALTIME_OUTBOX_RETENTION_DAYS || 3));
const MAX_LOOPS_WITHOUT_WORK = Math.max(5, Number(process.env.REALTIME_OUTBOX_IDLE_LOOPS || 12));

let isShuttingDown = false;
let lastCleanupAt = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const publishEvent = async (event = {}) => {
  const payload = event?.payload || {};
  if (!payload || typeof payload !== 'object') {
    return false;
  }

  await publishRealtimeEvent(payload);
  return true;
};

const processClaimedEvent = async (event = {}) => {
  try {
    await publishEvent(event);
    await markRealtimeOutboxEventPublished({
      id: event._id,
      lockOwner: event.lockOwner || WORKER_ID
    });
    return true;
  } catch (error) {
    await rescheduleRealtimeOutboxEvent({
      id: event._id,
      lockOwner: event.lockOwner || WORKER_ID,
      error,
      attempt: Number(event.attempts || 0)
    });
    console.error('Failed to publish realtime outbox event:', {
      eventId: String(event?._id || ''),
      eventType: String(event?.eventType || ''),
      message: String(error?.message || error || 'unknown error')
    });
    return false;
  }
};

const cleanupOutbox = async () => {
  try {
    const result = await purgePublishedRealtimeOutboxEvents({
      olderThanDays: CLEANUP_RETENTION_DAYS
    });
    if (result?.deletedCount) {
      console.log(`Realtime outbox cleanup removed ${result.deletedCount} published events.`);
    }
  } catch (error) {
    console.error('Realtime outbox cleanup failed:', error?.message || error);
  }
};

const run = async () => {
  await connectDB();
  console.log(`Realtime outbox worker started with id=${WORKER_ID}`);

  let idleLoops = 0;
  while (!isShuttingDown) {
    try {
      const events = await claimRealtimeOutboxEvents({
        limit: CLAIM_BATCH_SIZE,
        lockOwner: WORKER_ID
      });

      if (!events.length) {
        idleLoops += 1;
        if (idleLoops >= MAX_LOOPS_WITHOUT_WORK) {
          idleLoops = 0;
          const now = Date.now();
          if (now - lastCleanupAt >= CLEANUP_INTERVAL_MS) {
            lastCleanupAt = now;
            await cleanupOutbox();
          }
        }
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      idleLoops = 0;
      for (const event of events) {
        if (isShuttingDown) break;
        await processClaimedEvent(event);
      }
    } catch (error) {
      console.error('Realtime outbox worker loop error:', error?.message || error);
      await sleep(Math.max(POLL_INTERVAL_MS, 1000));
    }
  }
};

const shutdown = async () => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log('Shutting down realtime outbox worker...');
  await sleep(100);
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

run().catch((error) => {
  console.error('Realtime outbox worker failed to start:', error?.message || error);
  process.exit(1);
});
