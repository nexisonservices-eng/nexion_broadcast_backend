const { Queue, QueueEvents } = require('bullmq');
const Broadcast = require('../models/Broadcast');
const { createRedisConnection, isRedisDisabled } = require('../config/redis');

const queueName = 'broadcast-send';
const connection = createRedisConnection({
  maxRetriesPerRequest: null,
  enableOfflineQueue: true
});

connection.on('error', (error) => {
  const message = String(error?.message || '').trim();
  if (
    error?.code === 'ECONNREFUSED' ||
    message.includes('ECONNREFUSED') ||
    message.includes('connect ECONNREFUSED')
  ) {
    return;
  }
  console.error('Broadcast queue Redis error:', message || error);
});

const createNoopQueue = () => ({
  add: async (_name, data = {}, _opts = {}) => ({
    id: `noop-${Date.now()}`,
    data
  }),
  getJobCounts: async () => ({
    waiting: 0,
    active: 0,
    completed: 0,
    failed: 0,
    delayed: 0,
    paused: 0
  })
});

const createNoopQueueEvents = () => ({
  on: () => undefined,
  off: () => undefined,
  close: async () => undefined
});

const broadcastQueue = isRedisDisabled
  ? createNoopQueue()
  : new Queue(queueName, {
      connection,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 3000
        },
        removeOnComplete: {
          age: 3600,
          count: 50000
        },
        removeOnFail: false
      }
    });

const broadcastQueueEvents = isRedisDisabled
  ? createNoopQueueEvents()
  : new QueueEvents(queueName, { connection });

if (!isRedisDisabled) {
  broadcastQueueEvents.on('error', (error) => {
    const message = String(error?.message || '').trim();
    if (
      error?.code === 'ECONNREFUSED' ||
      message.includes('ECONNREFUSED') ||
      message.includes('connect ECONNREFUSED')
    ) {
      return;
    }
    console.error('Broadcast queue events Redis error:', message || error);
  });
}

const normalizeDelay = (scheduledAt) => {
  if (!scheduledAt) return 0;
  const scheduledDate = new Date(scheduledAt);
  if (Number.isNaN(scheduledDate.getTime())) return 0;
  return Math.max(0, scheduledDate.getTime() - Date.now());
};

const getBroadcastChunkProgressKeys = (broadcastId) => ({
  total: `broadcast:${broadcastId}:chunks:total`,
  state: `broadcast:${broadcastId}:chunks:state`
});

const initializeBroadcastChunkProgress = async (broadcastId, totalChunks) => {
  const keys = getBroadcastChunkProgressKeys(broadcastId);
  const ttlSeconds = 60 * 60 * 24;
  await connection
    .multi()
    .set(keys.total, String(Math.max(0, Number(totalChunks) || 0)), 'EX', ttlSeconds)
    .del(keys.state)
    .exec();
  return keys;
};

const markBroadcastChunkState = async (broadcastId, chunkId, state) => {
  const keys = getBroadcastChunkProgressKeys(broadcastId);
  const normalizedState = String(state || '').trim().toLowerCase() || 'done';
  await connection.hset(keys.state, String(chunkId), normalizedState);
  await connection.expire(keys.state, 60 * 60 * 24);

  const [totalRaw, stateMap] = await Promise.all([
    connection.get(keys.total),
    connection.hgetall(keys.state)
  ]);

  const total = Math.max(0, Number(totalRaw || 0) || 0);
  const values = Object.values(stateMap || {});
  const done = values.filter((value) => value === 'done').length;
  const failed = values.filter((value) => value === 'failed').length;
  const processed = done + failed;

  return {
    total,
    done,
    failed,
    processed,
    complete: total > 0 && processed >= total
  };
};

const getBroadcastChunkProgressSummary = async (broadcastId) => {
  const keys = getBroadcastChunkProgressKeys(broadcastId);
  const [totalRaw, stateMap] = await Promise.all([
    connection.get(keys.total),
    connection.hgetall(keys.state)
  ]);

  const total = Math.max(0, Number(totalRaw || 0) || 0);
  const values = Object.values(stateMap || {});
  const done = values.filter((value) => value === 'done').length;
  const failed = values.filter((value) => value === 'failed').length;
  const processed = done + failed;

  return {
    total,
    done,
    failed,
    processed,
    complete: total > 0 && processed >= total
  };
};

const clearBroadcastChunkProgress = async (broadcastId) => {
  const keys = getBroadcastChunkProgressKeys(broadcastId);
  await connection.del(keys.total, keys.state);
};

const enqueueBroadcastSend = async ({
  broadcastId,
  userId = null,
  companyId = null,
  scheduledAt = null,
  delayMs = null,
  reason = 'manual'
}) => {
  if (isRedisDisabled) {
    return {
      success: false,
      error: 'Broadcast queue is disabled in local mode'
    };
  }

  const broadcast = await Broadcast.findById(broadcastId)
    .select('_id status scheduledAt createdById companyId queueJobId')
    .lean();

  if (!broadcast) {
    return { success: false, error: 'Broadcast not found' };
  }

  const status = String(broadcast.status || '').toLowerCase();
  if (['cancelled', 'failed', 'completed'].includes(status)) {
    return {
      success: false,
      error: `Broadcast is already ${status} and cannot be queued`
    };
  }

  const delay =
    Number.isFinite(Number(delayMs)) && Number(delayMs) >= 0
      ? Math.max(0, Math.trunc(Number(delayMs)))
      : normalizeDelay(scheduledAt || broadcast.scheduledAt);
  const jobId = `broadcast:${String(broadcastId)}:plan`;
  const job = await broadcastQueue.add(
    'plan-broadcast',
    {
      broadcastId: String(broadcastId),
      userId: String(userId || broadcast.createdById || ''),
      companyId: String(companyId || broadcast.companyId || ''),
      reason
    },
    {
      jobId,
      delay
    }
  );

  const nextStatus = delay > 0 ? 'scheduled' : 'queued';
  await Broadcast.updateOne(
    { _id: broadcastId },
    {
      $set: {
        status: nextStatus,
        queueJobId: job.id,
        queueQueuedAt: new Date(),
        updatedAt: new Date()
      }
    }
  );

  return {
    success: true,
    data: {
      jobId: job.id,
      delay,
      status: nextStatus
    }
  };
};

const getBroadcastQueueCounts = async () =>
  broadcastQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed', 'paused');

const getQueueLagSnapshot = async () => {
  if (isRedisDisabled) {
    return {
      oldestWaitingAgeMs: 0,
      oldestDelayedAgeMs: 0,
      oldestWaitingJobId: null,
      oldestDelayedJobId: null
    };
  }

  const [waitingJobs, delayedJobs] = await Promise.all([
    broadcastQueue.getJobs(['waiting'], 0, 0, true),
    broadcastQueue.getJobs(['delayed'], 0, 0, true)
  ]);

  const now = Date.now();
  const oldestWaiting = waitingJobs?.[0] || null;
  const oldestDelayed = delayedJobs?.[0] || null;

  return {
    oldestWaitingAgeMs: oldestWaiting?.timestamp ? Math.max(0, now - oldestWaiting.timestamp) : 0,
    oldestDelayedAgeMs: oldestDelayed?.timestamp ? Math.max(0, now - oldestDelayed.timestamp) : 0,
    oldestWaitingJobId: oldestWaiting?.id || null,
    oldestDelayedJobId: oldestDelayed?.id || null
  };
};

module.exports = {
  queueName,
  connection,
  broadcastQueue,
  broadcastQueueEvents,
  enqueueBroadcastSend,
  getBroadcastQueueCounts,
  getQueueLagSnapshot,
  initializeBroadcastChunkProgress,
  markBroadcastChunkState,
  getBroadcastChunkProgressSummary,
  clearBroadcastChunkProgress,
  getBroadcastChunkProgressKeys
};
