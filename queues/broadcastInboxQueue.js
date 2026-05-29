const { Queue, QueueEvents } = require('bullmq');
const { createRedisConnection, isRedisDisabled, getRedisDisabledReason } = require('../config/redis');

const queueName = 'broadcast-inbox-write';
const connection = createRedisConnection({
  maxRetriesPerRequest: null,
  enableOfflineQueue: true
});

const createNoopQueue = () => ({
  add: async (_name, data = {}, _opts = {}) => ({
    id: `noop-${Date.now()}`,
    data
  }),
  addBulk: async (jobs = []) =>
    jobs.map((job, index) => ({
      id: `noop-${Date.now()}-${index}`,
      data: job?.data || {}
    })),
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

const localFallbackJobs = new Map();

const scheduleLocalFallbackJob = async ({ jobId, delayMs = 0, run }) => {
  const existing = localFallbackJobs.get(jobId);
  if (existing) {
    return existing.job;
  }

  const job = {
    id: jobId,
    data: {}
  };

  localFallbackJobs.set(jobId, { job });

  if (Math.max(0, Math.trunc(Number(delayMs) || 0)) <= 0) {
    try {
      if (typeof run === 'function') {
        await run();
      }
    } catch (error) {
      console.error('Local broadcast inbox fallback job failed:', error?.message || error);
    } finally {
      localFallbackJobs.delete(jobId);
    }
    return job;
  }

  const timer = setTimeout(async () => {
    try {
      if (typeof run === 'function') {
        await run();
      }
    } catch (error) {
      console.error('Local broadcast inbox fallback job failed:', error?.message || error);
    } finally {
      localFallbackJobs.delete(jobId);
    }
  }, Math.max(0, Math.trunc(Number(delayMs) || 0)));

  timer.unref?.();
  localFallbackJobs.set(jobId, { job, timer });

  return job;
};

const broadcastInboxQueue = isRedisDisabled
  ? createNoopQueue()
  : new Queue(queueName, {
      connection,
      defaultJobOptions: {
        attempts: 5,
        backoff: {
          type: 'exponential',
          delay: 2000
        },
        removeOnComplete: {
          age: 3600,
          count: 100000
        },
        removeOnFail: {
          age: 7 * 86400
        }
      }
    });

const broadcastInboxQueueEvents = isRedisDisabled
  ? createNoopQueueEvents()
  : new QueueEvents(queueName, { connection });

const enqueueBroadcastInboxWrite = async (payload = {}) => {
  const {
    broadcastId,
    userId,
    companyId,
    phoneNumber,
    message,
    whatsappResponse,
    broadcastDispatchKey = '',
    templateCategory = '',
    contactId = '',
    skipActivityLog = false,
    fallbackProcess = null
  } = payload;

  if (!broadcastId || !userId || !phoneNumber) {
    return { success: false, error: 'Missing broadcast inbox queue payload' };
  }

  if (isRedisDisabled) {
    if (typeof fallbackProcess !== 'function') {
      return {
        success: false,
        error: `${getRedisDisabledReason()}. Configure REDIS_URL or REDIS_HOST/REDIS_PORT to enable broadcast inbox writes.`
      };
    }

    const jobId = `broadcast-inbox:${String(broadcastDispatchKey || `${broadcastId}:${phoneNumber}`)}`;
    const job = await scheduleLocalFallbackJob({
      jobId,
      delayMs: 0,
      run: fallbackProcess
    });

    return { success: true, data: { jobId: job.id, localFallback: true } };
  }

  const job = await broadcastInboxQueue.add(
    'persist-broadcast-message',
    {
      broadcastId: String(broadcastId),
      userId: String(userId),
      companyId: companyId ? String(companyId) : '',
      phoneNumber: String(phoneNumber),
      message: String(message || ''),
      whatsappResponse: whatsappResponse || null,
      broadcastDispatchKey: String(broadcastDispatchKey || ''),
      templateCategory: String(templateCategory || ''),
      contactId: String(contactId || ''),
      skipActivityLog: Boolean(skipActivityLog)
    },
    {
      jobId: `broadcast-inbox:${String(broadcastDispatchKey || `${broadcastId}:${phoneNumber}`)}`
    }
  );

  return { success: true, data: { jobId: job.id } };
};

const getBroadcastInboxQueueCounts = async () =>
  broadcastInboxQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed', 'paused');

const getBroadcastInboxQueueLagSnapshot = async () => {
  if (isRedisDisabled) {
    return {
      oldestWaitingAgeMs: 0,
      oldestDelayedAgeMs: 0,
      oldestWaitingJobId: null,
      oldestDelayedJobId: null
    };
  }

  const [waitingJobs, delayedJobs] = await Promise.all([
    broadcastInboxQueue.getJobs(['waiting'], 0, 0, true),
    broadcastInboxQueue.getJobs(['delayed'], 0, 0, true)
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
  broadcastInboxQueue,
  broadcastInboxQueueEvents,
  enqueueBroadcastInboxWrite,
  getBroadcastInboxQueueCounts,
  getBroadcastInboxQueueLagSnapshot
};
