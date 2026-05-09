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
  if (isRedisDisabled) {
    return {
      success: false,
      error: `${getRedisDisabledReason()}. Configure REDIS_URL or REDIS_HOST/REDIS_PORT to enable broadcast inbox writes.`
    };
  }

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
    skipActivityLog = false
  } = payload;

  if (!broadcastId || !userId || !phoneNumber) {
    return { success: false, error: 'Missing broadcast inbox queue payload' };
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
