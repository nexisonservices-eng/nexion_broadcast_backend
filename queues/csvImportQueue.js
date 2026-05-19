const { Queue } = require("bullmq");
const { createRedisConnection, isRedisDisabled } = require("../config/redis");

const queueName = "csv-import";
const connection = createRedisConnection({
  maxRetriesPerRequest: null,
  enableOfflineQueue: true,
});

connection.on("error", (error) => {
  const message = String(error?.message || "").trim();
  if (
    error?.code === "ECONNREFUSED" ||
    message.includes("ECONNREFUSED") ||
    message.includes("connect ECONNREFUSED")
  ) {
    return;
  }
  console.error("CSV import queue Redis error:", message || error);
});

const createNoopQueue = () => ({
  add: async (_name, data = {}, _opts = {}) => ({
    id: `noop-${Date.now()}`,
    data,
  }),
  getJob: async () => null,
  getJobCounts: async () => ({
    waiting: 0,
    active: 0,
    completed: 0,
    failed: 0,
    delayed: 0,
    paused: 0,
  }),
});

const csvImportQueue = isRedisDisabled
  ? createNoopQueue()
  : new Queue(queueName, {
      connection,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 3000,
        },
        removeOnComplete: {
          age: 3600,
          count: 10000,
        },
        removeOnFail: false,
      },
    });

const enqueueCsvImport = async ({
  importJobId,
  userId,
  companyId,
  filePath,
  originalFileName,
}) => {
  const job = await csvImportQueue.add(
    "process-csv-import",
    {
      importJobId: String(importJobId || ""),
      userId: String(userId || ""),
      companyId: String(companyId || ""),
      filePath: String(filePath || ""),
      originalFileName: String(originalFileName || ""),
    },
    {
      jobId: `csv-import:${String(importJobId || "")}`,
    },
  );

  return {
    success: true,
    data: {
      jobId: job.id,
    },
  };
};

const cancelCsvImportQueueJob = async (jobId) => {
  if (!jobId || isRedisDisabled) {
    return { success: true, data: { cancelled: false, skipped: true } };
  }

  const job = await csvImportQueue.getJob(String(jobId || "").trim());
  if (!job) {
    return { success: true, data: { cancelled: false } };
  }

  await job.remove();
  return { success: true, data: { cancelled: true } };
};

module.exports = {
  queueName,
  connection,
  csvImportQueue,
  enqueueCsvImport,
  cancelCsvImportQueueJob,
};
