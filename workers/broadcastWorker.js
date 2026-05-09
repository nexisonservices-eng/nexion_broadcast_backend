require('dotenv').config();

const { Worker } = require('bullmq');
const Broadcast = require('../models/Broadcast');
const broadcastService = require('../services/broadcastService');
const {
  connection,
  broadcastQueue,
  initializeBroadcastChunkProgress,
  markBroadcastChunkState,
  getBroadcastChunkProgressSummary,
  clearBroadcastChunkProgress
} = require('../queues/broadcastQueue');
const { publishBroadcastEvent } = require('../realtime/broadcastEventBus');

const workerConcurrency = Math.max(1, Number(process.env.BROADCAST_WORKER_CONCURRENCY || 2));
const workerLimiterMax = Math.max(1, Number(process.env.BROADCAST_WORKER_RATE_MAX || 10));
const workerLimiterDuration = Math.max(1000, Number(process.env.BROADCAST_WORKER_RATE_DURATION || 1000));
const chunkSize = Math.max(1, Math.min(250, Number(process.env.BROADCAST_CHUNK_SIZE || 100)));
const progressBatchSize = Math.max(1, Number(process.env.BROADCAST_PROGRESS_BATCH_SIZE || 25));
const progressFlushMs = Math.max(250, Number(process.env.BROADCAST_PROGRESS_FLUSH_MS || 1000));

const splitRecipients = (recipients, size) => {
  const chunks = [];
  for (let index = 0; index < recipients.length; index += size) {
    chunks.push(recipients.slice(index, index + size));
  }
  return chunks;
};

const createBufferedBroadcaster = (userId) => {
  const buffer = [];
  let flushTimer = null;
  let flushing = Promise.resolve();

  const flush = async () => {
    if (!buffer.length) return;
    const events = buffer.splice(0, buffer.length);
    await publishBroadcastEvent({
      userId,
      payload: {
        type: 'broadcast_message_batch',
        events
      }
    });
  };

  const scheduleFlush = () => {
    if (flushTimer) return;
    flushTimer = setTimeout(async () => {
      flushTimer = null;
      try {
        await flush();
      } catch (error) {
        console.error('Failed to flush broadcast progress batch:', error.message);
      }
    }, progressFlushMs);
    flushTimer.unref?.();
  };

  const broadcaster = (payload) => {
    if (!payload) return;

    if (payload.type === 'message_sent') {
      buffer.push(payload);
      if (buffer.length >= progressBatchSize) {
        flushing = flushing
          .then(() => flush())
          .catch((error) => {
            console.error('Failed to publish broadcast batch:', error.message);
          });
      } else {
        scheduleFlush();
      }
      return;
    }

    publishBroadcastEvent({
      userId,
      payload
    }).catch((error) => {
      console.error('Failed to publish broadcast event:', error.message);
    });
  };

  broadcaster.flush = async () => {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    await flushing;
    await flush();
  };

  return broadcaster;
};

const finalizeBroadcastIfReady = async (broadcastId, userId) => {
  const summary = await getBroadcastChunkProgressSummary(broadcastId);

  if (!summary.complete) {
    return { complete: false, summary };
  }

  const broadcast = await Broadcast.findByIdAndUpdate(
    broadcastId,
    {
      $set: {
        status: 'completed',
        completedAt: new Date(),
        updatedAt: new Date()
      }
    },
    { new: true }
  );

  if (broadcast) {
    await publishBroadcastEvent({
      userId,
      payload: {
        type: 'broadcast_updated',
        action: 'completed',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
        queueSummary: summary
      }
    });
  }

  await clearBroadcastChunkProgress(broadcastId);
  return { complete: true, summary, broadcast };
};

const broadcastWorker = new Worker(
  'broadcast-send',
  async (job) => {
    const broadcastId = String(job.data?.broadcastId || '');
    const userId = String(job.data?.userId || '');
    if (!broadcastId) {
      throw new Error('Missing broadcastId');
    }

    if (job.name === 'plan-broadcast') {
      const broadcast = await Broadcast.findById(broadcastId).lean();
      if (!broadcast) {
        throw new Error(`Broadcast ${broadcastId} not found`);
      }

      const recipients = Array.isArray(broadcast.recipients) ? broadcast.recipients : [];
      const chunks = splitRecipients(recipients, chunkSize);
      await initializeBroadcastChunkProgress(broadcastId, chunks.length);

      if (chunks.length === 0) {
        await Broadcast.findByIdAndUpdate(
          broadcastId,
          {
            $set: {
              status: 'completed',
              completedAt: new Date(),
              updatedAt: new Date()
            }
          },
          { new: false }
        );
        await publishBroadcastEvent({
          userId: String(job.data?.userId || broadcast.createdById || ''),
          payload: {
            type: 'broadcast_updated',
            action: 'completed',
            broadcastId,
            queueSummary: { total: 0, done: 0, failed: 0, processed: 0, complete: true }
          }
        });
        return { plannedChunks: 0 };
      }

      await Broadcast.findByIdAndUpdate(
        broadcastId,
        {
          $set: {
            status: 'sending',
            startedAt: new Date(),
            updatedAt: new Date()
          }
        },
        { new: false }
      );

      await broadcastQueue.addBulk(
        chunks.map((recipientSubset, index) => ({
          name: 'send-broadcast-chunk',
          data: {
            broadcastId,
            userId: String(job.data?.userId || broadcast.createdById || ''),
            companyId: String(job.data?.companyId || broadcast.companyId || ''),
            chunkId: `${broadcastId}:${index}`,
            chunkIndex: index,
            totalChunks: chunks.length,
            recipientSubset,
            reason: job.data?.reason || 'planned'
          },
          opts: {
            jobId: `broadcast:${broadcastId}:chunk:${index}`,
            attempts: 3,
            backoff: {
              type: 'exponential',
              delay: 3000
            }
          }
        }))
      );

      await publishBroadcastEvent({
        userId: String(job.data?.userId || broadcast.createdById || ''),
        payload: {
          type: 'broadcast_updated',
          action: 'queued_chunks',
          broadcastId,
          totalChunks: chunks.length,
          chunkSize
        }
      });

      return { plannedChunks: chunks.length, chunkSize };
    }

    if (job.name === 'send-broadcast-chunk') {
      const broadcast = await Broadcast.findById(broadcastId).lean();
      if (!broadcast) {
        throw new Error(`Broadcast ${broadcastId} not found`);
      }

      const broadcaster = createBufferedBroadcaster(userId || String(broadcast.createdById || ''));
      try {
        const result = await broadcastService.sendBroadcast(
          broadcastId,
          broadcaster,
          null,
          {
            recipientSubset: Array.isArray(job.data?.recipientSubset) ? job.data.recipientSubset : [],
            skipFinalize: true,
            chunkId: String(job.data?.chunkId || job.id),
            chunkIndex: Number(job.data?.chunkIndex || 0)
          }
        );

        await broadcaster.flush();

        const summary = await markBroadcastChunkState(
          broadcastId,
          String(job.data?.chunkId || job.id),
          'done'
        );

        await publishBroadcastEvent({
          userId: userId || String(broadcast.createdById || ''),
          payload: {
            type: 'broadcast_chunk_completed',
            broadcastId,
            chunkId: String(job.data?.chunkId || job.id),
            chunkIndex: Number(job.data?.chunkIndex || 0),
            totalChunks: Number(job.data?.totalChunks || 0),
            summary,
            stats: result?.data?.stats || null
          }
        });

        if (summary.complete) {
          await finalizeBroadcastIfReady(
            broadcastId,
            userId || String(broadcast.createdById || '')
          );
        }

        return {
          broadcastId,
          chunkId: String(job.data?.chunkId || job.id),
          summary
        };
      } catch (error) {
        await broadcaster.flush().catch(() => {});
        const summary = await markBroadcastChunkState(
          broadcastId,
          String(job.data?.chunkId || job.id),
          'failed'
        );

        await publishBroadcastEvent({
          userId: userId || String(broadcast.createdById || ''),
          payload: {
            type: 'broadcast_chunk_failed',
            broadcastId,
            chunkId: String(job.data?.chunkId || job.id),
            chunkIndex: Number(job.data?.chunkIndex || 0),
            totalChunks: Number(job.data?.totalChunks || 0),
            summary,
            error: error.message
          }
        });

        if (summary.complete) {
          await Broadcast.findByIdAndUpdate(
            broadcastId,
            {
              $set: {
                status: 'completed',
                completedAt: new Date(),
                updatedAt: new Date()
              }
            },
            { new: false }
          );
          await clearBroadcastChunkProgress(broadcastId);
        }

        throw error;
      }
    }

    throw new Error(`Unsupported broadcast job type: ${job.name}`);
  },
  {
    connection,
    concurrency: workerConcurrency,
    limiter: {
      max: workerLimiterMax,
      duration: workerLimiterDuration
    }
  }
);

broadcastWorker.on('completed', (job) => {
  console.log(`Broadcast job completed: ${job.id}`);
});

broadcastWorker.on('failed', async (job, error) => {
  console.error(`Broadcast job failed: ${job?.id || 'unknown'}`, error?.message || error);
});

const shutdown = async () => {
  await broadcastWorker.close();
  await connection.quit();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(
  `Broadcast worker started with concurrency=${workerConcurrency}, limiter=${workerLimiterMax}/${workerLimiterDuration}ms`
);
