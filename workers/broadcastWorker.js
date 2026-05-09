require('dotenv').config();

const { Worker } = require('bullmq');
const Broadcast = require('../models/Broadcast');
const LeadActivity = require('../models/LeadActivity');
const broadcastService = require('../services/broadcastService');
const {
  connection,
  broadcastQueue,
  initializeBroadcastChunkProgress,
  markBroadcastChunkState,
  getBroadcastChunkProgressSummary,
  clearBroadcastChunkProgress
} = require('../queues/broadcastQueue');
const {
  queueName: broadcastInboxQueueName,
  connection: broadcastInboxConnection
} = require('../queues/broadcastInboxQueue');
const { publishBroadcastEvent } = require('../realtime/broadcastEventBus');
const { applyMarketingTemplateSent } = require('../services/whatsappOutreach/policy');

const workerConcurrency = Math.max(1, Number(process.env.BROADCAST_WORKER_CONCURRENCY || 2));
const workerLimiterMax = Math.max(1, Number(process.env.BROADCAST_WORKER_RATE_MAX || 10));
const workerLimiterDuration = Math.max(1000, Number(process.env.BROADCAST_WORKER_RATE_DURATION || 1000));
const chunkSize = Math.max(1, Math.min(250, Number(process.env.BROADCAST_CHUNK_SIZE || 100)));
const progressBatchSize = Math.max(1, Number(process.env.BROADCAST_PROGRESS_BATCH_SIZE || 25));
const progressFlushMs = Math.max(250, Number(process.env.BROADCAST_PROGRESS_FLUSH_MS || 1000));
const inboxEventBatchSize = Math.max(1, Number(process.env.BROADCAST_INBOX_EVENT_BATCH_SIZE || 25));
const inboxEventFlushMs = Math.max(250, Number(process.env.BROADCAST_INBOX_EVENT_FLUSH_MS || 1000));
const leadActivityBatchSize = Math.max(1, Number(process.env.BROADCAST_LEAD_ACTIVITY_BATCH_SIZE || 25));
const leadActivityFlushMs = Math.max(250, Number(process.env.BROADCAST_LEAD_ACTIVITY_FLUSH_MS || 1000));

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

const createBufferedInboxNotifier = (userId) => {
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
        broadcastId: events[0]?.broadcastId || null,
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
        console.error('Failed to flush broadcast inbox batch:', error.message);
      }
    }, inboxEventFlushMs);
    flushTimer.unref?.();
  };

  const notifier = (payload) => {
    if (!payload) return;
    buffer.push(payload);
    if (buffer.length >= inboxEventBatchSize) {
      flushing = flushing
        .then(() => flush())
        .catch((error) => {
          console.error('Failed to publish broadcast inbox batch:', error.message);
        });
      return;
    }
    scheduleFlush();
  };

  notifier.flush = async () => {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    await flushing;
    await flush();
  };

  return notifier;
};

const inboxNotifierMap = new Map();

const getInboxNotifier = (userId) => {
  const normalizedUserId = String(userId || '').trim();
  if (!normalizedUserId) return null;

  let notifier = inboxNotifierMap.get(normalizedUserId);
  if (!notifier) {
    notifier = createBufferedInboxNotifier(normalizedUserId);
    inboxNotifierMap.set(normalizedUserId, notifier);
  }

  return notifier;
};

const flushAllInboxNotifiers = async () => {
  await Promise.allSettled(
    Array.from(inboxNotifierMap.values()).map((notifier) =>
      typeof notifier?.flush === 'function' ? notifier.flush() : Promise.resolve()
    )
  );
};

const leadActivityBufferMap = new Map();

const createLeadActivityBuffer = () => {
  const buffer = [];
  let flushTimer = null;
  let flushing = Promise.resolve();

  const flush = async () => {
    if (!buffer.length) return;
    const activities = buffer.splice(0, buffer.length);
    const ops = activities.map((activity) => ({
      insertOne: {
        document: activity
      }
    }));
    await LeadActivity.bulkWrite(ops, { ordered: false });
  };

  const scheduleFlush = () => {
    if (flushTimer) return;
    flushTimer = setTimeout(async () => {
      flushTimer = null;
      try {
        await flush();
      } catch (error) {
        console.error('Failed to flush broadcast lead activities:', error.message);
      }
    }, leadActivityFlushMs);
    flushTimer.unref?.();
  };

  const enqueue = (activity) => {
    if (!activity) return;
    buffer.push(activity);
    if (buffer.length >= leadActivityBatchSize) {
      flushing = flushing
        .then(() => flush())
        .catch((error) => {
          console.error('Failed to bulk write broadcast lead activities:', error.message);
        });
      return;
    }
    scheduleFlush();
  };

  enqueue.flush = async () => {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    await flushing;
    await flush();
  };

  return enqueue;
};

const getLeadActivityBuffer = (userId) => {
  const normalizedUserId = String(userId || '').trim();
  if (!normalizedUserId) return null;

  let buffer = leadActivityBufferMap.get(normalizedUserId);
  if (!buffer) {
    buffer = createLeadActivityBuffer();
    leadActivityBufferMap.set(normalizedUserId, buffer);
  }

  return buffer;
};

const flushAllLeadActivities = async () => {
  await Promise.allSettled(
    Array.from(leadActivityBufferMap.values()).map((buffer) =>
      typeof buffer?.flush === 'function' ? buffer.flush() : Promise.resolve()
    )
  );
};

const finalizeBroadcastIfReady = async (broadcastId, userId) => {
  const summary = await getBroadcastChunkProgressSummary(broadcastId);

  if (!summary.complete) {
    return { complete: false, summary };
  }

  const finalStatus = Number(summary.failed || 0) > 0 ? 'completed_with_errors' : 'completed';

  const broadcast = await Broadcast.findByIdAndUpdate(
    broadcastId,
    {
      $set: {
        status: finalStatus,
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
        action: finalStatus,
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

      const recipients = await broadcastService.resolveBroadcastAudienceRecipients({
        broadcast,
        userId: broadcast.createdById,
        companyId: broadcast.companyId
      });
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

      try {
        const result = await broadcastService.sendBroadcast(
          broadcastId,
          null,
          null,
          {
            recipientSubset: Array.isArray(job.data?.recipientSubset) ? job.data.recipientSubset : [],
            skipFinalize: true,
            chunkId: String(job.data?.chunkId || job.id),
            chunkIndex: Number(job.data?.chunkIndex || 0)
          }
        );

        if (result?.rateLimited) {
          const retryAfterMs = Math.max(1000, Number(result.retryAfterMs || 0) || 1000);
          if (broadcastWorker && typeof broadcastWorker.rateLimit === 'function') {
            await broadcastWorker.rateLimit(retryAfterMs);
          }
          throw Worker.RateLimitError();
        }

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
        if (String(error?.message || '') === 'bullmq:rateLimitExceeded') {
          throw error;
        }
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
          const finalStatus = Number(summary.failed || 0) > 0 ? 'completed_with_errors' : 'completed';
          await Broadcast.findByIdAndUpdate(
            broadcastId,
            {
              $set: {
                status: finalStatus,
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

const broadcastInboxWorker = new Worker(
  broadcastInboxQueueName,
  async (job) => {
    const {
      broadcastId,
      userId,
      companyId,
      phoneNumber,
      message,
      whatsappResponse,
      broadcastDispatchKey = '',
      templateCategory = '',
      contactId = ''
    } = job.data || {};

    const resolvedContact =
      contactId
        ? await broadcastService.resolveContactForRecipient({
            userId,
            companyId,
            phone: phoneNumber
          })
        : null;

    const { conversation, message: savedMessage } = await broadcastService.updateConversation(
      phoneNumber,
      message,
      whatsappResponse,
      broadcastId,
      userId,
      companyId,
      broadcastDispatchKey
    );

    let contact = resolvedContact;
    if (!contact && conversation?.contactId) {
      contact = await broadcastService.resolveContactForRecipient({
        userId,
        companyId,
        phone: phoneNumber
      });
    }

    if (contact && String(templateCategory || '').toLowerCase() === 'marketing') {
      applyMarketingTemplateSent(contact, { now: new Date() });
      await contact.save();
    }

    if (contact && conversation && savedMessage) {
      const activityBuffer = getLeadActivityBuffer(userId);
      if (activityBuffer) {
        activityBuffer({
          userId,
          companyId: companyId || null,
          contactId: contact._id,
          conversationId: conversation._id || null,
          type: 'broadcast_sent',
          meta: {
            broadcastId,
            broadcastName: '',
            messageType: 'text',
            templateName: '',
            templateCategory,
            messagePreview: String(message || '').trim().slice(0, 280)
          },
          createdBy: String(userId || '').trim() || null
        });
      }
    }

    if (conversation && savedMessage) {
      const inboxNotifier = getInboxNotifier(userId);
      if (inboxNotifier) {
        inboxNotifier({
          broadcastId,
          conversationId: String(conversation?._id || ''),
          messageId: String(savedMessage?._id || ''),
          whatsappMessageId: String(savedMessage?.whatsappMessageId || ''),
          phoneNumber: String(phoneNumber || ''),
          status: String(savedMessage?.status || 'sent'),
          timestamp: String(savedMessage?.timestamp || new Date().toISOString())
        });
      }
    }

    return {
      success: Boolean(conversation && savedMessage),
      conversationId: String(conversation?._id || ''),
      messageId: String(savedMessage?._id || '')
    };
  },
  {
    connection: broadcastInboxConnection,
    concurrency: Math.max(2, Math.min(12, Number(process.env.BROADCAST_INBOX_WORKER_CONCURRENCY || 6)))
  }
);

broadcastWorker.on('completed', (job) => {
  console.log(`Broadcast job completed: ${job.id}`);
});

broadcastWorker.on('failed', async (job, error) => {
  console.error(`Broadcast job failed: ${job?.id || 'unknown'}`, error?.message || error);
});

broadcastInboxWorker.on('completed', (job) => {
  console.log(`Broadcast inbox job completed: ${job.id}`);
});

broadcastInboxWorker.on('failed', async (job, error) => {
  console.error(`Broadcast inbox job failed: ${job?.id || 'unknown'}`, error?.message || error);
});

const shutdown = async () => {
  await flushAllInboxNotifiers();
  await flushAllLeadActivities();
  await broadcastWorker.close();
  await broadcastInboxWorker.close();
  await connection.quit();
  await broadcastInboxConnection.quit();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(
  `Broadcast worker started with concurrency=${workerConcurrency}, limiter=${workerLimiterMax}/${workerLimiterDuration}ms`
);
