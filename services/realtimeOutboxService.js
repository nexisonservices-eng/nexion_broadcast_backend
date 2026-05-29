const crypto = require('crypto');
const RealtimeOutboxEvent = require('../models/RealtimeOutboxEvent');

const DEFAULT_LOCK_TTL_MS = Math.max(15_000, Number(process.env.REALTIME_OUTBOX_LOCK_TTL_MS || 45_000));
const DEFAULT_MAX_ATTEMPTS = Math.max(3, Number(process.env.REALTIME_OUTBOX_MAX_ATTEMPTS || 8));
const DEFAULT_BACKOFF_MS = Math.max(500, Number(process.env.REALTIME_OUTBOX_BACKOFF_MS || 1000));

const toCleanString = (value = '') => String(value || '').trim();

const buildDedupeKey = (event = {}) => {
  const normalized = toCleanString(event?.dedupeKey);
  if (normalized) return normalized;

  const eventType = toCleanString(event?.eventType || 'realtime_event');
  const scope = toCleanString(event?.scope || 'global');
  const userId = toCleanString(event?.userId || '');
  const companyId = toCleanString(event?.companyId || '');
  const conversationId = toCleanString(event?.conversationId || '');
  const room = toCleanString(event?.room || '');
  const payloadFingerprint = crypto
    .createHash('sha1')
    .update(JSON.stringify(event?.payload || {}))
    .digest('hex');

  return [
    eventType,
    scope,
    userId || 'no-user',
    companyId || 'no-company',
    conversationId || 'no-conversation',
    room || 'no-room',
    payloadFingerprint
  ].join(':');
};

const normalizeRealtimeOutboxEvent = (event = {}) => {
  const payload = event?.payload && typeof event.payload === 'object' ? event.payload : {};
  const availableAt = event?.availableAt ? new Date(event.availableAt) : new Date();
  const nextAttemptAt = event?.nextAttemptAt ? new Date(event.nextAttemptAt) : null;
  return {
    eventType: toCleanString(event?.eventType || payload?.type || 'realtime_event') || 'realtime_event',
    scope: toCleanString(event?.scope || payload?.scope || 'global') || 'global',
    userId: toCleanString(event?.userId || payload?.userId || '') || null,
    companyId: toCleanString(event?.companyId || payload?.companyId || '') || null,
    conversationId: toCleanString(event?.conversationId || payload?.conversationId || '') || null,
    room: toCleanString(event?.room || payload?.room || '') || '',
    dedupeKey: buildDedupeKey(event),
    payload,
    status: 'pending',
    priority: Number.isFinite(Number(event?.priority)) ? Number(event.priority) : 0,
    attempts: Number.isFinite(Number(event?.attempts)) ? Number(event.attempts) : 0,
    availableAt: Number.isNaN(availableAt.getTime()) ? new Date() : availableAt,
    lockedAt: null,
    lockOwner: '',
    publishedAt: null,
    lastError: '',
    nextAttemptAt: nextAttemptAt && !Number.isNaN(nextAttemptAt.getTime()) ? nextAttemptAt : null,
    source: toCleanString(event?.source || payload?.source || 'teamInbox') || 'teamInbox'
  };
};

const enqueueRealtimeOutboxEvent = async (event = {}) => {
  if (!event || typeof event !== 'object') return null;
  if (!event.payload || typeof event.payload !== 'object') return null;

  const normalized = normalizeRealtimeOutboxEvent(event);
  const existing = await RealtimeOutboxEvent.findOne({
    dedupeKey: normalized.dedupeKey
  });

  if (existing) {
    if (String(existing.status || '').toLowerCase() === 'published') {
      return existing.toObject ? existing.toObject() : existing;
    }

    existing.eventType = normalized.eventType;
    existing.scope = normalized.scope;
    existing.userId = normalized.userId;
    existing.companyId = normalized.companyId;
    existing.conversationId = normalized.conversationId;
    existing.room = normalized.room;
    existing.payload = normalized.payload;
    existing.priority = normalized.priority;
    existing.availableAt = normalized.availableAt;
    existing.nextAttemptAt = normalized.nextAttemptAt;
    existing.source = normalized.source;
    existing.lastError = '';
    await existing.save();
    return existing.toObject ? existing.toObject() : existing;
  }

  const created = await RealtimeOutboxEvent.create({
    ...normalized,
    status: 'pending'
  });

  return created.toObject ? created.toObject() : created;
};

const claimRealtimeOutboxEvents = async ({
  limit = 20,
  lockOwner = '',
  lockTtlMs = DEFAULT_LOCK_TTL_MS
} = {}) => {
  const normalizedLimit = Math.max(1, Math.min(Number(limit) || 20, 100));
  const normalizedLockOwner = toCleanString(lockOwner) || `realtime-outbox-${crypto.randomUUID()}`;
  const now = new Date();
  const staleLockThreshold = new Date(now.getTime() - Math.max(5000, Number(lockTtlMs) || DEFAULT_LOCK_TTL_MS));
  const events = [];

  for (let index = 0; index < normalizedLimit; index += 1) {
    const claimed = await RealtimeOutboxEvent.findOneAndUpdate(
      {
        status: { $in: ['pending', 'processing'] },
        availableAt: { $lte: now },
        $and: [
          {
            $or: [
              { nextAttemptAt: null },
              { nextAttemptAt: { $exists: false } },
              { nextAttemptAt: { $lte: now } }
            ]
          },
          {
            $or: [
              { lockedAt: null },
              { lockedAt: { $exists: false } },
              { lockedAt: { $lte: staleLockThreshold } }
            ]
          }
        ]
      },
      {
        $set: {
          status: 'processing',
          lockedAt: now,
          lockOwner: normalizedLockOwner,
          updatedAt: now
        }
      },
      {
        new: true,
        sort: { priority: -1, availableAt: 1, createdAt: 1 }
      }
    ).lean();

    if (!claimed) break;
    events.push(claimed);
  }

  return events;
};

const markRealtimeOutboxEventPublished = async ({ id, lockOwner = '' } = {}) => {
  const normalizedId = toCleanString(id);
  if (!normalizedId) return null;

  const updated = await RealtimeOutboxEvent.findOneAndUpdate(
    {
      _id: normalizedId,
      ...(toCleanString(lockOwner) ? { lockOwner: toCleanString(lockOwner) } : {})
    },
    {
      $set: {
        status: 'published',
        publishedAt: new Date(),
        lockedAt: null,
        lockOwner: '',
        lastError: '',
        updatedAt: new Date()
      }
    },
    { new: true }
  ).lean();

  return updated;
};

const rescheduleRealtimeOutboxEvent = async ({
  id,
  lockOwner = '',
  error = null,
  attempt = null,
  maxAttempts = DEFAULT_MAX_ATTEMPTS
} = {}) => {
  const normalizedId = toCleanString(id);
  if (!normalizedId) return null;

  const currentAttempt = Number.isFinite(Number(attempt)) ? Number(attempt) : 0;
  const nextAttempt = currentAttempt + 1;
  const nextDelayMs = Math.min(
    60_000,
    DEFAULT_BACKOFF_MS * Math.max(1, 2 ** Math.max(0, nextAttempt - 1))
  );
  const status = nextAttempt >= maxAttempts ? 'dead' : 'pending';
  const updated = await RealtimeOutboxEvent.findOneAndUpdate(
    {
      _id: normalizedId,
      ...(toCleanString(lockOwner) ? { lockOwner: toCleanString(lockOwner) } : {})
    },
    {
      $set: {
        status,
        lockedAt: null,
        lockOwner: '',
        lastError: toCleanString(error?.message || error || 'Realtime outbox publish failed'),
        nextAttemptAt: status === 'dead' ? null : new Date(Date.now() + nextDelayMs),
        updatedAt: new Date()
      },
      $inc: {
        attempts: 1
      }
    },
    { new: true }
  ).lean();

  return updated;
};

const purgePublishedRealtimeOutboxEvents = async ({
  olderThanDays = 3,
} = {}) => {
  const retentionDays = Math.max(1, Number(olderThanDays) || 3);
  const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
  return RealtimeOutboxEvent.deleteMany({
    status: 'published',
    publishedAt: { $lt: cutoff }
  });
};

const summarizeStatusBucket = async ({
  status,
  now = new Date(),
  lockTtlMs = DEFAULT_LOCK_TTL_MS,
  includeRetryPressure = false
} = {}) => {
  const statusFilter = Array.isArray(status) ? { $in: status } : status;
  const matchStage = {
    status: statusFilter
  };

  if (includeRetryPressure) {
    matchStage.$or = [
      { nextAttemptAt: null },
      { nextAttemptAt: { $exists: false } },
      { nextAttemptAt: { $lte: now } }
    ];
  }

  const pipeline = [
    { $match: matchStage },
    { $sort: { availableAt: 1, createdAt: 1, _id: 1 } },
    {
      $group: {
        _id: null,
        count: { $sum: 1 },
        oldestCreatedAt: { $first: '$createdAt' },
        oldestAvailableAt: { $first: '$availableAt' },
        oldestLockedAt: { $first: '$lockedAt' },
        newestUpdatedAt: { $last: '$updatedAt' }
      }
    }
  ];

  const [bucket] = await RealtimeOutboxEvent.aggregate(pipeline);
  const normalizedBucket = bucket || {
    count: 0,
    oldestCreatedAt: null,
    oldestAvailableAt: null,
    oldestLockedAt: null,
    newestUpdatedAt: null
  };

  const oldestQueuedAt = normalizedBucket.oldestAvailableAt || normalizedBucket.oldestCreatedAt;
  const oldestQueuedAgeMs = oldestQueuedAt ? Math.max(0, now.getTime() - new Date(oldestQueuedAt).getTime()) : 0;
  const oldestLockAgeMs = normalizedBucket.oldestLockedAt
    ? Math.max(0, now.getTime() - new Date(normalizedBucket.oldestLockedAt).getTime())
    : 0;

  return {
    count: normalizedBucket.count || 0,
    oldestCreatedAt: normalizedBucket.oldestCreatedAt || null,
    oldestAvailableAt: normalizedBucket.oldestAvailableAt || null,
    oldestLockedAt: normalizedBucket.oldestLockedAt || null,
    newestUpdatedAt: normalizedBucket.newestUpdatedAt || null,
    oldestQueuedAgeMs,
    oldestLockAgeMs,
    lockTtlMs: Math.max(5000, Number(lockTtlMs) || DEFAULT_LOCK_TTL_MS)
  };
};

const getRealtimeOutboxHealth = async ({
  lockTtlMs = DEFAULT_LOCK_TTL_MS,
  publishedRetentionDays = 3
} = {}) => {
  const now = new Date();
  const staleLockThreshold = new Date(now.getTime() - Math.max(5000, Number(lockTtlMs) || DEFAULT_LOCK_TTL_MS));
  const publishedSince = new Date(now.getTime() - Math.max(1, Number(publishedRetentionDays) || 3) * 24 * 60 * 60 * 1000);

  const [
    pending,
    processing,
    dead,
    failed,
    publishedRecentCount,
    dueCount,
    retryingCount,
    staleProcessingCount
  ] = await Promise.all([
    summarizeStatusBucket({ status: 'pending', now, lockTtlMs }),
    summarizeStatusBucket({ status: 'processing', now, lockTtlMs }),
    summarizeStatusBucket({ status: 'dead', now, lockTtlMs }),
    summarizeStatusBucket({ status: 'failed', now, lockTtlMs }),
    RealtimeOutboxEvent.countDocuments({
      status: 'published',
      publishedAt: { $gte: publishedSince }
    }),
    RealtimeOutboxEvent.countDocuments({
      status: { $in: ['pending', 'processing'] },
      availableAt: { $lte: now },
      $or: [
        { nextAttemptAt: null },
        { nextAttemptAt: { $exists: false } },
        { nextAttemptAt: { $lte: now } }
      ]
    }),
    RealtimeOutboxEvent.countDocuments({
      status: 'pending',
      attempts: { $gt: 0 }
    }),
    RealtimeOutboxEvent.countDocuments({
      status: 'processing',
      lockedAt: { $lte: staleLockThreshold }
    })
  ]);

  return {
    now: now.toISOString(),
    lockTtlMs: Math.max(5000, Number(lockTtlMs) || DEFAULT_LOCK_TTL_MS),
    publishedRetentionDays: Math.max(1, Number(publishedRetentionDays) || 3),
    queue: {
      pending,
      processing,
      dead,
      failed,
      dueCount,
      retryingCount,
      staleProcessingCount
    },
    throughput: {
      publishedRecentCount,
      publishedWindowStart: publishedSince.toISOString()
    },
    derived: {
      backlogCount: pending.count + processing.count,
      unhealthyCount: dead.count + failed.count + staleProcessingCount,
      oldestPendingAgeMs: pending.oldestQueuedAgeMs,
      oldestProcessingAgeMs: processing.oldestLockAgeMs,
      oldestDeadAgeMs: dead.oldestQueuedAgeMs,
      oldestFailedAgeMs: failed.oldestQueuedAgeMs
    }
  };
};

module.exports = {
  claimRealtimeOutboxEvents,
  enqueueRealtimeOutboxEvent,
  getRealtimeOutboxHealth,
  markRealtimeOutboxEventPublished,
  purgePublishedRealtimeOutboxEvents,
  rescheduleRealtimeOutboxEvent,
  toCleanString
};
