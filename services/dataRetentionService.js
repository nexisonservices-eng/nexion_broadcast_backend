const Message = require('../models/Message');
const BroadcastDispatch = require('../models/BroadcastDispatch');
const MessageArchive = require('../models/MessageArchive');
const BroadcastDispatchArchive = require('../models/BroadcastDispatchArchive');

const toPositiveInteger = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
};

const isEnabled = (value) => String(value || '').trim().toLowerCase() === 'true';

const buildCutoff = (days) => new Date(Date.now() - Math.max(0, days) * 24 * 60 * 60 * 1000);

const archiveCollectionBatch = async ({
  sourceModel,
  archiveModel,
  query,
  batchSize,
  projection,
  mapDoc
}) => {
  let archived = 0;

  while (true) {
    const docs = await sourceModel.find(query).select(projection).sort({ createdAt: 1, _id: 1 }).limit(batchSize).lean();
    if (!docs.length) break;

    const archiveDocs = docs
      .map(mapDoc)
      .filter(Boolean);

    if (archiveDocs.length) {
      await archiveModel.insertMany(archiveDocs, { ordered: false });
      archived += archiveDocs.length;
    }

    await sourceModel.deleteMany({ _id: { $in: docs.map((doc) => doc._id) } });
  }

  return archived;
};

const archiveOldMessages = async () => {
  if (!isEnabled(process.env.MESSAGE_ARCHIVE_ENABLED)) return { archived: 0 };

  const retentionDays = toPositiveInteger(process.env.MESSAGE_ARCHIVE_RETENTION_DAYS, 180);
  const batchSize = Math.max(25, toPositiveInteger(process.env.MESSAGE_ARCHIVE_BATCH_SIZE, 500));
  const cutoff = buildCutoff(retentionDays);
  const query = {
    timestamp: { $lt: cutoff },
    status: { $in: ['sent', 'delivered', 'read', 'failed', 'received'] }
  };

  const archived = await archiveCollectionBatch({
    sourceModel: Message,
    archiveModel: MessageArchive,
    query,
    batchSize,
    projection:
      '_id userId companyId conversationId sender senderName senderId text mediaUrl mediaType mediaCaption mediaPipelineRequestId attachment status whatsappMessageId whatsappTimestamp errorMessage errorCode broadcastId broadcastDispatchKey rawMessageType reactionEmoji whatsappContextMessageId interactionType interactionId interactionTitle timestamp isForwarded forwardedFrom replyTo leadScoring createdAt updatedAt',
    mapDoc: (doc) => ({
      sourceId: doc._id,
      companyId: doc.companyId || null,
      userId: doc.userId || null,
      conversationId: doc.conversationId || null,
      originalCreatedAt: doc.createdAt || doc.timestamp || null,
      payload: doc
    })
  });

  return { archived };
};

const archiveOldBroadcastDispatches = async () => {
  if (!isEnabled(process.env.BROADCAST_DISPATCH_ARCHIVE_ENABLED)) return { archived: 0 };

  const retentionDays = toPositiveInteger(
    process.env.BROADCAST_DISPATCH_ARCHIVE_RETENTION_DAYS,
    30
  );
  const batchSize = Math.max(
    25,
    toPositiveInteger(process.env.BROADCAST_DISPATCH_ARCHIVE_BATCH_SIZE, 500)
  );
  const cutoff = buildCutoff(retentionDays);
  const query = {
    createdAt: { $lt: cutoff },
    status: { $in: ['sent', 'failed', 'suppressed', 'skipped'] }
  };

  const archived = await archiveCollectionBatch({
    sourceModel: BroadcastDispatch,
    archiveModel: BroadcastDispatchArchive,
    query,
    batchSize,
    projection:
      '_id broadcastDispatchKey broadcastId userId companyId recipientPhone status claimedAt sentAt failedAt whatsappMessageId messageText messageKind templateName templateLanguage conversationId messageId errorMessage retryCount lastAttemptAt chunkId chunkIndex recipientIndex createdAt updatedAt',
    mapDoc: (doc) => ({
      sourceId: doc._id,
      broadcastId: doc.broadcastId || null,
      companyId: doc.companyId || null,
      userId: doc.userId || null,
      originalCreatedAt: doc.createdAt || null,
      payload: doc
    })
  });

  return { archived };
};

const runDataRetentionMaintenance = async () => {
  const [messageResult, broadcastDispatchResult] = await Promise.all([
    archiveOldMessages(),
    archiveOldBroadcastDispatches()
  ]);

  return {
    messagesArchived: Number(messageResult?.archived || 0),
    broadcastDispatchesArchived: Number(broadcastDispatchResult?.archived || 0)
  };
};

module.exports = {
  archiveOldMessages,
  archiveOldBroadcastDispatches,
  runDataRetentionMaintenance
};
