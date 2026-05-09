require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Conversation = require('../models/Conversation');
const {
  bulkUpsertConversationSummaries
} = require('../services/conversationSummaryService');

const SUMMARY_SELECT =
  '_id userId companyId contactId contactPhone contactName status assignedTo assignedToId tags priority lastMessageTime lastMessage lastMessageMediaType lastMessageAttachmentName lastMessageAttachmentPages lastMessageFrom lastMessageWhatsappMessageId lastMessageStatus unreadCount notes resolvedAt createdAt updatedAt';

const readArg = (names = []) => {
  const args = process.argv.slice(2);
  for (const name of names) {
    const eqMatch = args.find((arg) => arg.startsWith(`${name}=`));
    if (eqMatch) {
      return eqMatch.slice(name.length + 1);
    }

    const index = args.indexOf(name);
    if (index >= 0 && args[index + 1] && !String(args[index + 1]).startsWith('--')) {
      return args[index + 1];
    }
  }
  return '';
};

const hasFlag = (names = []) => {
  const args = process.argv.slice(2);
  return names.some((name) => args.includes(name) || args.some((arg) => arg.startsWith(`${name}=`)));
};

const parsePositiveInt = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
};

const parseObjectId = (value, label) => {
  const normalized = String(value || '').trim();
  if (!normalized) return null;
  if (!mongoose.Types.ObjectId.isValid(normalized)) {
    throw new Error(`${label} must be a valid ObjectId`);
  }
  return new mongoose.Types.ObjectId(normalized);
};

const buildSummaryPayload = (conversation = {}) => ({
  conversationId: conversation?._id,
  userId: conversation?.userId,
  companyId: conversation?.companyId,
  contactId: conversation?.contactId,
  contactPhone: conversation?.contactPhone,
  contactName: conversation?.contactName,
  status: conversation?.status,
  assignedTo: conversation?.assignedTo,
  assignedToId: conversation?.assignedToId,
  tags: Array.isArray(conversation?.tags) ? conversation.tags : [],
  priority: conversation?.priority,
  lastMessageTime:
    conversation?.lastMessageTime || conversation?.updatedAt || conversation?.createdAt || null,
  lastMessage: conversation?.lastMessage,
  lastMessageMediaType: conversation?.lastMessageMediaType,
  lastMessageAttachmentName: conversation?.lastMessageAttachmentName,
  lastMessageAttachmentPages: conversation?.lastMessageAttachmentPages,
  lastMessageFrom: conversation?.lastMessageFrom,
  lastMessageWhatsappMessageId: conversation?.lastMessageWhatsappMessageId,
  lastMessageStatus: conversation?.lastMessageStatus,
  unreadCount: conversation?.unreadCount,
  notes: conversation?.notes,
  resolvedAt: conversation?.resolvedAt
});

const flushBatch = async ({ batch, stats, dryRun }) => {
  if (!batch.length) return;

  stats.batches += 1;
  stats.processed += batch.length;

  if (dryRun) {
    batch.length = 0;
    return;
  }

  await bulkUpsertConversationSummaries(batch);
  batch.length = 0;
};

const run = async () => {
  const batchSize = parsePositiveInt(
    readArg(['--batch-size', '--batchSize']) || process.env.CONVERSATION_SUMMARY_BACKFILL_BATCH_SIZE,
    500
  );
  const limit = parsePositiveInt(readArg(['--limit']), 0);
  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const status = String(readArg(['--status']) || '').trim();
  const dryRun =
    hasFlag(['--dry-run', '--dryRun']) ||
    String(process.env.CONVERSATION_SUMMARY_BACKFILL_DRY_RUN || '').trim().toLowerCase() === 'true';

  await connectDB();

  const filters = {};
  if (companyId) filters.companyId = companyId;
  if (userId) filters.userId = userId;
  if (status) filters.status = status;

  console.log(
    JSON.stringify(
      {
        event: 'conversation_summary_backfill_start',
        batchSize,
        limit: limit || null,
        dryRun,
        filters: {
          companyId: companyId ? String(companyId) : null,
          userId: userId ? String(userId) : null,
          status: status || null
        }
      },
      null,
      2
    )
  );

  const cursor = Conversation.find(filters)
    .select(SUMMARY_SELECT)
    .sort({ _id: 1 })
    .lean()
    .cursor({ batchSize });

  const stats = {
    scanned: 0,
    processed: 0,
    batches: 0
  };

  const batch = [];
  for await (const conversation of cursor) {
    stats.scanned += 1;
    batch.push(buildSummaryPayload(conversation));

    if (limit > 0 && stats.scanned >= limit) {
      break;
    }

    if (batch.length >= batchSize) {
      await flushBatch({ batch, stats, dryRun });
      if (stats.scanned % Math.max(batchSize * 5, 1000) === 0) {
        console.log(
          JSON.stringify(
            {
              event: 'conversation_summary_backfill_progress',
              scanned: stats.scanned,
              processed: stats.processed,
              batches: stats.batches
            },
            null,
            2
          )
        );
      }
    }
  }

  await flushBatch({ batch, stats, dryRun });

  console.log(
    JSON.stringify(
      {
        event: 'conversation_summary_backfill_complete',
        scanned: stats.scanned,
        processed: stats.processed,
        batches: stats.batches,
        dryRun
      },
      null,
      2
    )
  );

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Conversation summary backfill failed:', error);
  try {
    await mongoose.connection.close();
  } catch (_closeError) {
    // ignore close failures
  }
  process.exitCode = 1;
});
