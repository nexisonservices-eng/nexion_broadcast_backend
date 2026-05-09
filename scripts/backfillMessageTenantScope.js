require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');

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

const buildMissingCompanyFilter = () => ({
  $or: [{ companyId: { $exists: false } }, { companyId: null }, { companyId: '' }]
});

const run = async () => {
  const batchSize = parsePositiveInt(
    readArg(['--batch-size', '--batchSize']) || process.env.MESSAGE_TENANT_SCOPE_BACKFILL_BATCH_SIZE,
    250
  );
  const limit = parsePositiveInt(readArg(['--limit']), 0);
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const dryRun =
    hasFlag(['--dry-run', '--dryRun']) ||
    String(process.env.MESSAGE_TENANT_SCOPE_BACKFILL_DRY_RUN || '').trim().toLowerCase() === 'true';

  await connectDB();

  const conversationFilters = buildMissingCompanyFilter();
  if (userId) {
    conversationFilters.userId = userId;
  }

  console.log(
    JSON.stringify(
      {
        event: 'message_tenant_scope_backfill_start',
        batchSize,
        limit: limit || null,
        dryRun,
        filters: {
          userId: userId ? String(userId) : null
        }
      },
      null,
      2
    )
  );

  const stats = {
    scannedConversations: 0,
    updatedConversations: 0,
    scannedMessages: 0,
    updatedMessages: 0,
    derivedConversationCompanyIds: 0
  };

  const conversationCursor = Conversation.find(conversationFilters)
    .select('_id userId companyId')
    .sort({ _id: 1 })
    .lean()
    .cursor({ batchSize });

  for await (const conversation of conversationCursor) {
    stats.scannedConversations += 1;
    if (limit > 0 && stats.scannedConversations > limit) break;

    const conversationId = conversation?._id;
    if (!conversationId) continue;

    let nextCompanyId = conversation.companyId || null;
    if (!nextCompanyId) {
      const scopedMessage = await Message.findOne({
        conversationId,
        companyId: { $exists: true, $nin: [null, ''] }
      })
        .select('companyId')
        .sort({ timestamp: -1, _id: -1 })
        .lean();

      if (scopedMessage?.companyId) {
        nextCompanyId = scopedMessage.companyId;
        stats.derivedConversationCompanyIds += 1;
      }
    }

    if (nextCompanyId && !String(conversation.companyId || '').trim()) {
      stats.updatedConversations += 1;
      if (!dryRun) {
        await Conversation.updateOne(
          { _id: conversationId },
          { $set: { companyId: nextCompanyId } }
        );
      }
    }
  }

  const messageFilters = buildMissingCompanyFilter();
  if (userId) {
    messageFilters.userId = userId;
  }

  const messageCursor = Message.find(messageFilters)
    .select('_id conversationId companyId')
    .sort({ _id: 1 })
    .lean()
    .cursor({ batchSize });

  for await (const message of messageCursor) {
    stats.scannedMessages += 1;
    if (limit > 0 && stats.scannedMessages > limit) break;

    const messageId = message?._id;
    const conversationId = message?.conversationId;
    if (!messageId || !conversationId || message.companyId) continue;

    const conversation = await Conversation.findOne({ _id: conversationId })
      .select('companyId')
      .lean();

    const conversationCompanyId = conversation?.companyId || null;
    if (!conversationCompanyId) continue;

    stats.updatedMessages += 1;
    if (!dryRun) {
      await Message.updateOne({ _id: messageId }, { $set: { companyId: conversationCompanyId } });
    }
  }

  console.log(
    JSON.stringify(
      {
        event: 'message_tenant_scope_backfill_complete',
        ...stats,
        dryRun
      },
      null,
      2
    )
  );

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Message tenant scope backfill failed:', error);
  try {
    await mongoose.connection.close();
  } catch (_closeError) {
    // ignore close failures
  }
  process.exitCode = 1;
});
