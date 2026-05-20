require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const ConversationSummary = require('../models/ConversationSummary');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const Message = require('../models/Message');
const { buildInboxSearchPlan } = require('../utils/inboxSearchPlan');
const { buildMessageCursorFilter, decodeMessageCursor, normalizePageLimit } = require('../utils/threadPagination');

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

const parseObjectId = (value, label) => {
  const normalized = String(value || '').trim();
  if (!normalized) return null;
  if (!mongoose.Types.ObjectId.isValid(normalized)) {
    throw new Error(`${label} must be a valid ObjectId`);
  }
  return new mongoose.Types.ObjectId(normalized);
};

const summarizePlan = (explain = {}) => {
  const winningPlan = explain?.queryPlanner?.winningPlan || {};
  const executionStats = explain?.executionStats || {};

  const walkStages = (node, found = []) => {
    if (!node || typeof node !== 'object') return found;
    if (node.stage) {
      found.push({ stage: node.stage, indexName: node.indexName || null });
    }
    if (node.inputStage) walkStages(node.inputStage, found);
    if (Array.isArray(node.inputStages)) node.inputStages.forEach((child) => walkStages(child, found));
    if (node.shards && typeof node.shards === 'object') {
      Object.values(node.shards).forEach((child) => walkStages(child, found));
    }
    if (node.queryPlan) walkStages(node.queryPlan, found);
    return found;
  };

  const stagePath = walkStages(winningPlan);
  const ixscanStage = stagePath.find((entry) => entry.stage === 'IXSCAN') || null;
  const leafStage = stagePath[stagePath.length - 1] || null;

  return {
    stage: leafStage?.stage || winningPlan.stage || 'unknown',
    indexName: ixscanStage?.indexName || leafStage?.indexName || null,
    stagePath,
    nReturned: executionStats.nReturned ?? null,
    totalDocsExamined: executionStats.totalDocsExamined ?? null,
    totalKeysExamined: executionStats.totalKeysExamined ?? null,
    executionTimeMillis: executionStats.executionTimeMillis ?? null
  };
};

const printPlan = async (label, query) => {
  const explain = await query.explain('executionStats');
  console.log(`\n## ${label}`);
  console.log(JSON.stringify(summarizePlan(explain), null, 2));
};

const run = async () => {
  await connectDB();

  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const conversationId = parseObjectId(readArg(['--conversation-id', '--conversationId']), 'conversationId');
  const search = String(readArg(['--search']) || '').trim();
  const cursor = decodeMessageCursor(readArg(['--cursor']) || '');
  const limit = normalizePageLimit(readArg(['--limit']) || 20);

  const tenantFilter = {};
  if (companyId) tenantFilter.companyId = companyId;
  if (userId) tenantFilter.userId = userId;

  await printPlan(
    'Conversation summary page',
    ConversationSummary.find(tenantFilter)
      .select('_id conversationId contactId contactPhone contactName status assignedTo lastMessageTime unreadCount')
      .sort({ lastMessageTime: -1, _id: -1 })
      .limit(limit)
  );

  if (search) {
    const searchPlan = buildInboxSearchPlan(search);
    const summaryQuery = ConversationSummary.find({
      ...tenantFilter,
      ...(searchPlan.summaryClause || {})
    })
      .select('_id conversationId contactPhone contactName lastMessage lastMessageTime unreadCount')
      .sort({ lastMessageTime: -1, _id: -1 })
      .limit(limit);
    if (searchPlan.hint) {
      summaryQuery.hint(searchPlan.hint);
    }
    await printPlan(`Conversation summary search (${searchPlan.mode || 'none'})`, summaryQuery);
  }

  if (conversationId) {
    const conversation = await Conversation.findOne({
      ...tenantFilter,
      _id: conversationId
    })
      .select('_id userId companyId contactId contactPhone contactName status assignedTo lastMessageTime lastMessage unreadCount createdAt updatedAt')
      .lean();

    await printPlan(
      'Conversation by id',
      Conversation.find({
        ...tenantFilter,
        _id: conversationId
      })
        .select('_id userId companyId contactId contactPhone contactName status assignedTo lastMessageTime lastMessage unreadCount createdAt updatedAt')
        .lean()
    );

    if (conversation?.contactId) {
      await printPlan(
        'Conversation contact hydrate',
        Contact.find({
          ...tenantFilter,
          _id: conversation.contactId
        })
          .select('_id name phone email tags status stage leadScore whatsappOptInStatus lastInboundMessageAt serviceWindowClosesAt')
          .lean()
      );
    }

    const threadFilters = {
      ...tenantFilter,
      conversationId,
      ...(cursor ? buildMessageCursorFilter(cursor) : {})
    };
    await printPlan(
      'Conversation thread page',
      Message.find(threadFilters)
        .select('_id conversationId sender senderName text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment replyTo replyToMessageId errorMessage')
        .sort({ timestamp: -1, _id: -1 })
        .limit(limit + 1)
    );
  }

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Inbox explain failed:', error);
  try {
    await mongoose.connection.close();
  } catch {}
  process.exit(1);
});
