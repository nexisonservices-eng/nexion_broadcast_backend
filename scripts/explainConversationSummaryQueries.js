require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const ConversationSummary = require('../models/ConversationSummary');
const Conversation = require('../models/Conversation');
const { buildInboxSearchPlan } = require('../utils/inboxSearchPlan');

const toJson = (value) => JSON.stringify(value, null, 2);

const summarizePlan = (explain = {}) => {
  const winningPlan = explain?.queryPlanner?.winningPlan || {};
  const executionStats = explain?.executionStats || {};

  const walkStages = (node, found = []) => {
    if (!node || typeof node !== 'object') return found;
    if (node.stage) {
      found.push({
        stage: node.stage,
        indexName: node.indexName || null
      });
    }

    if (node.inputStage) walkStages(node.inputStage, found);
    if (Array.isArray(node.inputStages)) {
      node.inputStages.forEach((child) => walkStages(child, found));
    }
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

const printPlan = async (label, cursor) => {
  const explain = await cursor.explain('executionStats');
  const summary = summarizePlan(explain);
  console.log(`\n=== ${label} ===`);
  console.log(toJson(summary));
};

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

const run = async () => {
  await connectDB();

  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const status = String(readArg(['--status']) || '').trim();
  const assignedTo = String(readArg(['--assigned-to', '--assignedTo']) || '').trim();
  const search = String(readArg(['--search']) || '').trim();
  const conversationId = parseObjectId(readArg(['--conversation-id', '--conversationId']), 'conversationId');
  const limit = Math.max(1, Math.min(Number(readArg(['--limit']) || 50) || 50, 200));

  const tenantFilter = {};
  if (companyId) tenantFilter.companyId = companyId;
  if (userId) tenantFilter.userId = userId;
  if (status) tenantFilter.status = status;
  if (assignedTo) tenantFilter.assignedTo = assignedTo;

  await printPlan(
    'Summary inbox page',
    ConversationSummary.find(tenantFilter)
      .select('_id conversationId contactId contactPhone contactName status assignedTo lastMessageTime unreadCount')
      .sort({ lastMessageTime: -1, _id: -1 })
      .limit(limit)
  );

  await printPlan(
    'Summary active inbox page',
    ConversationSummary.find({
      ...tenantFilter,
      status: 'active'
    })
      .select('_id conversationId contactId contactPhone contactName status assignedTo lastMessageTime unreadCount')
      .sort({ lastMessageTime: -1, _id: -1 })
      .limit(limit)
  );

  if (search) {
    const searchPlan = buildInboxSearchPlan(search);
    let summarySearchQuery = ConversationSummary.find({
      ...tenantFilter,
      ...(searchPlan.summaryClause || {})
    })
      .select('_id conversationId contactPhone contactName lastMessage lastMessageTime unreadCount')
      .sort({ lastMessageTime: -1, _id: -1 });

    if (searchPlan.hint) {
      summarySearchQuery = summarySearchQuery.hint(searchPlan.hint);
    }

    await printPlan(
      `Summary search by contact fields (${searchPlan.mode || 'none'})`,
      summarySearchQuery.limit(limit)
    );

    await printPlan(
      'Conversation fallback search',
      Conversation.find({
        ...tenantFilter,
        ...(searchPlan.fallbackClause || {})
      })
        .select('_id conversationId contactPhone contactName lastMessage lastMessageTime unreadCount')
        .sort({ lastMessageTime: -1, _id: -1 })
        .limit(limit)
    );
  }

  if (conversationId) {
    await printPlan(
      'Summary single conversation lookup',
      ConversationSummary.find({
        conversationId,
        ...tenantFilter
      }).select('_id conversationId contactPhone contactName status assignedTo lastMessageTime unreadCount')
    );
  }

  if (assignedTo) {
    await printPlan(
      'Summary assigned inbox page',
      ConversationSummary.find({
        ...tenantFilter,
        assignedTo
      })
        .select('_id conversationId contactPhone contactName status assignedTo lastMessageTime unreadCount')
        .sort({ lastMessageTime: -1, _id: -1 })
        .limit(limit)
    );
  }

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Conversation summary explain failed:', error);
  try {
    await mongoose.connection.close();
  } catch (_closeError) {
    // ignore close failures
  }
  process.exit(1);
});
