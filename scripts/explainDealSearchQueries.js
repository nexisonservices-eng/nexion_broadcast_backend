require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Deal = require('../models/Deal');
const { buildDealSearchPlan } = require('../utils/dealSearchPlan');
const {
  buildDealCursorFilter,
  decodeDealCursor
} = require('../utils/dealPagination');

const readArg = (names = []) => {
  for (const name of names) {
    const prefix = `${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
  }
  return null;
};

const parseObjectId = (value, label) => {
  const normalized = String(value || '').trim();
  if (!normalized) return null;
  if (!mongoose.Types.ObjectId.isValid(normalized)) {
    throw new Error(`Invalid ${label}: ${normalized}`);
  }
  return new mongoose.Types.ObjectId(normalized);
};

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

const printPlan = async (label, query) => {
  const explain = await query.explain('executionStats');
  const summary = summarizePlan(explain);
  console.log(`\n## ${label}`);
  console.log(JSON.stringify(summary, null, 2));
};

const run = async () => {
  await connectDB();

  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const search = String(readArg(['--search']) || '').trim();
  const status = String(readArg(['--status']) || '').trim();
  const stage = String(readArg(['--stage']) || '').trim();
  const ownerId = String(readArg(['--owner-id', '--ownerId']) || '').trim();
  const contactId = parseObjectId(readArg(['--contact-id', '--contactId']), 'contactId');
  const cursor = String(readArg(['--cursor']) || '').trim();
  const limit = Math.max(1, Math.min(Number(readArg(['--limit']) || 50) || 50, 200));

  const filters = {};
  if (companyId) filters.companyId = companyId;
  if (userId) filters.userId = userId;
  if (status) filters.status = status;
  if (stage) filters.stage = stage;
  if (ownerId) filters.ownerId = ownerId;
  if (contactId) filters.contactId = contactId;

  const searchPlan = buildDealSearchPlan(search);
  const decodedCursor = cursor ? decodeDealCursor(cursor) : null;
  const sortClause = { expectedCloseAtSort: 1, updatedAt: -1, createdAt: -1, _id: -1 };

  let summaryQuery = Deal.find({
    ...filters,
    ...(searchPlan.summaryClause || {})
  })
    .select('_id title productName source lostReason expectedCloseAt expectedCloseAtSort updatedAt createdAt')
    .sort(sortClause);

  await printPlan(`Deal search (${searchPlan.mode || 'none'})`, summaryQuery.limit(limit));

  if (searchPlan.fallbackClause) {
    const fallbackQuery = Deal.find({
      ...filters,
      ...(searchPlan.fallbackClause || {})
    })
      .select('_id title productName source lostReason expectedCloseAt expectedCloseAtSort updatedAt createdAt')
      .sort(sortClause)
      .limit(limit);

    await printPlan('Deal fallback search', fallbackQuery);
  }

  if (decodedCursor) {
    await printPlan(
      'Deal cursor page',
      Deal.find({
        ...filters,
        ...(searchPlan.summaryClause || {}),
        ...buildDealCursorFilter(decodedCursor)
      })
        .select('_id title productName source lostReason expectedCloseAt expectedCloseAtSort updatedAt createdAt')
        .sort(sortClause)
        .limit(limit)
    );
  }

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Deal search explain failed:', error);
  try {
    await mongoose.connection.close();
  } catch {}
  process.exit(1);
});
