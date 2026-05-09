require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Contact = require('../models/Contact');
const { buildContactSearchPlan } = require('../utils/contactSearchPlan');

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
  const tags = String(readArg(['--tags']) || '').trim();
  const limit = Math.max(1, Math.min(Number(readArg(['--limit']) || 50) || 50, 200));

  const tenantFilter = {};
  if (companyId) tenantFilter.companyId = companyId;
  if (userId) tenantFilter.userId = userId;

  const searchPlan = buildContactSearchPlan(search);
  const baseFilters = { ...tenantFilter };

  if (tags) {
    baseFilters.tags = { $in: tags.split(',').map((tag) => String(tag || '').trim()).filter(Boolean) };
  }

  let summaryQuery = Contact.find({
    ...baseFilters,
    ...(searchPlan.summaryClause || {})
  })
    .select('_id name phone email nameLower phoneDigits lastContact createdAt')
    .sort({ lastContact: -1, createdAt: -1, _id: -1 });

  if (searchPlan.hint) {
    summaryQuery = summaryQuery.hint(searchPlan.hint);
  }

  await printPlan(`Contact search (${searchPlan.mode || 'none'})`, summaryQuery.limit(limit));

  if (searchPlan.fallbackClause) {
    const fallbackQuery = Contact.find({
      ...baseFilters,
      ...(searchPlan.fallbackClause || {})
    })
      .select('_id name phone email nameLower phoneDigits lastContact createdAt')
      .sort({ lastContact: -1, createdAt: -1, _id: -1 })
      .limit(limit);

    await printPlan('Contact fallback search', fallbackQuery);
  }

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Contact search explain failed:', error);
  try {
    await mongoose.connection.close();
  } catch {}
  process.exit(1);
});
