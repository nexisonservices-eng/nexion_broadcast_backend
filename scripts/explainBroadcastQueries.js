require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Broadcast = require('../models/Broadcast');
const BroadcastDispatch = require('../models/BroadcastDispatch');

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
  console.log(`\n## ${label}`);
  console.log(JSON.stringify(summarizePlan(explain), null, 2));
};

const run = async () => {
  await connectDB();

  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const broadcastId = parseObjectId(readArg(['--broadcast-id', '--broadcastId']), 'broadcastId');
  const recipientPhone = String(readArg(['--recipient-phone', '--recipientPhone']) || '').trim();
  const status = String(readArg(['--status']) || '').trim();
  const limit = Math.max(1, Math.min(Number(readArg(['--limit']) || 50) || 50, 200));

  const tenantFilter = {};
  if (companyId) tenantFilter.companyId = companyId;
  if (userId) tenantFilter.createdById = userId;

  await printPlan(
    'Broadcast list page',
    Broadcast.find({
      ...tenantFilter,
      ...(status ? { status } : {})
    })
      .select('_id name companyId createdById status scheduledAt createdAt recipientCount')
      .sort({ createdAt: -1, _id: -1 })
      .limit(limit)
  );

  await printPlan(
    'Scheduled broadcast claim',
    Broadcast.findOne({
      ...tenantFilter,
      status: 'scheduled',
      scheduledAt: { $lte: new Date() }
    })
      .select('_id status scheduledAt createdById companyId')
      .sort({ scheduledAt: 1 })
  );

  if (broadcastId) {
    await printPlan(
      'Broadcast dispatch page',
      BroadcastDispatch.find({
        ...tenantFilter,
        broadcastId
      })
        .select('_id broadcastId recipientPhone status claimedAt sentAt failedAt recipientIndex chunkId chunkIndex')
        .sort({ recipientIndex: 1, _id: 1 })
        .limit(limit)
    );

    await printPlan(
      'Broadcast dispatch status page',
      BroadcastDispatch.find({
        ...tenantFilter,
        broadcastId,
        ...(status ? { status } : {})
      })
        .select('_id broadcastId recipientPhone status claimedAt sentAt failedAt recipientIndex')
        .sort({ updatedAt: -1 })
        .limit(limit)
    );
  }

  if (recipientPhone) {
    await printPlan(
      'Broadcast dispatch recipient lookup',
      BroadcastDispatch.find({
        ...tenantFilter,
        ...(broadcastId ? { broadcastId } : {}),
        recipientPhone
      })
        .select('_id broadcastId recipientPhone status recipientIndex')
        .sort({ recipientIndex: 1, _id: 1 })
        .limit(limit)
    );
  }

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Broadcast explain failed:', error);
  try {
    await mongoose.connection.close();
  } catch {}
  process.exit(1);
});
