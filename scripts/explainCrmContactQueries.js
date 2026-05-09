require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Contact = require('../models/Contact');
const LeadTask = require('../models/LeadTask');
const { buildCrmContactSearchPlan } = require('../utils/crmContactSearchPlan');
const { buildCrmContactListHint } = require('../utils/crmContactQueryPlan');
const {
  CONTACT_CURSOR_SORT,
  buildCrmContactCursorFilter,
  decodeCrmContactCursor
} = require('../utils/crmContactPagination');

const LEAD_STATUSES = ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'unqualified', 'won', 'lost'];
const CONTACT_QUEUES = [
  'my_leads',
  'unassigned',
  'overdue_followups',
  'due_today',
  'today_calls',
  'high_score',
  'needs_reply',
  'opted_in'
];
const CRM_CONTACT_LIST_FIELDS = '_id name phone email tags stage status source sourceType ownerId temperature dealValue lostReason nextFollowUpAt lastContact lastContactAt lastStageChangedAt leadScore leadScoreBreakdown isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata whatsappOptOutAt lastInboundMessageAt serviceWindowClosesAt createdAt updatedAt';

const readArg = (names = []) => {
  for (const name of names) {
    const prefix = `${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
  }
  return null;
};

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) => (mongoose.Types.ObjectId.isValid(value) ? value : null);

const safeDate = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const mergeFiltersWithAnd = (...filters) => {
  const validFilters = filters.filter(
    (filter) => filter && typeof filter === 'object' && Object.keys(filter).length > 0
  );
  if (validFilters.length === 0) return {};
  if (validFilters.length === 1) return validFilters[0];
  return { $and: validFilters };
};

const buildScopedFilter = (companyId, userId, extra = {}) => {
  const conditions = [];
  if (companyId) conditions.push({ companyId });
  if (userId) conditions.push({ userId });
  if (extra && Object.keys(extra).length > 0) conditions.push(extra);
  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const getDayRange = (value = new Date()) => {
  const baseDate = safeDate(value) || new Date();
  return {
    startOfDay: new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate(), 0, 0, 0, 0),
    endOfDay: new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate(), 23, 59, 59, 999)
  };
};

const buildContactQueueFilter = (queue) => {
  const normalizedQueue = toCleanString(queue).toLowerCase();
  if (!normalizedQueue) return {};

  const now = new Date();
  const { startOfDay, endOfDay } = getDayRange(now);

  switch (normalizedQueue) {
    case 'my_leads':
      return {};
    case 'unassigned':
      return {
        $or: [{ ownerId: null }, { ownerId: '' }, { ownerId: { $exists: false } }]
      };
    case 'overdue_followups':
      return {
        nextFollowUpAt: { $ne: null, $lt: now }
      };
    case 'due_today':
      return {
        nextFollowUpAt: { $gte: startOfDay, $lte: endOfDay }
      };
    case 'high_score':
      return {
        leadScore: { $gte: 60 }
      };
    case 'needs_reply':
      return {
        lastInboundMessageAt: { $ne: null },
        $or: [
          { lastContactAt: null },
          { lastContactAt: { $exists: false } },
          { $expr: { $gt: ['$lastInboundMessageAt', '$lastContactAt'] } }
        ]
      };
    case 'opted_in':
      return {
        whatsappOptInStatus: 'opted_in'
      };
    default:
      return {};
  }
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

  const companyId = toObjectIdIfValid(readArg(['--company-id', '--companyId']));
  const userId = toObjectIdIfValid(readArg(['--user-id', '--userId']));
  const ownerId = toObjectIdIfValid(readArg(['--owner-id', '--ownerId']));
  const search = String(readArg(['--search']) || '').trim();
  const queue = String(readArg(['--queue']) || '').trim();
  const cursorParam = String(readArg(['--cursor']) || '').trim();
  const page = Math.max(1, Number(readArg(['--page']) || 1) || 1);
  const limit = Math.max(1, Math.min(Number(readArg(['--limit']) || 50) || 50, 200));

  const tenantFilter = {};
  if (companyId) tenantFilter.companyId = companyId;
  if (userId) tenantFilter.userId = userId;

  const extraFilter = {};
  const normalizedQueue = toCleanString(queue).toLowerCase();
  if (normalizedQueue && !CONTACT_QUEUES.includes(normalizedQueue)) {
    throw new Error(`Invalid queue: ${normalizedQueue}`);
  }

  if (normalizedQueue) {
    if (normalizedQueue === 'my_leads') {
      if (ownerId || userId) {
        extraFilter.ownerId = ownerId || userId;
      }
    } else if (normalizedQueue === 'today_calls') {
      const { startOfDay, endOfDay } = getDayRange(new Date());
      const todayCallTasks = await LeadTask.find(
        buildScopedFilter(companyId, userId, {
          taskType: 'call',
          status: { $in: ['pending', 'in_progress'] },
          dueAt: { $gte: startOfDay, $lte: endOfDay }
        })
      )
        .select('contactId')
        .lean();

      const contactIds = Array.from(
        new Set((todayCallTasks || []).map((task) => String(task?.contactId || '').trim()).filter(Boolean))
      );

      extraFilter._id = {
        $in: contactIds.map((contactId) => new mongoose.Types.ObjectId(contactId))
      };
    } else {
      Object.assign(extraFilter, buildContactQueueFilter(normalizedQueue));
    }
  }

  const searchPlan = buildCrmContactSearchPlan(search);
  const normalizedCursor = toCleanString(cursorParam);
  const hasCursor = Boolean(normalizedCursor);
  const cursor = hasCursor ? decodeCrmContactCursor(normalizedCursor) : null;
  if (hasCursor && !cursor) {
    throw new Error('Invalid cursor');
  }

  const cursorFilter = hasCursor ? buildCrmContactCursorFilter(cursor) : {};
  const scopedFilter = buildScopedFilter(
    companyId,
    userId,
    mergeFiltersWithAnd(extraFilter, searchPlan.summaryClause || {}, cursorFilter)
  );
  const sortClause = { ...CONTACT_CURSOR_SORT };
  const ownerScopeId = ownerId || (normalizedQueue === 'my_leads' ? userId : null);
  const listHint = buildCrmContactListHint({ ownerScopeId, searchPlan });

  let query = Contact.find(scopedFilter)
    .select(CRM_CONTACT_LIST_FIELDS)
    .sort(sortClause)
    .limit(hasCursor ? limit + 1 : limit);

  if (listHint) {
    query = query.hint(listHint);
  }

  await printPlan(`CRM contacts (${hasCursor ? 'cursor' : 'page'} / ${searchPlan.mode || 'none'})`, query);

  if (searchPlan.fallbackClause) {
    const fallbackFilter = buildScopedFilter(
      companyId,
      userId,
      mergeFiltersWithAnd(extraFilter, searchPlan.fallbackClause, cursorFilter)
    );
    let fallbackQuery = Contact.find(fallbackFilter)
      .select(CRM_CONTACT_LIST_FIELDS)
      .sort(sortClause)
      .limit(hasCursor ? limit + 1 : limit);

    if (listHint) {
      fallbackQuery = fallbackQuery.hint(listHint);
    }

    await printPlan('CRM contacts fallback', fallbackQuery);
  }

  if (!hasCursor) {
    const pageScopedFilter = buildScopedFilter(
      companyId,
      userId,
      mergeFiltersWithAnd(extraFilter, searchPlan.summaryClause || {})
    );
    const pageQuery = Contact.find(pageScopedFilter)
      .select(CRM_CONTACT_LIST_FIELDS)
      .sort(sortClause)
      .skip((page - 1) * limit)
      .limit(limit);

    if (listHint) {
      pageQuery.hint(listHint);
    }

    await printPlan('CRM contacts offset page', pageQuery);
  }

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('CRM contacts explain failed:', error);
  try {
    await mongoose.connection.close();
  } catch {}
  process.exit(1);
});
