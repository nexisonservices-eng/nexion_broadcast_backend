const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const auth = require('../middleware/auth');
const { requireCrmPolicy } = require('../middleware/crmPolicy');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Deal = require('../models/Deal');
const Broadcast = require('../models/Broadcast');
const Message = require('../models/Message');
const LeadTask = require('../models/LeadTask');
const LeadActivity = require('../models/LeadActivity');
const CrmAutomationRun = require('../models/CrmAutomationRun');
const CrmPipelineView = require('../models/CrmPipelineView');
const CrmPipelineStage = require('../models/CrmPipelineStage');
const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');
const {
  ContactDocument,
  CONTACT_DOCUMENT_TYPES,
  CONTACT_DOCUMENT_VERIFICATION_STATUSES
} = require('../models/ContactDocument');
const {
  uploadContactDocumentAttachment
} = require('../services/contactDocumentStorageService');
const {
  generateSignedAttachmentUrl,
  generateAttachmentDownloadUrl,
  deleteInboxAttachment
} = require('../services/inboxMediaService');
const {
  getCrmOwnerDashboard,
  runCrmFollowUpAutomation
} = require('../services/crmOpsService');
const {
  getCrmReportsSummary,
  getCrmFunnelReport,
  getCrmCohortReport,
  getCrmOwnerPerformanceReport
} = require('../services/crmReportsService');
const { getLeadScoringSettings } = require('../services/leadScoringService');

const router = express.Router();
router.use(auth);
router.use(requireCrmPolicy());

const LEAD_STAGES = ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'won', 'lost'];
const LEAD_STATUSES = ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'unqualified', 'won', 'lost'];
const LEAD_TEMPERATURES = ['cold', 'warm', 'hot'];
const DEAL_STAGES = ['discovery', 'qualified', 'proposal', 'negotiation', 'won', 'lost'];
const DEAL_STATUSES = ['open', 'won', 'lost'];
const TASK_PRIORITIES = ['low', 'medium', 'high'];
const TASK_STATUSES = ['pending', 'in_progress', 'completed', 'cancelled'];
const TASK_TYPES = ['follow_up', 'call', 'whatsapp', 'email', 'meeting', 'demo', 'other'];
const TASK_RECURRENCE_FREQUENCIES = ['none', 'daily', 'weekly', 'monthly'];
const TASK_OPEN_STATUSES = ['pending', 'in_progress'];
const LEAD_STAGE_ORDER = ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'won', 'lost'];
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
const DOCUMENT_ACCESS_MODES = ['view', 'download'];
const CRM_CONTACT_LIST_FIELDS = [
  '_id',
  'name',
  'phone',
  'email',
  'tags',
  'stage',
  'status',
  'source',
  'sourceType',
  'ownerId',
  'temperature',
  'dealValue',
  'lostReason',
  'nextFollowUpAt',
  'lastContact',
  'lastContactAt',
  'lastStageChangedAt',
  'leadScore',
  'leadScoreBreakdown',
  'archivedAt',
  'archivedBy',
  'isBlocked',
  'whatsappOptInStatus',
  'whatsappOptInAt',
  'whatsappOptInSource',
  'whatsappOptInScope',
  'whatsappOptInTextSnapshot',
  'whatsappOptInProofType',
  'whatsappOptInProofId',
  'whatsappOptInProofUrl',
  'whatsappOptInCapturedBy',
  'whatsappOptInPageUrl',
  'whatsappOptInIp',
  'whatsappOptInUserAgent',
  'whatsappOptInMetadata',
  'whatsappOptOutAt',
  'lastInboundMessageAt',
  'serviceWindowClosesAt',
  'createdAt',
  'updatedAt'
].join(' ');
const CRM_CONTACT_FIELD_WHITELIST = new Set(CRM_CONTACT_LIST_FIELDS.split(/\s+/).filter(Boolean));
const CRM_TASK_CONTACT_FIELDS = 'name phone stage status leadScore temperature ownerId nextFollowUpAt';
const CRM_DEAL_CONTACT_FIELDS =
  'name phone email stage status temperature ownerId leadScore nextFollowUpAt dealValue';
const CRM_DEAL_LIST_FIELDS = [
  '_id',
  'title',
  'stage',
  'status',
  'value',
  'probability',
  'currency',
  'expectedCloseAt',
  'ownerId',
  'productName',
  'source',
  'notes',
  'lostReason',
  'wonAt',
  'lostAt',
  'createdAt',
  'updatedAt',
  'contactId'
].join(' ');
const CRM_MESSAGE_FIELDS =
  '_id sender senderName text mediaType mediaCaption status timestamp broadcastId';
const CRM_CONVERSATION_FIELDS =
  '_id status assignedTo lastMessage lastMessageTime lastMessageFrom unreadCount contactPhone contactName';
const DEFAULT_LEAD_PIPELINE_STAGES = [
  { key: 'new', label: 'New Lead', color: '#5f8fc3', order: 0 },
  { key: 'contacted', label: 'Contacted', color: '#4a8bbd', order: 1 },
  { key: 'nurturing', label: 'Nurturing', color: '#6f7bd0', order: 2 },
  { key: 'qualified', label: 'Qualified', color: '#4f9d6c', order: 3 },
  { key: 'proposal', label: 'Proposal Sent', color: '#d18a3a', order: 4 },
  { key: 'won', label: 'Won', color: '#1d9b5e', order: 5 },
  { key: 'lost', label: 'Lost', color: '#c45a5a', order: 6 }
];
const DEFAULT_LEAD_STAGE_KEY_SET = new Set(DEFAULT_LEAD_PIPELINE_STAGES.map((stage) => stage.key));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.CRM_DOCUMENT_MAX_FILE_SIZE_BYTES || 30 * 1024 * 1024)
  }
});

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) => (mongoose.Types.ObjectId.isValid(value) ? value : null);
const toFiniteNumber = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};
const encodeCursor = (payload = {}) => Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
const decodeCursor = (value) => {
  const normalized = toCleanString(value);
  if (!normalized) return null;
  try {
    const parsed = JSON.parse(Buffer.from(normalized, 'base64url').toString('utf8'));
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
};
const buildContactProjection = (fields) => {
  const requestedFields = toCleanString(fields)
    .split(',')
    .map((field) => field.trim())
    .filter(Boolean);
  if (!requestedFields.length || requestedFields.includes('list')) return CRM_CONTACT_LIST_FIELDS;
  const allowedFields = requestedFields.filter((field) => CRM_CONTACT_FIELD_WHITELIST.has(field));
  if (!allowedFields.includes('_id')) allowedFields.unshift('_id');
  return allowedFields.length ? allowedFields.join(' ') : CRM_CONTACT_LIST_FIELDS;
};
const slugifyPipelineStageKey = (value) =>
  toCleanString(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'stage';

const normalizePipelineStageResponse = (stage) => ({
  id: String(stage?._id || stage?.id || '').trim(),
  key: toCleanString(stage?.key).toLowerCase(),
  label: toCleanString(stage?.label),
  color: toCleanString(stage?.color) || '#5f8fc3',
  order: Number.isFinite(Number(stage?.order)) ? Number(stage.order) : 0,
  isArchived: Boolean(stage?.isArchived),
  contactCount: Number.isFinite(Number(stage?.contactCount)) ? Number(stage.contactCount) : 0,
  createdAt: stage?.createdAt || null,
  updatedAt: stage?.updatedAt || null
});

const buildPipelineStageScope = (req) => ({
  userId: String(req.user?.id || '').trim(),
  ...(req.companyId ? { companyId: String(req.companyId).trim() } : {})
});

const getDefaultPipelineStages = () => DEFAULT_LEAD_PIPELINE_STAGES.map((stage) => ({
  ...stage,
  isArchived: false,
  contactCount: 0
}));

const bootstrapDefaultPipelineStages = async (req) => {
  const scope = buildPipelineStageScope(req);
  const existingCount = await CrmPipelineStage.countDocuments(scope);
  if (existingCount > 0) {
    return CrmPipelineStage.find({ ...scope, isArchived: { $ne: true } })
      .sort({ order: 1, createdAt: 1 })
      .lean();
  }

  const stages = DEFAULT_LEAD_PIPELINE_STAGES.map((stage) => ({
    ...scope,
    key: stage.key,
    label: stage.label,
    color: stage.color,
    order: stage.order,
    isArchived: false
  }));

  try {
    await CrmPipelineStage.insertMany(stages, { ordered: false });
  } catch (error) {
    if (String(error?.code || '') !== '11000') {
      throw error;
    }
  }

  return CrmPipelineStage.find({ ...scope, isArchived: { $ne: true } })
    .sort({ order: 1, createdAt: 1 })
    .lean();
};

const loadPipelineStages = async (req) => {
  const scope = buildPipelineStageScope(req);
  let stages = await CrmPipelineStage.find({ ...scope, isArchived: { $ne: true } })
    .sort({ order: 1, createdAt: 1 })
    .lean();

  if (!stages.length) {
    stages = await bootstrapDefaultPipelineStages(req);
  }

  const activeStages = stages.filter((stage) => !stage?.isArchived);
  return activeStages.map((stage, index) => ({
    ...normalizePipelineStageResponse(stage),
    order: Number.isFinite(Number(stage?.order)) ? Number(stage.order) : index
  }));
};

const loadPipelineStageMap = async (req) => {
  const stages = await loadPipelineStages(req);
  return {
    stages,
    keySet: new Set(stages.map((stage) => stage.key).filter(Boolean)),
    stageMap: new Map(stages.map((stage) => [stage.key, stage]))
  };
};

const getPipelineStageStatus = (stageKey) => {
  const normalized = toCleanString(stageKey).toLowerCase();
  if (DEFAULT_LEAD_STAGE_KEY_SET.has(normalized)) {
    return normalized === 'proposal' ? 'proposal' : normalized;
  }
  return '';
};

const emitCrmRealtimeEvent = (req, payload = {}) => {
  const sendToUser = req?.app?.locals?.sendToUser;
  if (typeof sendToUser !== 'function') return;

  const userId = toCleanString(req?.user?.id);
  if (!userId) return;

  sendToUser(userId, {
    type: 'crm_changed',
    scope: 'crm',
    timestamp: new Date().toISOString(),
    ...payload,
    contactId: toCleanString(payload?.contactId),
    dealId: toCleanString(payload?.dealId),
    taskId: toCleanString(payload?.taskId),
    filterPresetId: toCleanString(payload?.filterPresetId),
    conversationId: toCleanString(payload?.conversationId)
  });
};

const toCrmContactRealtimePayload = (contact = {}) => {
  const source = typeof contact?.toObject === 'function' ? contact.toObject() : contact || {};
  return {
    _id: String(source?._id || source?.id || '').trim(),
    name: toCleanString(source?.name),
    phone: toCleanString(source?.phone),
    email: toCleanString(source?.email),
    tags: Array.isArray(source?.tags) ? source.tags : [],
    stage: toCleanString(source?.stage || 'new').toLowerCase(),
    status: toCleanString(source?.status || 'new').toLowerCase(),
    source: toCleanString(source?.source),
    sourceType: toCleanString(source?.sourceType),
    ownerId: toCleanString(source?.ownerId),
    temperature: toCleanString(source?.temperature),
    dealValue: Number.isFinite(Number(source?.dealValue)) ? Number(source.dealValue) : 0,
    lostReason: toCleanString(source?.lostReason),
    nextFollowUpAt: source?.nextFollowUpAt || null,
    lastContact: source?.lastContact || null,
    lastContactAt: source?.lastContactAt || null,
    lastStageChangedAt: source?.lastStageChangedAt || null,
    leadScore: Number.isFinite(Number(source?.leadScore)) ? Number(source.leadScore) : 0,
    leadScoreBreakdown: source?.leadScoreBreakdown || {},
    whatsappOptInStatus: toCleanString(source?.whatsappOptInStatus),
    lastInboundMessageAt: source?.lastInboundMessageAt || null,
    notes: toCleanString(source?.notes),
    createdAt: source?.createdAt || null,
    updatedAt: source?.updatedAt || null
  };
};

const normalizeFilterPresetFilters = (filters = {}) => ({
  search: toCleanString(filters?.search),
  queue: ['all', ...CONTACT_QUEUES].includes(toCleanString(filters?.queue).toLowerCase())
    ? toCleanString(filters?.queue).toLowerCase()
    : 'all',
  status: ['all', ...LEAD_STATUSES].includes(toCleanString(filters?.status).toLowerCase())
    ? toCleanString(filters?.status).toLowerCase()
    : 'all',
  owner: toCleanString(filters?.owner) || 'all',
  sortOrder: ['newest', 'oldest'].includes(toCleanString(filters?.sortOrder).toLowerCase())
    ? toCleanString(filters?.sortOrder).toLowerCase()
    : 'newest',
  archive: ['active', 'archived', 'all'].includes(toCleanString(filters?.archive).toLowerCase())
    ? toCleanString(filters?.archive).toLowerCase()
    : 'active',
  dealStatus: ['all', ...DEAL_STATUSES].includes(toCleanString(filters?.dealStatus).toLowerCase())
    ? toCleanString(filters?.dealStatus).toLowerCase()
    : 'all',
  dealStage: ['all', ...DEAL_STAGES].includes(toCleanString(filters?.dealStage).toLowerCase())
    ? toCleanString(filters?.dealStage).toLowerCase()
    : 'all',
  dealOwnerId: toCleanString(filters?.dealOwnerId),
  dealSearch: toCleanString(filters?.dealSearch),
  dealCreatedFrom: toCleanString(filters?.dealCreatedFrom),
  dealCreatedTo: toCleanString(filters?.dealCreatedTo),
  dealUpdatedFrom: toCleanString(filters?.dealUpdatedFrom),
  dealUpdatedTo: toCleanString(filters?.dealUpdatedTo),
  dealExpectedCloseFrom: toCleanString(filters?.dealExpectedCloseFrom),
  dealExpectedCloseTo: toCleanString(filters?.dealExpectedCloseTo),
  dealValueMin: toCleanString(filters?.dealValueMin),
  dealValueMax: toCleanString(filters?.dealValueMax),
  dealQuickFilter: ['all', 'my_deals', 'recently_updated', 'high_value', 'closing_this_week'].includes(
    toCleanString(filters?.dealQuickFilter).toLowerCase()
  )
    ? toCleanString(filters?.dealQuickFilter).toLowerCase()
    : 'all'
});

const toFilterPresetResponse = (view) => ({
  id: String(view?._id || view?.id || '').trim(),
  label: String(view?.label || '').trim(),
  filters: normalizeFilterPresetFilters(view?.filters || {}),
  presetType: toCleanString(view?.presetType) || 'filter_preset',
  createdAt: view?.createdAt || null,
  updatedAt: view?.updatedAt || null
});

const getDayRange = (value = new Date()) => {
  const baseDate = safeDate(value) || new Date();
  return {
    startOfDay: new Date(
      baseDate.getFullYear(),
      baseDate.getMonth(),
      baseDate.getDate(),
      0,
      0,
      0,
      0
    ),
    endOfDay: new Date(
      baseDate.getFullYear(),
      baseDate.getMonth(),
      baseDate.getDate(),
      23,
      59,
      59,
      999
    )
  };
};

const mergeFiltersWithAnd = (...filters) => {
  const validFilters = filters.filter(
    (filter) => filter && typeof filter === 'object' && Object.keys(filter).length > 0
  );
  if (validFilters.length === 0) return {};
  if (validFilters.length === 1) return validFilters[0];
  return { $and: validFilters };
};

const normalizeDealLifecycle = ({
  existingStage = 'discovery',
  existingStatus = 'open',
  nextStage,
  nextStatus,
  existingWonAt = null,
  existingLostAt = null
}) => {
  let resolvedStage = DEAL_STAGES.includes(toCleanString(nextStage).toLowerCase())
    ? toCleanString(nextStage).toLowerCase()
    : DEAL_STAGES.includes(toCleanString(existingStage).toLowerCase())
      ? toCleanString(existingStage).toLowerCase()
      : 'discovery';

  let resolvedStatus = DEAL_STATUSES.includes(toCleanString(nextStatus).toLowerCase())
    ? toCleanString(nextStatus).toLowerCase()
    : DEAL_STATUSES.includes(toCleanString(existingStatus).toLowerCase())
      ? toCleanString(existingStatus).toLowerCase()
      : 'open';

  if (resolvedStage === 'won') resolvedStatus = 'won';
  if (resolvedStage === 'lost') resolvedStatus = 'lost';
  if (resolvedStatus === 'won') resolvedStage = 'won';
  if (resolvedStatus === 'lost') resolvedStage = 'lost';
  if (resolvedStatus === 'open' && ['won', 'lost'].includes(resolvedStage)) {
    resolvedStage = 'discovery';
  }

  return {
    stage: resolvedStage,
    status: resolvedStatus,
    wonAt: resolvedStatus === 'won' ? existingWonAt || new Date() : null,
    lostAt: resolvedStatus === 'lost' ? existingLostAt || new Date() : null
  };
};

const buildScopedFilter = (req, extra = {}) => {
  const conditions = [];
  const scopeCandidates = [];
  const normalizedCompanyId = toObjectIdIfValid(req?.companyId);
  const normalizedUserId = toObjectIdIfValid(req?.user?.id);

  if (normalizedCompanyId) {
    scopeCandidates.push({ companyId: normalizedCompanyId });
  } else if (req?.companyId) {
    scopeCandidates.push({ companyId: req.companyId });
  }

  if (normalizedUserId) {
    scopeCandidates.push({ userId: normalizedUserId });
  } else if (req?.user?.id) {
    scopeCandidates.push({ userId: req.user.id });
  }

  if (scopeCandidates.length === 1) {
    conditions.push(scopeCandidates[0]);
  } else if (scopeCandidates.length > 1) {
    conditions.push({ $or: scopeCandidates });
  }

  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }

  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const buildDealCursorFilter = (cursorValue) => {
  const decodedCursor = decodeCursor(cursorValue);
  if (!decodedCursor?._id || !decodedCursor?.updatedAt) return null;

  const cursorId = toObjectIdIfValid(decodedCursor._id);
  const cursorUpdatedAt = new Date(decodedCursor.updatedAt);
  if (!cursorId || Number.isNaN(cursorUpdatedAt.getTime())) return null;

  return {
    $or: [
      { updatedAt: { $lt: cursorUpdatedAt } },
      { updatedAt: cursorUpdatedAt, _id: { $lt: new mongoose.Types.ObjectId(cursorId) } }
    ]
  };
};

const buildDealListSort = () => ({ updatedAt: -1, _id: -1 });

const buildDealSearchFilter = async (req, search) => {
  const normalizedSearch = toCleanString(search);
  if (!normalizedSearch) return {};

  const searchRegex = new RegExp(normalizedSearch, 'i');
  const contactFilter = buildScopedFilter(req, {
    $or: [
      { name: { $regex: normalizedSearch, $options: 'i' } },
      { phone: { $regex: normalizedSearch, $options: 'i' } },
      { email: { $regex: normalizedSearch, $options: 'i' } },
      { ownerId: { $regex: normalizedSearch, $options: 'i' } },
      { tags: { $in: [searchRegex] } }
    ]
  });
  const contactIds = await Contact.distinct('_id', contactFilter);
  const normalizedContactIds = (contactIds || [])
    .map((contactId) => String(contactId || '').trim())
    .filter((contactId) => toObjectIdIfValid(contactId));

  const searchClauses = [
    { title: { $regex: normalizedSearch, $options: 'i' } },
    { productName: { $regex: normalizedSearch, $options: 'i' } },
    { source: { $regex: normalizedSearch, $options: 'i' } },
    { notes: { $regex: normalizedSearch, $options: 'i' } },
    { lostReason: { $regex: normalizedSearch, $options: 'i' } }
  ];

  if (normalizedContactIds.length > 0) {
    searchClauses.push({
      contactId: { $in: normalizedContactIds.map((contactId) => new mongoose.Types.ObjectId(contactId)) }
    });
  }

  return { $or: searchClauses };
};

const buildDealFieldRangeFilter = (field, fromValue, toValue) => {
  const parsedFrom = safeDate(fromValue);
  const parsedTo = safeDate(toValue);
  if (fromValue && !parsedFrom) {
    const error = new Error(`Invalid ${field}From date`);
    error.status = 400;
    throw error;
  }
  if (toValue && !parsedTo) {
    const error = new Error(`Invalid ${field}To date`);
    error.status = 400;
    throw error;
  }
  if (!parsedFrom && !parsedTo) return {};

  const filter = {};
  filter[field] = {};
  if (parsedFrom) filter[field].$gte = parsedFrom;
  if (parsedTo) filter[field].$lte = parsedTo;
  return filter;
};

const buildDealListFilter = async (req, query = {}) => {
  const extraFilters = [];
  const normalizedStage = toCleanString(query?.stage).toLowerCase();
  const normalizedStatus = toCleanString(query?.status).toLowerCase();
  const normalizedOwnerId = toCleanString(query?.ownerId);
  const normalizedContactId = toCleanString(query?.contactId);
  const normalizedSearch = toCleanString(query?.search);
  const normalizedValueMin = toFiniteNumber(query?.valueMin);
  const normalizedValueMax = toFiniteNumber(query?.valueMax);

  if (normalizedStage && normalizedStage !== 'all') {
    if (!DEAL_STAGES.includes(normalizedStage)) {
      const error = new Error('Invalid deal stage filter');
      error.status = 400;
      throw error;
    }
    extraFilters.push({ stage: normalizedStage });
  }

  if (normalizedStatus && normalizedStatus !== 'all') {
    if (!DEAL_STATUSES.includes(normalizedStatus)) {
      const error = new Error('Invalid deal status filter');
      error.status = 400;
      throw error;
    }
    extraFilters.push({ status: normalizedStatus });
  }

  if (normalizedOwnerId) {
    extraFilters.push({ ownerId: normalizedOwnerId });
  }

  if (normalizedContactId) {
    if (!toObjectIdIfValid(normalizedContactId)) {
      const error = new Error('Invalid contactId');
      error.status = 400;
      throw error;
    }
    extraFilters.push({ contactId: normalizedContactId });
  }

  const createdRange = buildDealFieldRangeFilter('createdAt', query?.createdFrom, query?.createdTo);
  const updatedRange = buildDealFieldRangeFilter('updatedAt', query?.updatedFrom, query?.updatedTo);
  const expectedCloseRange = buildDealFieldRangeFilter(
    'expectedCloseAt',
    query?.expectedCloseFrom,
    query?.expectedCloseTo
  );

  if (Object.keys(createdRange).length > 0) extraFilters.push(createdRange);
  if (Object.keys(updatedRange).length > 0) extraFilters.push(updatedRange);
  if (Object.keys(expectedCloseRange).length > 0) extraFilters.push(expectedCloseRange);

  if (Number.isFinite(normalizedValueMin) || Number.isFinite(normalizedValueMax)) {
    const valueFilter = {};
    if (Number.isFinite(normalizedValueMin)) valueFilter.$gte = normalizedValueMin;
    if (Number.isFinite(normalizedValueMax)) valueFilter.$lte = normalizedValueMax;
    extraFilters.push({ value: valueFilter });
  }

  const searchFilter = normalizedSearch ? await buildDealSearchFilter(req, normalizedSearch) : {};
  return buildScopedFilter(req, mergeFiltersWithAnd(...extraFilters, searchFilter));
};

const buildContactQueueFilter = (req, queue) => {
  const normalizedQueue = toCleanString(queue).toLowerCase();
  if (!normalizedQueue) return {};

  const now = new Date();
  const { startOfDay, endOfDay } = getDayRange(now);

  switch (normalizedQueue) {
    case 'my_leads':
      return req?.user?.id ? { ownerId: req.user.id } : {};
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
      return null;
  }
};

const buildCrmContactScopedFilter = async (req, query = {}, options = {}) => {
  const {
    search,
    stage,
    status,
    ownerId,
    queue,
    minScore,
    maxScore,
    hasFollowUp,
    archive
  } = query || {};
  const includeStage = options.includeStage !== false;
  const extraFilter = {};
  const normalizedStage = toCleanString(stage).toLowerCase();
  const normalizedStatus = toCleanString(status).toLowerCase();
  const normalizedOwnerId = toCleanString(ownerId);

  if (includeStage && normalizedStage) {
    const stageMap = await loadPipelineStageMap(req);
    if (!stageMap.keySet.has(normalizedStage)) {
      const error = new Error('Invalid stage filter');
      error.status = 400;
      throw error;
    }
    extraFilter.stage = normalizedStage;
  }

  if (normalizedStatus) {
    if (!LEAD_STATUSES.includes(normalizedStatus)) {
      const error = new Error('Invalid status filter');
      error.status = 400;
      throw error;
    }
    extraFilter.status = normalizedStatus;
  }

  if (normalizedOwnerId) {
    extraFilter.ownerId = normalizedOwnerId;
  }

  const normalizedQueue = toCleanString(queue).toLowerCase();
  if (normalizedQueue && !CONTACT_QUEUES.includes(normalizedQueue)) {
    const error = new Error('Invalid queue filter');
    error.status = 400;
    throw error;
  }

  const minScoreNumber = Number(minScore);
  const maxScoreNumber = Number(maxScore);
  if (Number.isFinite(minScoreNumber) || Number.isFinite(maxScoreNumber)) {
    extraFilter.leadScore = {};
    if (Number.isFinite(minScoreNumber)) extraFilter.leadScore.$gte = minScoreNumber;
    if (Number.isFinite(maxScoreNumber)) extraFilter.leadScore.$lte = maxScoreNumber;
  }

  if (String(hasFollowUp || '').toLowerCase() === 'true') {
    extraFilter.nextFollowUpAt = { $ne: null };
  }
  if (String(hasFollowUp || '').toLowerCase() === 'false') {
    extraFilter.nextFollowUpAt = null;
  }

  const normalizedArchive = toCleanString(archive).toLowerCase();
  if (normalizedArchive === 'archived') {
    extraFilter.archivedAt = { $ne: null };
  } else if (normalizedArchive !== 'all') {
    extraFilter.$or = [
      { archivedAt: null },
      { archivedAt: { $exists: false } }
    ];
  }

  const normalizedSearch = toCleanString(search);
  const searchFilter = normalizedSearch
    ? {
        $or: [
          { name: { $regex: normalizedSearch, $options: 'i' } },
          { phone: { $regex: normalizedSearch, $options: 'i' } },
          { email: { $regex: normalizedSearch, $options: 'i' } },
          { source: { $regex: normalizedSearch, $options: 'i' } },
          { ownerId: { $regex: normalizedSearch, $options: 'i' } },
          { stage: { $regex: normalizedSearch, $options: 'i' } },
          { notes: { $regex: normalizedSearch, $options: 'i' } },
          { tags: { $in: [new RegExp(normalizedSearch, 'i')] } }
        ]
      }
    : {};

  const queueFilter = buildContactQueueFilter(req, normalizedQueue);
  if (normalizedQueue === 'today_calls') {
    const { startOfDay, endOfDay } = getDayRange(new Date());
    const todayCallTasks = await LeadTask.find(
      buildScopedFilter(req, {
        taskType: 'call',
        status: { $in: TASK_OPEN_STATUSES },
        dueAt: { $gte: startOfDay, $lte: endOfDay }
      })
    )
      .select('contactId')
      .lean();

    const contactIds = Array.from(
      new Set((todayCallTasks || []).map((task) => String(task?.contactId || '').trim()).filter(Boolean))
    );

    extraFilter._id = {
      $in: contactIds
        .filter((contactId) => toObjectIdIfValid(contactId))
        .map((contactId) => new mongoose.Types.ObjectId(contactId))
    };
  }

  return buildScopedFilter(req, mergeFiltersWithAnd(extraFilter, searchFilter, queueFilter || {}));
};

const buildTaskBucketFilter = (bucket) => {
  const normalizedBucket = toCleanString(bucket).toLowerCase();
  if (!normalizedBucket) return {};

  const now = new Date();
  const { startOfDay, endOfDay } = getDayRange(now);

  switch (normalizedBucket) {
    case 'overdue':
      return {
        status: { $in: TASK_OPEN_STATUSES },
        dueAt: { $ne: null, $lt: now }
      };
    case 'due_today':
      return {
        status: { $in: TASK_OPEN_STATUSES },
        dueAt: { $gte: startOfDay, $lte: endOfDay }
      };
    case 'upcoming':
      return {
        status: { $in: TASK_OPEN_STATUSES },
        dueAt: { $gt: endOfDay }
      };
    case 'completed':
      return { status: 'completed' };
    case 'open':
      return { status: { $in: TASK_OPEN_STATUSES } };
    default:
      return null;
  }
};

const buildTaskSearchFilter = async (req, search) => {
  const normalizedSearch = toCleanString(search);
  if (!normalizedSearch) return {};

  const searchRegex = new RegExp(normalizedSearch, 'i');
  const contactSearchFilter = buildScopedFilter(req, {
    $or: [
      { name: { $regex: normalizedSearch, $options: 'i' } },
      { phone: { $regex: normalizedSearch, $options: 'i' } },
      { email: { $regex: normalizedSearch, $options: 'i' } },
      { source: { $regex: normalizedSearch, $options: 'i' } },
      { ownerId: { $regex: normalizedSearch, $options: 'i' } },
      { stage: { $regex: normalizedSearch, $options: 'i' } },
      { notes: { $regex: normalizedSearch, $options: 'i' } },
      { tags: { $in: [searchRegex] } }
    ]
  });

  const matchingContactIds = await Contact.distinct('_id', contactSearchFilter);
  const normalizedContactIds = (matchingContactIds || [])
    .map((contactId) => String(contactId || '').trim())
    .filter((contactId) => toObjectIdIfValid(contactId));

  const orClauses = [
    { title: { $regex: normalizedSearch, $options: 'i' } },
    { description: { $regex: normalizedSearch, $options: 'i' } },
    { taskType: { $regex: normalizedSearch, $options: 'i' } },
    { assignedTo: { $regex: normalizedSearch, $options: 'i' } },
    { completedBy: { $regex: normalizedSearch, $options: 'i' } },
    { 'comments.text': { $regex: normalizedSearch, $options: 'i' } }
  ];

  if (normalizedContactIds.length > 0) {
    orClauses.push({
      contactId: {
        $in: normalizedContactIds.map((contactId) => new mongoose.Types.ObjectId(contactId))
      }
    });
  }

  return { $or: orClauses };
};

const TASK_PRIORITY_RANK = {
  low: 0,
  medium: 1,
  high: 2
};

const getTaskPriorityRank = (value) => {
  const normalized = toCleanString(value).toLowerCase();
  return Number.isFinite(TASK_PRIORITY_RANK[normalized]) ? TASK_PRIORITY_RANK[normalized] : TASK_PRIORITY_RANK.medium;
};

const getTaskPriorityFromRank = (rank) => {
  const normalizedRank = Number(rank);
  if (normalizedRank >= TASK_PRIORITY_RANK.high) return 'high';
  if (normalizedRank <= TASK_PRIORITY_RANK.low) return 'low';
  return 'medium';
};

const buildTaskCursorFilter = (cursorValue) => {
  const decodedCursor = decodeCursor(cursorValue);
  if (!decodedCursor?.createdAt || !decodedCursor?._id) return null;

  const cursorId = toObjectIdIfValid(decodedCursor._id);
  const cursorCreatedAt = new Date(decodedCursor.createdAt);
  if (!cursorId || Number.isNaN(cursorCreatedAt.getTime())) return null;

  const cursorDueAt = decodedCursor?.dueAt ? new Date(decodedCursor.dueAt) : null;
  const cursorPriority = getTaskPriorityFromRank(decodedCursor?.priorityRank);
  const lowerPriorityValues =
    cursorPriority === 'high'
      ? ['medium', 'low']
      : cursorPriority === 'medium'
        ? ['low']
        : [];
  const sameDueAtFilters = [
    lowerPriorityValues.length
      ? { dueAt: cursorDueAt, priority: { $in: lowerPriorityValues } }
      : null,
    {
      dueAt: cursorDueAt,
      priority: cursorPriority,
      $or: [
        { createdAt: { $lt: cursorCreatedAt } },
        { createdAt: cursorCreatedAt, _id: { $lt: cursorId } }
      ]
    }
  ].filter(Boolean);

  if (cursorDueAt === null) {
    return {
      $or: [
        ...sameDueAtFilters,
        { dueAt: { $ne: null } }
      ]
    };
  }

  return {
    $or: [
      { dueAt: { $gt: cursorDueAt } },
      ...sameDueAtFilters
    ]
  };
};

const normalizeTaskRecurrence = (recurrence = null) => {
  const source = recurrence && typeof recurrence === 'object' ? recurrence : {};
  const frequency = toCleanString(source.frequency).toLowerCase() || 'none';
  const interval = Math.min(Math.max(Number(source.interval) || 1, 1), 90);

  return {
    frequency: TASK_RECURRENCE_FREQUENCIES.includes(frequency) ? frequency : 'none',
    interval
  };
};

const buildNextRecurringDate = (dateValue, recurrence = {}) => {
  const parsedDate = safeDate(dateValue);
  const normalizedRecurrence = normalizeTaskRecurrence(recurrence);
  if (!parsedDate || normalizedRecurrence.frequency === 'none') return null;

  const nextDate = new Date(parsedDate);
  if (normalizedRecurrence.frequency === 'daily') {
    nextDate.setDate(nextDate.getDate() + normalizedRecurrence.interval);
  }
  if (normalizedRecurrence.frequency === 'weekly') {
    nextDate.setDate(nextDate.getDate() + 7 * normalizedRecurrence.interval);
  }
  if (normalizedRecurrence.frequency === 'monthly') {
    nextDate.setMonth(nextDate.getMonth() + normalizedRecurrence.interval);
  }

  return nextDate;
};

const getLeadStageRank = (stage) => LEAD_STAGE_ORDER.indexOf(toCleanString(stage).toLowerCase());

const buildContactSourceAttribution = (contact = {}) => {
  const metadata = contact?.whatsappOptInMetadata && typeof contact.whatsappOptInMetadata === 'object'
    ? contact.whatsappOptInMetadata
    : {};
  const customFields = contact?.customFields && typeof contact.customFields === 'object'
    ? contact.customFields
    : {};

  return {
    sourceType: toCleanString(contact?.sourceType || 'manual') || 'manual',
    sourceLabel: toCleanString(contact?.source) || toCleanString(contact?.sourceType) || 'Manual',
    acquisitionChannel:
      {
        public_opt_in: 'Public WhatsApp Opt-In',
        meta_lead_ads: 'Meta Lead Ads',
        incoming_call: 'Incoming Call',
        incoming_message: 'Incoming Message',
        imported: 'Imported',
        manual: 'Manual'
      }[toCleanString(contact?.sourceType).toLowerCase()] || 'Manual',
    optInSource: toCleanString(contact?.whatsappOptInSource),
    campaignId:
      toCleanString(metadata?.campaignId) ||
      toCleanString(metadata?.campaign) ||
      toCleanString(customFields?.campaignId),
    adId: toCleanString(metadata?.adId) || toCleanString(customFields?.adId),
    adSetId: toCleanString(metadata?.adSetId) || toCleanString(customFields?.adSetId),
    formId: toCleanString(metadata?.formId) || toCleanString(customFields?.formId),
    pageUrl: toCleanString(contact?.whatsappOptInPageUrl),
    proofType: toCleanString(contact?.whatsappOptInProofType),
    proofId: toCleanString(contact?.whatsappOptInProofId),
    capturedBy: toCleanString(contact?.whatsappOptInCapturedBy)
  };
};

const buildConsentAudit = (contact = {}, consentLogs = []) => ({
  status: toCleanString(contact?.whatsappOptInStatus || 'unknown') || 'unknown',
  optedInAt: contact?.whatsappOptInAt || null,
  optedOutAt: contact?.whatsappOptOutAt || null,
  scope: toCleanString(contact?.whatsappOptInScope || 'unknown') || 'unknown',
  source: toCleanString(contact?.whatsappOptInSource),
  proofType: toCleanString(contact?.whatsappOptInProofType),
  proofId: toCleanString(contact?.whatsappOptInProofId),
  proofUrl: toCleanString(contact?.whatsappOptInProofUrl),
  consentText: toCleanString(contact?.whatsappOptInTextSnapshot),
  logs: Array.isArray(consentLogs) ? consentLogs : []
});

const buildUnifiedTimeline = ({ activities = [], messages = [] } = {}) => {
  const activityItems = (Array.isArray(activities) ? activities : []).map((activity, index) => ({
    _id: String(activity?._id || `activity-${index}`).trim(),
    type: 'activity',
    createdAt: activity?.createdAt || null,
    payload: activity
  }));

  const messageItems = (Array.isArray(messages) ? messages : []).map((messageItem, index) => ({
    _id: String(messageItem?._id || `message-${index}`).trim(),
    type: 'message',
    createdAt: messageItem?.timestamp || messageItem?.createdAt || null,
    payload: messageItem
  }));

  return [...activityItems, ...messageItems]
    .sort((left, right) => new Date(right.createdAt || 0) - new Date(left.createdAt || 0))
    .slice(0, 40);
};

const buildLeadScoringInsight = (contact = {}, settings = null) => {
  const automation = settings?.automation || {};
  const leadScore = Number(contact?.leadScore || 0);

  return {
    score: leadScore,
    breakdown: contact?.leadScoreBreakdown || { read: 0, reply: 0, keyword: 0 },
    automationEnabled: automation?.isEnabled === true,
    stageThreshold: Number(automation?.stageThreshold || 0),
    taskThreshold: Number(automation?.taskThreshold || 0),
    recommendedTemplate:
      automation?.isEnabled && leadScore >= Number(automation?.taskThreshold || 0)
        ? toCleanString(automation?.recommendedTemplate)
        : '',
    stageRecommendation:
      automation?.isEnabled &&
      leadScore >= Number(automation?.stageThreshold || 0) &&
      getLeadStageRank(contact?.stage) < getLeadStageRank(automation?.stageOnThreshold)
        ? toCleanString(automation?.stageOnThreshold)
        : ''
  };
};

const buildActivityScopeFilter = (req, extra = {}) => {
  const normalizedCompanyId = toObjectIdIfValid(req?.companyId);
  const conditions = [];

  if (normalizedCompanyId) {
    conditions.push({ companyId: normalizedCompanyId });
  } else if (req?.companyId) {
    conditions.push({ companyId: req.companyId });
  }

  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }

  if (conditions.length === 0) return extra || {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const normalizeActivityListLimit = (value, fallback = 40, max = 200) =>
  Math.min(Math.max(Number(value) || fallback, 1), max);

const buildMeetingListItem = (activity = {}) => {
  const contact = activity?.contactId || {};
  return {
    _id: String(activity?._id || '').trim(),
    type: 'meeting_scheduled',
    createdAt: activity?.createdAt || null,
    contact: {
      _id: String(contact?._id || '').trim(),
      name: toCleanString(contact?.name),
      phone: toCleanString(contact?.phone),
      stage: toCleanString(contact?.stage),
      ownerId: toCleanString(contact?.ownerId),
      leadScore: Number(contact?.leadScore || 0)
    },
    summary: toCleanString(activity?.meta?.summary) || 'Meeting',
    meetingUrl: toCleanString(activity?.meta?.meetingUrl),
    eventId: toCleanString(activity?.meta?.eventId),
    eventHtmlLink: toCleanString(activity?.meta?.eventHtmlLink),
    start: activity?.meta?.start || null,
    end: activity?.meta?.end || null,
    createdBy: toCleanString(activity?.createdBy)
  };
};

const buildOwnerNotificationItem = (activity = {}) => {
  const contact = activity?.contactId || {};
  const meta = activity?.meta && typeof activity.meta === 'object' ? activity.meta : {};
  const readAt = meta?.readAt || null;

  return {
    _id: String(activity?._id || '').trim(),
    type: 'owner_notified',
    createdAt: activity?.createdAt || null,
    readAt,
    isRead: Boolean(readAt),
    ownerId: toCleanString(meta?.ownerId),
    automationRule: toCleanString(meta?.automationRule),
    recommendedTemplate: toCleanString(meta?.recommendedTemplate),
    leadScore: Number(meta?.leadScore || 0),
    contact: {
      _id: String(contact?._id || '').trim(),
      name: toCleanString(contact?.name),
      phone: toCleanString(contact?.phone),
      stage: toCleanString(contact?.stage),
      ownerId: toCleanString(contact?.ownerId),
      leadScore: Number(contact?.leadScore || 0)
    }
  };
};

const buildBroadcastScopeFilter = (req, extra = {}) => {
  const conditions = [];

  if (req?.companyId) {
    conditions.push({ companyId: req.companyId });
  }

  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }

  if (conditions.length === 0) return extra || {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const attachBroadcastContextToMessages = async (req, messages = []) => {
  const list = Array.isArray(messages) ? messages : [];
  const broadcastIds = Array.from(
    new Set(
      list
        .map((message) => String(message?.broadcastId || '').trim())
        .filter(Boolean)
    )
  )
    .map((value) => toObjectIdIfValid(value))
    .filter(Boolean);

  if (!broadcastIds.length) return list;

  const broadcasts = await Broadcast.find(
    buildBroadcastScopeFilter(req, { _id: { $in: broadcastIds } })
  )
    .select('_id name status messageType templateName')
    .lean();

  const broadcastMap = new Map(
    (broadcasts || []).map((broadcast) => [String(broadcast?._id || '').trim(), broadcast])
  );

  return list.map((message) => {
    const broadcast = broadcastMap.get(String(message?.broadcastId || '').trim());
    if (!broadcast) return message;

    return {
      ...message,
      broadcastName: toCleanString(broadcast?.name),
      broadcastStatus: toCleanString(broadcast?.status),
      broadcastMessageType: toCleanString(broadcast?.messageType),
      broadcastTemplateName: toCleanString(broadcast?.templateName)
    };
  });
};

const buildDealSummary = async (req, extraFilter = {}) => {
  const [dealMetrics] = await Deal.aggregate([
    {
      $match: buildScopedFilter(req, extraFilter)
    },
    {
      $project: {
        stage: { $toLower: { $ifNull: ['$stage', 'discovery'] } },
        status: { $toLower: { $ifNull: ['$status', 'open'] } },
        value: { $ifNull: ['$value', 0] },
        probability: { $ifNull: ['$probability', 0] }
      }
    },
    {
      $project: {
        stage: 1,
        status: 1,
        value: 1,
        weightedValue: {
          $multiply: ['$value', { $divide: ['$probability', 100] }]
        }
      }
    },
    {
      $facet: {
        summary: [
          {
            $group: {
              _id: null,
              total: { $sum: 1 },
              open: {
                $sum: {
                  $cond: [{ $eq: ['$status', 'open'] }, 1, 0]
                }
              },
              won: {
                $sum: {
                  $cond: [{ $eq: ['$status', 'won'] }, 1, 0]
                }
              },
              lost: {
                $sum: {
                  $cond: [{ $eq: ['$status', 'lost'] }, 1, 0]
                }
              },
              pipelineValue: {
                $sum: {
                  $cond: [{ $eq: ['$status', 'open'] }, '$value', 0]
                }
              },
              weightedValue: {
                $sum: {
                  $cond: [{ $eq: ['$status', 'open'] }, '$weightedValue', 0]
                }
              },
              wonValue: {
                $sum: {
                  $cond: [{ $eq: ['$status', 'won'] }, '$value', 0]
                }
              }
            }
          }
        ],
        byStage: [
          {
            $group: {
              _id: '$stage',
              count: { $sum: 1 },
              value: { $sum: '$value' }
            }
          }
        ]
      }
    }
  ]);

  const summary = dealMetrics?.summary?.[0] || {};
  const byStage = (dealMetrics?.byStage || []).reduce((accumulator, item) => {
    const key = toCleanString(item?._id).toLowerCase() || 'discovery';
    accumulator[key] = {
      count: Number(item?.count || 0),
      value: Number(item?.value || 0)
    };
    return accumulator;
  }, {});

  return {
    total: Number(summary.total || 0),
    open: Number(summary.open || 0),
    won: Number(summary.won || 0),
    lost: Number(summary.lost || 0),
    pipelineValue: Number(summary.pipelineValue || 0),
    weightedValue: Number(summary.weightedValue || 0),
    wonValue: Number(summary.wonValue || 0),
    byStage
  };
};

const syncContactDealSnapshot = async (req, contactId) => {
  if (!toObjectIdIfValid(contactId)) return null;

  try {
    const contact = await Contact.findOne(buildScopedFilter(req, { _id: contactId }));
    if (!contact) return null;

    const dealSummary = await buildDealSummary(req, { contactId });
    contact.dealValue = Number(dealSummary.pipelineValue || 0);
    await contact.save();
    return contact;
  } catch (error) {
    console.error('CRM deal snapshot sync failed:', error.message);
    return null;
  }
};

const safeDate = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const toCleanStringArray = (value) => {
  if (Array.isArray(value)) {
    return value.map((item) => toCleanString(item)).filter(Boolean);
  }

  const normalized = String(value || '').trim();
  if (!normalized) return [];

  if ((normalized.startsWith('[') && normalized.endsWith(']')) || (normalized.startsWith('"') && normalized.endsWith('"'))) {
    try {
      const parsed = JSON.parse(normalized);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => toCleanString(item)).filter(Boolean);
      }
    } catch (_error) {
      // Fall through to comma splitting.
    }
  }

  return normalized
    .split(',')
    .map((item) => toCleanString(item))
    .filter(Boolean);
};

const buildTaskSummary = async (req, extraFilter = {}) => {
  const now = new Date();
  const { startOfDay, endOfDay } = getDayRange(now);

  const totalFilter = buildScopedFilter(req, extraFilter);
  const openFilter = buildScopedFilter(req, mergeFiltersWithAnd(extraFilter, { status: { $in: TASK_OPEN_STATUSES } }));
  const overdueFilter = buildScopedFilter(
    req,
    mergeFiltersWithAnd(extraFilter, {
      status: { $in: TASK_OPEN_STATUSES },
      dueAt: { $ne: null, $lt: now }
    })
  );
  const dueTodayFilter = buildScopedFilter(
    req,
    mergeFiltersWithAnd(extraFilter, {
      status: { $in: TASK_OPEN_STATUSES },
      dueAt: { $gte: startOfDay, $lte: endOfDay }
    })
  );
  const completedFilter = buildScopedFilter(req, mergeFiltersWithAnd(extraFilter, { status: 'completed' }));
  const highPriorityFilter = buildScopedFilter(
    req,
    mergeFiltersWithAnd(extraFilter, { status: { $in: TASK_OPEN_STATUSES }, priority: 'high' })
  );
  const todayCallsFilter = buildScopedFilter(
    req,
    mergeFiltersWithAnd(extraFilter, {
      status: { $in: TASK_OPEN_STATUSES },
      taskType: 'call',
      dueAt: { $gte: startOfDay, $lte: endOfDay }
    })
  );
  const followUpTotalFilter = buildScopedFilter(
    req,
    mergeFiltersWithAnd(extraFilter, { taskType: 'follow_up' })
  );
  const followUpCompletedFilter = buildScopedFilter(
    req,
    mergeFiltersWithAnd(extraFilter, { taskType: 'follow_up', status: 'completed' })
  );

  const [total, open, overdue, dueToday, completed, highPriority, todayCalls, followUpTotal, followUpCompleted] = await Promise.all([
    LeadTask.countDocuments(totalFilter),
    LeadTask.countDocuments(openFilter),
    LeadTask.countDocuments(overdueFilter),
    LeadTask.countDocuments(dueTodayFilter),
    LeadTask.countDocuments(completedFilter),
    LeadTask.countDocuments(highPriorityFilter),
    LeadTask.countDocuments(todayCallsFilter),
    LeadTask.countDocuments(followUpTotalFilter),
    LeadTask.countDocuments(followUpCompletedFilter)
  ]);

  return {
    total,
    open,
    overdue,
    dueToday,
    completed,
    highPriority,
    todayCalls,
    completionRate: total > 0 ? Number(((completed / total) * 100).toFixed(1)) : 0,
    followUpCompletionRate:
      followUpTotal > 0 ? Number(((followUpCompleted / followUpTotal) * 100).toFixed(1)) : 0
  };
};

const loadMergedContactActivity = async (req, contactId, limit = 100) => {
  const scopedFilter = buildScopedFilter(req, { contactId });
  const [activities, consentLogs] = await Promise.all([
    LeadActivity.find(scopedFilter)
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean(),
    WhatsAppConsentLog.find(scopedFilter)
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean()
  ]);

  const consentActivities = (consentLogs || []).map((log) => ({
    _id: log._id,
    userId: log.userId,
    companyId: log.companyId,
    contactId: log.contactId,
    conversationId: null,
    type: log.action === 'opt_out' ? 'whatsapp_opt_out' : 'whatsapp_opt_in',
    meta: {
      source: log.source,
      scope: log.scope,
      proofType: log.proofType,
      proofId: log.proofId,
      capturedBy: log.capturedBy,
      consentText: log.consentText
    },
    createdAt: log.createdAt
  }));

  return [...activities, ...consentActivities]
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))
    .slice(0, limit);
};

const logLeadActivity = async ({
  req,
  contactId,
  conversationId = null,
  type,
  meta = {}
}) => {
  try {
    if (!contactId) return;
    await LeadActivity.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId,
      conversationId,
      type,
      meta,
      createdBy: req.user.id || null
    });
  } catch (error) {
    console.error('CRM activity log failed:', error.message);
  }
};

const findRecentConversationIdForContact = async (req, contactId) => {
  const conversation = await Conversation.findOne(
    buildScopedFilter(req, { contactId })
  )
    .sort({ lastMessageTime: -1, updatedAt: -1 })
    .select('_id');

  return conversation?._id || null;
};

const loadAuthorizedContact = async (req, contactId) => {
  if (!toObjectIdIfValid(contactId)) {
    const error = new Error('Invalid contact id');
    error.status = 400;
    throw error;
  }

  const contact = await Contact.findOne(buildScopedFilter(req, { _id: contactId }));
  if (!contact) {
    const error = new Error('Contact not found');
    error.status = 404;
    throw error;
  }

  return contact;
};

const resolveAuthorizedContactForDocumentWrite = async (req, contactId, conversationId) => {
  try {
    return await loadAuthorizedContact(req, contactId);
  } catch (error) {
    const normalizedConversationId = String(conversationId || '').trim();
    if ((error?.status || 500) !== 404 || !toObjectIdIfValid(normalizedConversationId)) {
      throw error;
    }

    const conversation = await Conversation.findOne(
      buildScopedFilter(req, { _id: normalizedConversationId })
    )
      .select('_id contactId')
      .lean();

    if (!conversation?.contactId) {
      throw error;
    }

    return loadAuthorizedContact(req, conversation.contactId);
  }
};

const resolveDocumentConversationId = async (req, contact, conversationId) => {
  const normalizedConversationId = String(conversationId || '').trim();
  if (!normalizedConversationId) {
    return findRecentConversationIdForContact(req, contact._id);
  }

  if (!toObjectIdIfValid(normalizedConversationId)) {
    return findRecentConversationIdForContact(req, contact._id);
  }

  const conversation = await Conversation.findOne(
    buildScopedFilter(req, {
      _id: normalizedConversationId,
      contactId: contact._id
    })
  ).select('_id');

  if (!conversation) {
    return findRecentConversationIdForContact(req, contact._id);
  }

  return conversation._id;
};

const runSingleDocumentUpload = (req, res) =>
  new Promise((resolve, reject) => {
    upload.single('file')(req, res, (error) => {
      if (error) return reject(error);
      resolve(req.file);
    });
  });

const buildDocumentAccessPayload = ({ document, mode }) => {
  const attachment = document?.attachment || {};
  const directUrl = String(attachment?.secureUrl || '').trim();
  const accessPayload =
    String(mode || '').trim().toLowerCase() === 'download'
      ? generateAttachmentDownloadUrl({
          attachment: {
            ...attachment,
            secureUrl: directUrl
          }
        })
      : generateSignedAttachmentUrl({
          attachment: {
            ...attachment,
            secureUrl: directUrl
          },
          mode: 'view'
        });

  return {
    url: String(accessPayload?.url || directUrl || '').trim(),
    expiresAt: accessPayload?.expiresAt || null,
    fileName: String(attachment?.originalFileName || document?.title || 'document').trim() || 'document'
  };
};

const getCrmRouteErrorStatus = (error) => {
  if (String(error?.code || '').trim().toUpperCase() === 'LIMIT_FILE_SIZE') {
    return 413;
  }
  return error?.status || 500;
};

router.get('/filter-presets', async (req, res) => {
  try {
    const presets = await CrmPipelineView.find({
      userId: String(req.user?.id || '').trim(),
      ...(req.companyId ? { companyId: String(req.companyId).trim() } : {}),
      presetType: 'filter_preset'
    })
      .sort({ updatedAt: -1, createdAt: -1 })
      .lean();

    res.json({ success: true, data: presets.map(toFilterPresetResponse) });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/filter-presets', async (req, res) => {
  try {
    const label = toCleanString(req.body?.label);
    if (!label) {
      return res.status(400).json({ success: false, error: 'Preset label is required' });
    }

    const preset = await CrmPipelineView.create({
      userId: String(req.user?.id || '').trim(),
      companyId: req.companyId ? String(req.companyId).trim() : '',
      label,
      filters: normalizeFilterPresetFilters(req.body?.filters || {}),
      presetType: 'filter_preset'
    });

    emitCrmRealtimeEvent(req, {
      entity: 'filter_preset',
      action: 'created',
      filterPresetId: String(preset._id || '').trim()
    });

    res.status(201).json({ success: true, data: toFilterPresetResponse(preset) });
  } catch (error) {
    if (String(error?.code || '') === '11000') {
      return res.status(409).json({ success: false, error: 'A filter preset with that name already exists.' });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/filter-presets/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid preset id' });
    }

    const preset = await CrmPipelineView.findOne({
      _id: id,
      userId: String(req.user?.id || '').trim(),
      ...(req.companyId ? { companyId: String(req.companyId).trim() } : {}),
      presetType: 'filter_preset'
    });

    if (!preset) {
      return res.status(404).json({ success: false, error: 'Filter preset not found' });
    }

    if (req.body?.label !== undefined) {
      const label = toCleanString(req.body.label);
      if (!label) {
        return res.status(400).json({ success: false, error: 'Preset label is required' });
      }
      preset.label = label;
    }
    if (req.body?.filters !== undefined) {
      preset.filters = normalizeFilterPresetFilters(req.body.filters);
    }
    await preset.save();

    emitCrmRealtimeEvent(req, {
      entity: 'filter_preset',
      action: 'updated',
      filterPresetId: String(preset._id || '').trim()
    });

    res.json({ success: true, data: toFilterPresetResponse(preset) });
  } catch (error) {
    if (String(error?.code || '') === '11000') {
      return res.status(409).json({ success: false, error: 'A filter preset with that name already exists.' });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

router.delete('/filter-presets/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid preset id' });
    }

    const preset = await CrmPipelineView.findOneAndDelete({
      _id: id,
      userId: String(req.user?.id || '').trim(),
      ...(req.companyId ? { companyId: String(req.companyId).trim() } : {}),
      presetType: 'filter_preset'
    });

    if (!preset) {
      return res.status(404).json({ success: false, error: 'Filter preset not found' });
    }

    emitCrmRealtimeEvent(req, {
      entity: 'filter_preset',
      action: 'deleted',
      filterPresetId: String(preset._id || '').trim()
    });

    res.json({ success: true, data: { id: String(preset._id || '').trim() } });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/pipeline-stages', async (req, res) => {
  try {
    const stages = await loadPipelineStages(req);
    const stageCounts = await Contact.aggregate([
      { $match: buildScopedFilter(req) },
      {
        $group: {
          _id: { $toLower: { $ifNull: ['$stage', 'new'] } },
          count: { $sum: 1 }
        }
      }
    ]);

    const countMap = (stageCounts || []).reduce((accumulator, item) => {
      const key = toCleanString(item?._id).toLowerCase();
      if (key) accumulator[key] = Number(item?.count || 0);
      return accumulator;
    }, {});

    res.json({
      success: true,
      data: {
        stages: stages.map((stage) => ({
          ...stage,
          contactCount: Number(countMap[stage.key] || 0)
        })),
        apiAvailable: true
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/pipeline-stages', async (req, res) => {
  try {
    const label = toCleanString(req.body?.label);
    const color = toCleanString(req.body?.color) || '#5f8fc3';
    if (!label) {
      return res.status(400).json({ success: false, error: 'Stage label is required' });
    }

    const scope = buildPipelineStageScope(req);
    const stages = await loadPipelineStages(req);
    const normalizedLabel = label.toLowerCase();
    const labelExists = stages.some((stage) => String(stage.label || '').trim().toLowerCase() === normalizedLabel);
    if (labelExists) {
      return res.status(409).json({ success: false, error: 'A pipeline stage with that name already exists.' });
    }

    const existingKeys = new Set(stages.map((stage) => stage.key));
    let key = slugifyPipelineStageKey(label);
    if (existingKeys.has(key)) {
      let suffix = 2;
      while (existingKeys.has(`${key}_${suffix}`)) {
        suffix += 1;
      }
      key = `${key}_${suffix}`;
    }

    const nextOrder = stages.length ? Math.max(...stages.map((stage) => Number(stage.order) || 0)) + 1 : 0;
    const stage = await CrmPipelineStage.create({
      ...scope,
      key,
      label,
      color,
      order: nextOrder,
      isArchived: false
    });

    emitCrmRealtimeEvent(req, {
      entity: 'pipeline_stage',
      action: 'created',
      pipelineStageId: String(stage._id || '').trim()
    });

    res.status(201).json({
      success: true,
      data: normalizePipelineStageResponse(stage)
    });
  } catch (error) {
    if (String(error?.code || '') === '11000') {
      return res.status(409).json({ success: false, error: 'A pipeline stage with that key already exists.' });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/pipeline-stages/reorder', async (req, res) => {
  try {
    const requestedStageIds = Array.isArray(req.body?.stageIds)
      ? req.body.stageIds.map((value) => toCleanString(value)).filter(Boolean)
      : [];

    if (!requestedStageIds.length) {
      return res.status(400).json({
        success: false,
        error: 'Stage ids are required'
      });
    }

    const uniqueStageIds = Array.from(new Set(requestedStageIds));
    if (uniqueStageIds.length !== requestedStageIds.length) {
      return res.status(400).json({
        success: false,
        error: 'Stage ids must be unique'
      });
    }

    const scope = buildPipelineStageScope(req);
    const stages = await CrmPipelineStage.find({
      ...scope,
      isArchived: { $ne: true }
    })
      .sort({ order: 1, createdAt: 1 })
      .lean();

    if (stages.length !== uniqueStageIds.length) {
      return res.status(400).json({
        success: false,
        error: 'Stage order must include every active pipeline stage'
      });
    }

    const stageMap = new Map(stages.map((stage) => [String(stage._id || '').trim(), stage]));
    const unknownStageIds = uniqueStageIds.filter((stageId) => !stageMap.has(stageId));
    if (unknownStageIds.length > 0) {
      return res.status(400).json({
        success: false,
        error: 'Stage order contains an unknown stage id'
      });
    }

    const orderedStages = uniqueStageIds
      .map((stageId) => stageMap.get(stageId))
      .filter(Boolean);

    const orderedStageIdSet = new Set(uniqueStageIds);
    const remainingStages = stages.filter((stage) => !orderedStageIdSet.has(String(stage._id || '').trim()));
    const nextOrderedStages = [...orderedStages, ...remainingStages];

    await Promise.all(
      nextOrderedStages.map((stage, index) =>
        CrmPipelineStage.updateOne(
          { _id: stage._id, ...scope },
          { $set: { order: index } }
        )
      )
    );

    const nextStages = await CrmPipelineStage.find({
      ...scope,
      isArchived: { $ne: true }
    })
      .sort({ order: 1, createdAt: 1 })
      .lean();

    emitCrmRealtimeEvent(req, {
      entity: 'pipeline_stage',
      action: 'reordered',
      pipelineStageIds: uniqueStageIds
    });

    res.json({
      success: true,
      data: {
        stageIds: nextOrderedStages.map((stage) => String(stage._id || '').trim()),
        stages: nextStages.map(normalizePipelineStageResponse)
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/pipeline-stages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid stage id' });
    }

    const stage = await CrmPipelineStage.findOne({
      _id: id,
      ...buildPipelineStageScope(req)
    });

    if (!stage) {
      return res.status(404).json({ success: false, error: 'Pipeline stage not found' });
    }

    if (req.body?.label !== undefined) {
      const label = toCleanString(req.body.label);
      if (!label) {
        return res.status(400).json({ success: false, error: 'Stage label is required' });
      }
      stage.label = label;
    }

    if (req.body?.color !== undefined) {
      stage.color = toCleanString(req.body.color) || stage.color;
    }

    if (req.body?.order !== undefined) {
      const normalizedOrder = Number(req.body.order);
      if (!Number.isFinite(normalizedOrder) || normalizedOrder < 0) {
        return res.status(400).json({ success: false, error: 'Stage order must be a non-negative number' });
      }
      stage.order = normalizedOrder;
    }

    if (req.body?.isArchived !== undefined) {
      stage.isArchived = Boolean(req.body.isArchived);
    }

    await stage.save();

    emitCrmRealtimeEvent(req, {
      entity: 'pipeline_stage',
      action: 'updated',
      pipelineStageId: String(stage._id || '').trim()
    });

    res.json({
      success: true,
      data: normalizePipelineStageResponse(stage)
    });
  } catch (error) {
    if (String(error?.code || '') === '11000') {
      return res.status(409).json({ success: false, error: 'A pipeline stage with that name already exists.' });
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

router.delete('/pipeline-stages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid stage id' });
    }

    const scope = buildPipelineStageScope(req);
    const stage = await CrmPipelineStage.findOne({
      _id: id,
      ...scope
    });

    if (!stage) {
      return res.status(404).json({ success: false, error: 'Pipeline stage not found' });
    }

    const stages = await CrmPipelineStage.find({
      ...scope,
      _id: { $ne: stage._id },
      isArchived: { $ne: true }
    })
      .sort({ order: 1, createdAt: 1 })
      .lean();

    if (!stages.length) {
      return res.status(400).json({
        success: false,
        error: 'At least one pipeline stage must remain active.'
      });
    }

    const contactCount = await Contact.countDocuments(buildScopedFilter(req, { stage: stage.key }));
    const fallbackStageKey = toCleanString(req.body?.fallbackStageKey).toLowerCase();
    const fallbackStageId = toCleanString(req.body?.fallbackStageId);
    const fallbackStage =
      stages.find((item) => item.key === fallbackStageKey) ||
      stages.find((item) => String(item._id || '') === fallbackStageId) ||
      null;

    if (contactCount > 0 && !fallbackStage) {
      return res.status(409).json({
        success: false,
        error: 'This stage still has leads. Choose a fallback stage before deleting it.',
        contactCount
      });
    }

    if (contactCount > 0 && fallbackStage) {
      const fallbackStatus = getPipelineStageStatus(fallbackStage.key);
      const updatePayload = {
        stage: fallbackStage.key,
        lastStageChangedAt: new Date()
      };
      if (fallbackStatus) {
        updatePayload.status = fallbackStatus;
      }

      await Contact.updateMany(buildScopedFilter(req, { stage: stage.key }), {
        $set: updatePayload
      });
    }

    await CrmPipelineStage.deleteOne({ _id: stage._id, ...scope });

    emitCrmRealtimeEvent(req, {
      entity: 'pipeline_stage',
      action: 'deleted',
      pipelineStageId: String(stage._id || '').trim(),
      fallbackStageKey: fallbackStage ? fallbackStage.key : '',
      movedContactCount: Number(contactCount || 0)
    });

    res.json({
      success: true,
      data: {
        id: String(stage._id || '').trim(),
        movedContactCount: Number(contactCount || 0),
        fallbackStageKey: fallbackStage ? fallbackStage.key : ''
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/contacts', async (req, res) => {
  try {
    const { page = 1, limit = 50, cursor = '' } = req.query;
    const sortOrder = toCleanString(req.query?.sortOrder).toLowerCase();
    const useCursor = Boolean(toCleanString(cursor)) || toCleanString(req.query?.cursorMode).toLowerCase() === 'true';
    const contactSort =
      sortOrder === 'oldest'
        ? { createdAt: 1, _id: 1 }
        : sortOrder === 'newest'
          ? { createdAt: -1, _id: -1 }
          : { nextFollowUpAt: 1, leadScore: -1, lastContact: -1, createdAt: -1 };
    const pageNumber = Math.max(Number(page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const skip = (pageNumber - 1) * pageSize;
    let scopedFilter = await buildCrmContactScopedFilter(req, req.query);
    const totalFilter = scopedFilter;
    const decodedCursor = decodeCursor(cursor);
    if (useCursor && decodedCursor?.createdAt && decodedCursor?._id) {
      const cursorDate = new Date(decodedCursor.createdAt);
      const cursorId = toObjectIdIfValid(decodedCursor._id);
      if (!Number.isNaN(cursorDate.getTime()) && cursorId) {
        const cursorFilter =
          sortOrder === 'oldest'
            ? {
                $or: [
                  { createdAt: { $gt: cursorDate } },
                  { createdAt: cursorDate, _id: { $gt: new mongoose.Types.ObjectId(cursorId) } }
                ]
              }
            : {
                $or: [
                  { createdAt: { $lt: cursorDate } },
                  { createdAt: cursorDate, _id: { $lt: new mongoose.Types.ObjectId(cursorId) } }
                ]
              };
        scopedFilter = mergeFiltersWithAnd(scopedFilter, cursorFilter);
      }
    }
    const projection = buildContactProjection(req.query?.fields);

    const [contacts, total] = await Promise.all([
      Contact.find(scopedFilter)
        .select(projection)
        .sort(contactSort)
        .skip(useCursor ? 0 : skip)
        .limit(useCursor ? pageSize + 1 : pageSize)
        .lean(),
      Contact.countDocuments(totalFilter)
    ]);
    const pageContacts = useCursor ? contacts.slice(0, pageSize) : contacts;
    const lastContact = pageContacts[pageContacts.length - 1] || null;
    const nextCursor =
      useCursor && contacts.length > pageSize && lastContact
        ? encodeCursor({
            createdAt: lastContact.createdAt || new Date(0).toISOString(),
            _id: String(lastContact._id || '')
          })
        : '';

    res.json({
      success: true,
      data: pageContacts,
      nextCursor,
      hasMore: useCursor ? contacts.length > pageSize : pageNumber < Math.max(Math.ceil(total / pageSize), 1),
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total,
        totalPages: Math.max(Math.ceil(total / pageSize), 1)
      }
    });
  } catch (error) {
    res.status(error?.status || 500).json({ success: false, error: error.message });
  }
});

router.post('/contacts/bulk', async (req, res) => {
  try {
    const action = toCleanString(req.body?.action).toLowerCase();
    const validContactIds = Array.isArray(req.body?.contactIds)
      ? req.body.contactIds.filter((contactId) => toObjectIdIfValid(contactId))
      : [];
    if (!['assign', 'add_tags', 'remove_tags', 'archive', 'unarchive', 'export', 'delete'].includes(action)) {
      return res.status(400).json({ success: false, error: 'Invalid bulk action' });
    }
    if (!validContactIds.length) {
      return res.status(400).json({ success: false, error: 'Choose at least one lead.' });
    }

    const filter = buildScopedFilter(req, {
      _id: { $in: validContactIds.map((contactId) => new mongoose.Types.ObjectId(contactId)) }
    });
    const contacts = await Contact.find(filter).select(CRM_CONTACT_LIST_FIELDS).lean();
    if (!contacts.length) {
      return res.status(404).json({ success: false, error: 'No leads found for bulk action' });
    }

    if (action === 'export') {
      return res.json({
        success: true,
        data: {
          action,
          count: contacts.length,
          contacts
        }
      });
    }

    if (action === 'delete') {
      const actorId = toCleanString(req.user?.id);
      const result = await Contact.deleteMany(filter);
      await Promise.all(
        contacts.map((contact) =>
          logLeadActivity({
            req,
            contactId: contact._id,
            type: 'contact_deleted',
            meta: {
              bulkAction: action
            },
            createdBy: actorId
          })
        )
      );

      emitCrmRealtimeEvent(req, {
        entity: 'contact',
        action: 'bulk_deleted',
        bulkAction: action,
        contactIds: contacts.map((contact) => String(contact._id || '')),
        count: Number(result?.deletedCount || contacts.length)
      });

      return res.json({
        success: true,
        data: {
          action,
          count: Number(result?.deletedCount || contacts.length)
        }
      });
    }

    const now = new Date();
    const actorId = toCleanString(req.user?.id);
    const update = {};
    if (action === 'assign') {
      update.$set = { ownerId: toCleanString(req.body?.ownerId) || null };
    } else if (action === 'add_tags') {
      const tags = toCleanStringArray(req.body?.tags);
      update.$addToSet = { tags: { $each: tags } };
    } else if (action === 'remove_tags') {
      const tags = toCleanStringArray(req.body?.tags);
      update.$pull = { tags: { $in: tags } };
    } else if (action === 'archive') {
      update.$set = { archivedAt: now, archivedBy: actorId };
    } else if (action === 'unarchive') {
      update.$set = { archivedAt: null, archivedBy: null };
    }

    const result = await Contact.updateMany(filter, update);
    const activityType = action === 'assign' ? 'owner_changed' : 'contact_updated';
    await Promise.all(
      contacts.map((contact) =>
        logLeadActivity({
          req,
          contactId: contact._id,
          type: activityType,
          meta: {
            bulkAction: action,
            ownerId: action === 'assign' ? toCleanString(req.body?.ownerId) || null : undefined,
            tags: ['add_tags', 'remove_tags'].includes(action) ? toCleanStringArray(req.body?.tags) : undefined
          },
          createdBy: actorId
        })
      )
    );

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'bulk_updated',
      bulkAction: action,
      contactIds: contacts.map((contact) => String(contact._id || '')),
      count: Number(result?.modifiedCount || contacts.length)
    });

    res.json({
      success: true,
      data: {
        action,
        count: Number(result?.modifiedCount || contacts.length)
      }
    });
  } catch (error) {
    res.status(error?.status || 500).json({ success: false, error: error.message });
  }
});

router.get('/contacts/summary', async (req, res) => {
  try {
    const scopedFilter = await buildCrmContactScopedFilter(req, req.query, { includeStage: false });

    const [summaryResult] = await Contact.aggregate([
      { $match: scopedFilter },
      {
        $project: {
          stage: { $toLower: { $ifNull: ['$stage', 'new'] } },
          status: { $toLower: { $ifNull: ['$status', 'new'] } },
          leadScore: { $ifNull: ['$leadScore', 0] },
          dealValue: { $ifNull: ['$dealValue', 0] }
        }
      },
      {
        $facet: {
          total: [
            {
              $group: {
                _id: null,
                count: { $sum: 1 },
                value: { $sum: '$dealValue' },
                qualified: {
                  $sum: {
                    $cond: [
                      { $or: [{ $eq: ['$status', 'qualified'] }, { $eq: ['$stage', 'qualified'] }] },
                      1,
                      0
                    ]
                  }
                },
                averageLeadScore: { $avg: '$leadScore' }
              }
            }
          ],
          byStage: [
            {
              $group: {
                _id: '$stage',
                count: { $sum: 1 },
                value: { $sum: '$dealValue' },
                averageLeadScore: { $avg: '$leadScore' }
              }
            }
          ]
        }
      }
    ]);

    const total = summaryResult?.total?.[0] || {};
    const byStage = (summaryResult?.byStage || []).reduce((acc, item) => {
      const key = toCleanString(item?._id).toLowerCase() || 'new';
      acc[key] = {
        count: Number(item?.count || 0),
        value: Number(item?.value || 0),
        averageLeadScore: Number(Number(item?.averageLeadScore || 0).toFixed(2))
      };
      return acc;
    }, {});

    res.json({
      success: true,
      data: {
        total: Number(total?.count || 0),
        value: Number(total?.value || 0),
        qualified: Number(total?.qualified || 0),
        averageLeadScore: Number(Number(total?.averageLeadScore || 0).toFixed(2)),
        byStage,
        generatedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(error?.status || 500).json({ success: false, error: error.message });
  }
});

router.get('/contacts/owners', async (req, res) => {
  try {
    const ownerQuery = {
      ...(req.query || {}),
      ownerId: ''
    };
    const scopedFilter = await buildCrmContactScopedFilter(req, ownerQuery, { includeStage: false });
    const owners = await Contact.distinct('ownerId', scopedFilter);

    res.json({
      success: true,
      data: (Array.isArray(owners) ? owners : [])
        .map((ownerId) => toCleanString(ownerId))
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right))
        .slice(0, 200)
    });
  } catch (error) {
    res.status(error?.status || 500).json({ success: false, error: error.message });
  }
});

router.get('/contacts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const [
      taskSummary,
      dealSummary,
      recentTasks,
      recentDeals,
      recentDocuments,
      recentActivities,
      recentConversation,
      recentConsentLogs,
      leadScoringSettings
    ] = await Promise.all([
      buildTaskSummary(req, { contactId: contact._id }),
      buildDealSummary(req, { contactId: contact._id }),
      LeadTask.find(buildScopedFilter(req, { contactId: contact._id }))
        .sort({ dueAt: 1, createdAt: -1 })
        .limit(6)
        .lean(),
      Deal.find(buildScopedFilter(req, { contactId: contact._id }))
        .populate('contactId', CRM_DEAL_CONTACT_FIELDS)
        .sort({ updatedAt: -1, expectedCloseAt: 1, createdAt: -1 })
        .limit(6)
        .lean(),
      ContactDocument.find(
        buildScopedFilter(req, {
          contactId: contact._id,
          status: { $ne: 'deleted' }
        })
      )
        .sort({ createdAt: -1, updatedAt: -1 })
        .limit(6)
        .lean(),
      loadMergedContactActivity(req, contact._id, 20),
      Conversation.findOne(buildScopedFilter(req, { contactId: contact._id }))
        .sort({ lastMessageTime: -1, updatedAt: -1 })
        .select(CRM_CONVERSATION_FIELDS)
        .lean(),
      WhatsAppConsentLog.find(buildScopedFilter(req, { contactId: contact._id }))
        .sort({ createdAt: -1 })
        .limit(10)
        .lean(),
      getLeadScoringSettings({
        userId: req.user?.id,
        companyId: req.companyId || null
      }).catch(() => null)
    ]);

    const recentMessagesRaw = recentConversation?._id
      ? await Message.find(
          buildScopedFilter(req, { conversationId: recentConversation._id })
        )
          .sort({ timestamp: -1 })
          .limit(10)
          .select(CRM_MESSAGE_FIELDS)
          .lean()
      : [];

    const recentMessages = await attachBroadcastContextToMessages(req, recentMessagesRaw);

    const timeline = buildUnifiedTimeline({
      activities: recentActivities,
      messages: recentMessages
    });
    const recentMeetings = (recentActivities || []).filter(
      (activity) => toCleanString(activity?.type).toLowerCase() === 'meeting_scheduled'
    );
    const recentOwnerNotifications = (recentActivities || []).filter(
      (activity) => toCleanString(activity?.type).toLowerCase() === 'owner_notified'
    );

    res.json({
      success: true,
      data: {
        ...contact.toObject(),
        openTasksCount: taskSummary.open,
        lastActivity: recentActivities[0] || null,
        taskSummary,
        dealSummary,
        sourceAttribution: buildContactSourceAttribution(contact),
        consentAudit: buildConsentAudit(contact, recentConsentLogs),
        leadScoring: buildLeadScoringInsight(contact, leadScoringSettings),
        recentTasks,
        recentDeals,
        recentDocuments,
        recentActivities,
        recentMeetings,
        recentOwnerNotifications,
        recentConsentLogs,
        recentConversation,
        recentMessages,
        timeline
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/contacts/:id/stage', async (req, res) => {
  try {
    const { id } = req.params;
    const stage = toCleanString(req.body?.stage).toLowerCase();
    const stageMap = await loadPipelineStageMap(req);

    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }
    if (!stageMap.keySet.has(stage)) {
      return res.status(400).json({ success: false, error: 'Invalid stage' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousStage = contact.stage || '';
    const previousStatus = contact.status || '';
    contact.stage = stage;
    contact.lastStageChangedAt = new Date();

    const nextStatus = getPipelineStageStatus(stage);
    if (nextStatus) {
      contact.status = nextStatus;
    }

    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'stage_changed',
      meta: {
        previousStage,
        nextStage: contact.stage,
        previousStatus,
        nextStatus: contact.status
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'stage_updated',
      contactId: String(contact._id || '').trim(),
      contact: toCrmContactRealtimePayload(contact)
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/contacts/:id/profile', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousProfile = {
      name: contact.name,
      email: contact.email,
      source: contact.source,
      temperature: contact.temperature,
      dealValue: contact.dealValue,
      lostReason: contact.lostReason,
      nextFollowUpAt: contact.nextFollowUpAt,
      notes: contact.notes
    };

    if (req.body?.name !== undefined) {
      contact.name = toCleanString(req.body.name);
    }

    if (req.body?.email !== undefined) {
      contact.email = toCleanString(req.body.email);
    }

    if (req.body?.source !== undefined) {
      contact.source = toCleanString(req.body.source);
    }

    if (req.body?.temperature !== undefined) {
      const normalizedTemperature = toCleanString(req.body.temperature).toLowerCase();
      if (!LEAD_TEMPERATURES.includes(normalizedTemperature)) {
        return res.status(400).json({ success: false, error: 'Invalid temperature' });
      }
      contact.temperature = normalizedTemperature;
    }

    if (req.body?.dealValue !== undefined) {
      if (req.body.dealValue === null || req.body.dealValue === '') {
        contact.dealValue = 0;
      } else {
        const normalizedDealValue = toFiniteNumber(req.body.dealValue);
        if (normalizedDealValue === null || normalizedDealValue < 0) {
          return res.status(400).json({ success: false, error: 'dealValue must be a valid non-negative number' });
        }
        contact.dealValue = normalizedDealValue;
      }
    }

    if (req.body?.lostReason !== undefined) {
      contact.lostReason = toCleanString(req.body.lostReason);
    }

    if (req.body?.notes !== undefined) {
      contact.notes = toCleanString(req.body.notes);
    }

    if (req.body?.nextFollowUpAt !== undefined) {
      if (!req.body.nextFollowUpAt) {
        contact.nextFollowUpAt = null;
      } else {
        const parsedNextFollowUpAt = safeDate(req.body.nextFollowUpAt);
        if (!parsedNextFollowUpAt) {
          return res.status(400).json({ success: false, error: 'Invalid nextFollowUpAt date' });
        }
        contact.nextFollowUpAt = parsedNextFollowUpAt;
      }
    }

    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'contact_profile_updated',
      meta: {
        previousProfile,
        nextProfile: {
          name: contact.name,
          email: contact.email,
          source: contact.source,
          temperature: contact.temperature,
          dealValue: contact.dealValue,
          lostReason: contact.lostReason,
          nextFollowUpAt: contact.nextFollowUpAt,
          notes: contact.notes
        }
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'profile_updated',
      contactId: String(contact._id || '').trim(),
      contact: toCrmContactRealtimePayload(contact)
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/contacts/:id/owner', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousOwner = contact.ownerId || null;
    const nextOwner = toCleanString(req.body?.ownerId) || null;
    contact.ownerId = nextOwner;
    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'owner_changed',
      meta: {
        previousOwner,
        nextOwner
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'owner_updated',
      contactId: String(contact._id || '').trim(),
      contact: toCrmContactRealtimePayload(contact)
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/contacts/:id/notes', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    if (req.body?.note === undefined) {
      return res.status(400).json({ success: false, error: 'note is required' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousNote = String(contact.notes || '').trim();
    const nextNote = toCleanString(req.body.note);
    contact.notes = nextNote;

    const followUpDate = safeDate(req.body?.nextFollowUpAt);
    if (req.body?.nextFollowUpAt !== undefined) {
      contact.nextFollowUpAt = followUpDate;
    }

    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'note_updated',
      meta: {
        previousNote,
        nextNote,
        nextFollowUpAt: contact.nextFollowUpAt
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'notes_updated',
      contactId: String(contact._id || '').trim(),
      contact: toCrmContactRealtimePayload(contact)
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/contacts/:id/documents', async (req, res) => {
  try {
    const contact = await loadAuthorizedContact(req, req.params.id);
    const documents = await ContactDocument.find(
      buildScopedFilter(req, {
        contactId: contact._id,
        status: { $ne: 'deleted' }
      })
    ).sort({ createdAt: -1, updatedAt: -1 });

    res.json({
      success: true,
      data: documents,
      meta: {
        documentTypes: CONTACT_DOCUMENT_TYPES,
        verificationStatuses: CONTACT_DOCUMENT_VERIFICATION_STATUSES
      }
    });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.post('/contacts/:id/documents', async (req, res) => {
  try {
    await runSingleDocumentUpload(req, res);
    const contact = await resolveAuthorizedContactForDocumentWrite(
      req,
      req.params.id,
      req.body?.conversationId
    );

    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Document file is required' });
    }

    const documentType = toCleanString(req.body?.documentType).toLowerCase() || 'other';
    if (!CONTACT_DOCUMENT_TYPES.includes(documentType)) {
      return res.status(400).json({ success: false, error: 'Invalid document type' });
    }

    const verificationStatus =
      toCleanString(req.body?.verificationStatus).toLowerCase() || 'not_required';
    if (!CONTACT_DOCUMENT_VERIFICATION_STATUSES.includes(verificationStatus)) {
      return res.status(400).json({ success: false, error: 'Invalid verification status' });
    }

    const conversationId = await resolveDocumentConversationId(req, contact, req.body?.conversationId);
    const attachment = await uploadContactDocumentAttachment({
      file: req.file,
      user: req.user,
      contact,
      companyContext: {
        companyId: req.companyId || req.user?.companyId || null,
        companyName: req.user?.companyName || '',
        companySlug: req.user?.companySlug || '',
        cloudinaryFolderRoot: req.user?.cloudinaryFolderRoot || ''
      },
      sender: String(req.user?.username || req.user?.email || req.user?.id || '').trim(),
      recipient: String(contact?.phone || '').trim()
    });

    const document = await ContactDocument.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId: contact._id,
      conversationId: conversationId || null,
      title: toCleanString(req.body?.title) || String(attachment?.originalFileName || '').trim(),
      description: toCleanString(req.body?.description),
      documentType,
      verificationStatus,
      tags: toCleanStringArray(req.body?.tags),
      attachment,
      metadata: {
        source: 'crm_contact_panel',
        uploadedVia: 'team_inbox'
      },
      createdBy: req.user.id || null
    });

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId,
      type: 'document_uploaded',
      meta: {
        documentId: String(document._id),
        title: document.title,
        documentType: document.documentType,
        fileName: attachment?.originalFileName || '',
        verificationStatus: document.verificationStatus
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'document_uploaded',
      contactId: String(contact._id || '').trim(),
      conversationId: String(conversationId || '').trim(),
      contact: toCrmContactRealtimePayload(contact)
    });

    res.status(201).json({ success: true, data: document });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.get('/deals', async (req, res) => {
  try {
    const { page = 1, limit = 50, cursor = '' } = req.query;
    const useCursor = Boolean(toCleanString(cursor)) || toCleanString(req.query?.cursorMode).toLowerCase() === 'true';
    const pageNumber = Math.max(Number(page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(limit) || 50, 1), 100);
    const sortOrder = buildDealListSort();
    const listFilter = await buildDealListFilter(req, req.query);
    const totalFilter = listFilter;
    const decodedCursor = decodeCursor(cursor);
    let scopedFilter = listFilter;
    if (useCursor && decodedCursor?.updatedAt && decodedCursor?._id) {
      const cursorFilter = buildDealCursorFilter(cursor);
      if (cursorFilter) {
        scopedFilter = mergeFiltersWithAnd(scopedFilter, cursorFilter);
      }
    }

    const [deals, total] = await Promise.all([
      Deal.find(scopedFilter)
        .select(CRM_DEAL_LIST_FIELDS)
        .populate('contactId', CRM_DEAL_CONTACT_FIELDS)
        .sort(sortOrder)
        .skip(useCursor ? 0 : (pageNumber - 1) * pageSize)
        .limit(useCursor ? pageSize + 1 : pageSize)
        .lean(),
      useCursor ? Promise.resolve(0) : Deal.countDocuments(totalFilter)
    ]);

    const pageDeals = useCursor ? deals.slice(0, pageSize) : deals;
    const lastDeal = pageDeals[pageDeals.length - 1] || null;
    const nextCursor =
      useCursor && deals.length > pageSize && lastDeal
        ? encodeCursor({
            updatedAt: lastDeal.updatedAt || new Date(0).toISOString(),
            _id: String(lastDeal._id || '')
          })
        : '';

    res.json({
      success: true,
      data: pageDeals,
      nextCursor,
      hasMore: useCursor ? deals.length > pageSize : pageNumber < Math.max(Math.ceil(total / pageSize), 1),
      pagination: useCursor
        ? {
            limit: pageSize,
            nextCursor,
            hasMore: deals.length > pageSize,
            total: 0,
            totalPages: 0
          }
        : {
            page: pageNumber,
            limit: pageSize,
            total,
            totalPages: Math.max(Math.ceil(total / pageSize), 1)
          }
    });
  } catch (error) {
    res.status(error?.status || 500).json({ success: false, error: error.message });
  }
});

router.get('/deals/metrics', async (req, res) => {
  try {
    const metricsFilter = await buildDealListFilter(req, req.query);
    const dealSummary = await buildDealSummary(req, metricsFilter);
    res.json({ success: true, data: dealSummary });
  } catch (error) {
    res.status(error?.status || 500).json({ success: false, error: error.message });
  }
});

router.post('/deals', async (req, res) => {
  try {
    const {
      contactId,
      conversationId,
      title,
      stage = 'discovery',
      status = 'open',
      value = 0,
      probability = 0,
      currency = 'INR',
      expectedCloseAt = null,
      ownerId = null,
      productName = '',
      source = '',
      notes = '',
      lostReason = ''
    } = req.body || {};

    if (!toObjectIdIfValid(contactId)) {
      return res.status(400).json({ success: false, error: 'Valid contactId is required' });
    }

    const normalizedTitle = toCleanString(title);
    if (!normalizedTitle) {
      return res.status(400).json({ success: false, error: 'Deal title is required' });
    }

    const normalizedValue = toFiniteNumber(value);
    if (normalizedValue === null || normalizedValue < 0) {
      return res.status(400).json({ success: false, error: 'value must be a valid non-negative number' });
    }

    const normalizedProbability = toFiniteNumber(probability);
    if (normalizedProbability === null || normalizedProbability < 0 || normalizedProbability > 100) {
      return res.status(400).json({ success: false, error: 'probability must be between 0 and 100' });
    }

    const parsedExpectedCloseAt = safeDate(expectedCloseAt);
    if (expectedCloseAt !== null && expectedCloseAt !== undefined && !parsedExpectedCloseAt) {
      return res.status(400).json({ success: false, error: 'Invalid expectedCloseAt date' });
    }

    const normalizedLifecycle = normalizeDealLifecycle({
      nextStage: stage,
      nextStatus: status
    });

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: contactId })).select(
      '_id ownerId source'
    );
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const finalConversationId = toObjectIdIfValid(conversationId)
      ? conversationId
      : await findRecentConversationIdForContact(req, contact._id);

    const deal = await Deal.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId: contact._id,
      conversationId: finalConversationId,
      title: normalizedTitle,
      stage: normalizedLifecycle.stage,
      status: normalizedLifecycle.status,
      value: normalizedValue,
      probability: normalizedProbability,
      currency: toCleanString(currency) || 'INR',
      expectedCloseAt: parsedExpectedCloseAt,
      ownerId: toCleanString(ownerId) || contact.ownerId || null,
      productName: toCleanString(productName),
      source: toCleanString(source) || toCleanString(contact.source),
      notes: toCleanString(notes),
      lostReason: toCleanString(lostReason),
      wonAt: normalizedLifecycle.wonAt,
      lostAt: normalizedLifecycle.lostAt,
      createdBy: req.user.id || null,
      updatedBy: req.user.id || null
    });

    const syncedContact = await syncContactDealSnapshot(req, deal.contactId);

    await logLeadActivity({
      req,
      contactId: deal.contactId,
      conversationId: deal.conversationId,
      type: 'deal_created',
      meta: {
        dealId: String(deal._id),
        title: deal.title,
        stage: deal.stage,
        status: deal.status,
        value: deal.value,
        probability: deal.probability
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'deal',
      action: 'created',
      contactId: String(deal.contactId || '').trim(),
      dealId: String(deal._id || '').trim(),
      contact: syncedContact ? toCrmContactRealtimePayload(syncedContact) : null
    });

    res.status(201).json({ success: true, data: deal });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/deals/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid deal id' });
    }

    const deal = await Deal.findOne(buildScopedFilter(req, { _id: id }));
    if (!deal) {
      return res.status(404).json({ success: false, error: 'Deal not found' });
    }

    const previousDeal = {
      title: deal.title,
      stage: deal.stage,
      status: deal.status,
      value: deal.value,
      probability: deal.probability,
      expectedCloseAt: deal.expectedCloseAt,
      ownerId: deal.ownerId,
      productName: deal.productName,
      source: deal.source,
      notes: deal.notes,
      lostReason: deal.lostReason
    };

    if (req.body?.title !== undefined) {
      const nextTitle = toCleanString(req.body.title);
      if (!nextTitle) {
        return res.status(400).json({ success: false, error: 'Deal title cannot be empty' });
      }
      deal.title = nextTitle;
    }

    if (req.body?.value !== undefined) {
      const normalizedValue = toFiniteNumber(req.body.value);
      if (normalizedValue === null || normalizedValue < 0) {
        return res.status(400).json({ success: false, error: 'value must be a valid non-negative number' });
      }
      deal.value = normalizedValue;
    }

    if (req.body?.probability !== undefined) {
      const normalizedProbability = toFiniteNumber(req.body.probability);
      if (normalizedProbability === null || normalizedProbability < 0 || normalizedProbability > 100) {
        return res.status(400).json({ success: false, error: 'probability must be between 0 and 100' });
      }
      deal.probability = normalizedProbability;
    }

    if (req.body?.currency !== undefined) {
      deal.currency = toCleanString(req.body.currency) || 'INR';
    }

    if (req.body?.expectedCloseAt !== undefined) {
      if (!req.body.expectedCloseAt) {
        deal.expectedCloseAt = null;
      } else {
        const parsedExpectedCloseAt = safeDate(req.body.expectedCloseAt);
        if (!parsedExpectedCloseAt) {
          return res.status(400).json({ success: false, error: 'Invalid expectedCloseAt date' });
        }
        deal.expectedCloseAt = parsedExpectedCloseAt;
      }
    }

    if (req.body?.ownerId !== undefined) {
      deal.ownerId = toCleanString(req.body.ownerId) || null;
    }

    if (req.body?.productName !== undefined) {
      deal.productName = toCleanString(req.body.productName);
    }

    if (req.body?.source !== undefined) {
      deal.source = toCleanString(req.body.source);
    }

    if (req.body?.notes !== undefined) {
      deal.notes = toCleanString(req.body.notes);
    }

    if (req.body?.lostReason !== undefined) {
      deal.lostReason = toCleanString(req.body.lostReason);
    }

    const normalizedLifecycle = normalizeDealLifecycle({
      existingStage: deal.stage,
      existingStatus: deal.status,
      nextStage: req.body?.stage,
      nextStatus: req.body?.status,
      existingWonAt: deal.wonAt,
      existingLostAt: deal.lostAt
    });
    deal.stage = normalizedLifecycle.stage;
    deal.status = normalizedLifecycle.status;
    deal.wonAt = normalizedLifecycle.wonAt;
    deal.lostAt = normalizedLifecycle.lostAt;
    deal.updatedBy = req.user.id || null;

    await deal.save();
    const syncedContact = await syncContactDealSnapshot(req, deal.contactId);

    const activityType =
      previousDeal.status !== 'won' && deal.status === 'won'
        ? 'deal_won'
        : previousDeal.status !== 'lost' && deal.status === 'lost'
          ? 'deal_lost'
          : 'deal_updated';

    await logLeadActivity({
      req,
      contactId: deal.contactId,
      conversationId: deal.conversationId,
      type: activityType,
      meta: {
        dealId: String(deal._id),
        previousDeal,
        nextDeal: {
          title: deal.title,
          stage: deal.stage,
          status: deal.status,
          value: deal.value,
          probability: deal.probability,
          expectedCloseAt: deal.expectedCloseAt,
          ownerId: deal.ownerId,
          productName: deal.productName,
          source: deal.source,
          notes: deal.notes,
          lostReason: deal.lostReason
        }
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'deal',
      action: 'updated',
      contactId: String(deal.contactId || '').trim(),
      dealId: String(deal._id || '').trim(),
      contact: syncedContact ? toCrmContactRealtimePayload(syncedContact) : null
    });

    res.json({ success: true, data: deal });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.delete('/deals/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid deal id' });
    }

    const deal = await Deal.findOne(buildScopedFilter(req, { _id: id }));
    if (!deal) {
      return res.status(404).json({ success: false, error: 'Deal not found' });
    }

    await Deal.deleteOne(buildScopedFilter(req, { _id: deal._id }));
    const syncedContact = await syncContactDealSnapshot(req, deal.contactId);

    await logLeadActivity({
      req,
      contactId: deal.contactId,
      conversationId: deal.conversationId,
      type: 'deal_deleted',
      meta: {
        dealId: String(deal._id),
        title: deal.title,
        stage: deal.stage,
        status: deal.status,
        value: deal.value
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'deal',
      action: 'deleted',
      contactId: String(deal.contactId || '').trim(),
      dealId: String(deal._id || '').trim(),
      contact: syncedContact ? toCrmContactRealtimePayload(syncedContact) : null
    });

    res.json({
      success: true,
      data: {
        _id: deal._id,
        deleted: true
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/tasks', async (req, res) => {
  try {
    const {
      contactId,
      conversationId,
      title,
      description,
      taskType = 'follow_up',
      dueAt,
      reminderAt,
      priority = 'medium',
      status = 'pending',
      assignedTo = null,
      recurrence = null,
      comment = ''
    } = req.body || {};

    if (!toObjectIdIfValid(contactId)) {
      return res.status(400).json({ success: false, error: 'Valid contactId is required' });
    }

    const normalizedTitle = toCleanString(title);
    if (!normalizedTitle) {
      return res.status(400).json({ success: false, error: 'Task title is required' });
    }

    const normalizedPriority = toCleanString(priority).toLowerCase() || 'medium';
    const normalizedStatus = toCleanString(status).toLowerCase() || 'pending';
    const normalizedTaskType = toCleanString(taskType).toLowerCase() || 'follow_up';

    if (!TASK_PRIORITIES.includes(normalizedPriority)) {
      return res.status(400).json({ success: false, error: 'Invalid task priority' });
    }
    if (!TASK_STATUSES.includes(normalizedStatus)) {
      return res.status(400).json({ success: false, error: 'Invalid task status' });
    }
    if (!TASK_TYPES.includes(normalizedTaskType)) {
      return res.status(400).json({ success: false, error: 'Invalid task type' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: contactId })).select('_id');
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const parsedDueAt = safeDate(dueAt);
    const parsedReminderAt = safeDate(reminderAt);
    if (dueAt !== undefined && !parsedDueAt) {
      return res.status(400).json({ success: false, error: 'Invalid dueAt date' });
    }
    if (reminderAt !== undefined && !parsedReminderAt) {
      return res.status(400).json({ success: false, error: 'Invalid reminderAt date' });
    }

    const finalConversationId = toObjectIdIfValid(conversationId)
      ? conversationId
      : await findRecentConversationIdForContact(req, contact._id);
    const normalizedRecurrence = normalizeTaskRecurrence(recurrence);
    const initialComment = toCleanString(comment);

    const task = await LeadTask.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId: contact._id,
      conversationId: finalConversationId,
      title: normalizedTitle,
      description: toCleanString(description),
      taskType: normalizedTaskType,
      dueAt: parsedDueAt,
      reminderAt: parsedReminderAt,
      priority: normalizedPriority,
      status: normalizedStatus,
      assignedTo: toCleanString(assignedTo) || null,
      createdBy: req.user.id || null,
      recurrence: normalizedRecurrence,
      comments: initialComment
        ? [
            {
              text: initialComment,
              createdBy: req.user.id || null,
              createdAt: new Date()
            }
          ]
        : [],
      completedAt: normalizedStatus === 'completed' ? new Date() : null,
      completedBy: normalizedStatus === 'completed' ? req.user.id || null : null
    });

    await logLeadActivity({
      req,
      contactId: task.contactId,
      conversationId: task.conversationId,
      type: 'task_created',
      meta: {
        taskId: String(task._id),
        title: task.title,
        taskType: task.taskType,
        dueAt: task.dueAt,
        reminderAt: task.reminderAt,
        priority: task.priority,
        status: task.status,
        assignedTo: task.assignedTo,
        recurrence: task.recurrence,
        commentCount: Array.isArray(task.comments) ? task.comments.length : 0
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'task',
      action: 'created',
      contactId: String(task.contactId || '').trim(),
      taskId: String(task._id || '').trim()
    });

    res.status(201).json({ success: true, data: task });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/tasks', async (req, res) => {
  try {
    const {
      status,
      priority,
      taskType,
      assignedTo,
      contactId,
      bucket,
      dueFrom,
      dueTo,
      search,
      cursor = '',
      page = 1,
      limit = 50
    } = req.query;

    const extraFilter = {};
    const statusList = String(status || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);
    const priorityList = String(priority || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);
    const taskTypeList = String(taskType || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);

    if (statusList.length > 0) {
      const invalidStatus = statusList.find((item) => !TASK_STATUSES.includes(item));
      if (invalidStatus) {
        return res.status(400).json({ success: false, error: `Invalid task status: ${invalidStatus}` });
      }
      extraFilter.status = { $in: statusList };
    }

    if (priorityList.length > 0) {
      const invalidPriority = priorityList.find((item) => !TASK_PRIORITIES.includes(item));
      if (invalidPriority) {
        return res.status(400).json({ success: false, error: `Invalid task priority: ${invalidPriority}` });
      }
      extraFilter.priority = { $in: priorityList };
    }

    if (taskTypeList.length > 0) {
      const invalidTaskType = taskTypeList.find((item) => !TASK_TYPES.includes(item));
      if (invalidTaskType) {
        return res.status(400).json({ success: false, error: `Invalid task type: ${invalidTaskType}` });
      }
      extraFilter.taskType = { $in: taskTypeList };
    }

    const normalizedAssignedTo = toCleanString(assignedTo);
    if (normalizedAssignedTo) extraFilter.assignedTo = normalizedAssignedTo;

    const normalizedBucket = toCleanString(bucket).toLowerCase();
    const bucketFilter = buildTaskBucketFilter(bucket);
    if (normalizedBucket && normalizedBucket !== 'all' && !bucketFilter) {
      return res.status(400).json({ success: false, error: 'Invalid task bucket' });
    }

    const normalizedContactId = toCleanString(contactId);
    if (normalizedContactId) {
      if (!toObjectIdIfValid(normalizedContactId)) {
        return res.status(400).json({ success: false, error: 'Invalid contactId' });
      }
      extraFilter.contactId = normalizedContactId;
    }

    const parsedDueFrom = safeDate(dueFrom);
    const parsedDueTo = safeDate(dueTo);
    if (dueFrom && !parsedDueFrom) {
      return res.status(400).json({ success: false, error: 'Invalid dueFrom date' });
    }
    if (dueTo && !parsedDueTo) {
      return res.status(400).json({ success: false, error: 'Invalid dueTo date' });
    }
    if (parsedDueFrom || parsedDueTo) {
      extraFilter.dueAt = {};
      if (parsedDueFrom) extraFilter.dueAt.$gte = parsedDueFrom;
      if (parsedDueTo) extraFilter.dueAt.$lte = parsedDueTo;
    }

    const pageNumber = Math.max(Number(page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const useCursor = Boolean(toCleanString(cursor)) || toCleanString(req.query?.cursorMode).toLowerCase() === 'true';
    const searchFilter = await buildTaskSearchFilter(req, search);
    const cursorFilter = useCursor ? buildTaskCursorFilter(cursor) : null;
    const totalFilter = buildScopedFilter(
      req,
      mergeFiltersWithAnd(extraFilter, bucketFilter || {}, searchFilter || {})
    );
    const scopedFilter = buildScopedFilter(
      req,
      mergeFiltersWithAnd(extraFilter, bucketFilter || {}, searchFilter || {}, cursorFilter || {})
    );

    const [tasks, total] = await Promise.all([
      LeadTask.find(scopedFilter)
        .populate('contactId', CRM_TASK_CONTACT_FIELDS)
        .sort({ dueAt: 1, priority: -1, createdAt: -1, _id: -1 })
        .skip(useCursor ? 0 : (pageNumber - 1) * pageSize)
        .limit(useCursor ? pageSize + 1 : pageSize)
        .lean(),
      LeadTask.countDocuments(totalFilter)
    ]);

    const pageTasks = useCursor ? tasks.slice(0, pageSize) : tasks;
    const lastTask = pageTasks[pageTasks.length - 1] || null;
    const nextCursor =
      useCursor && tasks.length > pageSize && lastTask
        ? encodeCursor({
            dueAt: lastTask.dueAt ? new Date(lastTask.dueAt).toISOString() : null,
            priorityRank: getTaskPriorityRank(lastTask.priority),
            createdAt: lastTask.createdAt ? new Date(lastTask.createdAt).toISOString() : null,
            _id: String(lastTask._id || '')
          })
        : '';

    res.json({
      success: true,
      data: pageTasks,
      nextCursor,
      hasMore: useCursor ? tasks.length > pageSize : pageNumber < Math.max(Math.ceil(total / pageSize), 1),
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total,
        totalPages: Math.max(Math.ceil(total / pageSize), 1)
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/tasks/summary', async (req, res) => {
  try {
    const extraFilter = {};
    const normalizedAssignedTo = toCleanString(req.query?.assignedTo);
    const normalizedContactId = toCleanString(req.query?.contactId);
    const normalizedStatusList = String(req.query?.status || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);
    const normalizedPriorityList = String(req.query?.priority || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);
    const normalizedTaskTypeList = String(req.query?.taskType || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);
    const normalizedBucket = toCleanString(req.query?.bucket).toLowerCase();
    const parsedDueFrom = safeDate(req.query?.dueFrom);
    const parsedDueTo = safeDate(req.query?.dueTo);
    const searchFilter = await buildTaskSearchFilter(req, req.query?.search);

    if (normalizedStatusList.length > 0) {
      const invalidStatus = normalizedStatusList.find((item) => !TASK_STATUSES.includes(item));
      if (invalidStatus) {
        return res.status(400).json({ success: false, error: `Invalid task status: ${invalidStatus}` });
      }
      extraFilter.status = { $in: normalizedStatusList };
    }

    if (normalizedPriorityList.length > 0) {
      const invalidPriority = normalizedPriorityList.find((item) => !TASK_PRIORITIES.includes(item));
      if (invalidPriority) {
        return res.status(400).json({ success: false, error: `Invalid task priority: ${invalidPriority}` });
      }
      extraFilter.priority = { $in: normalizedPriorityList };
    }

    if (normalizedTaskTypeList.length > 0) {
      const invalidTaskType = normalizedTaskTypeList.find((item) => !TASK_TYPES.includes(item));
      if (invalidTaskType) {
        return res.status(400).json({ success: false, error: `Invalid task type: ${invalidTaskType}` });
      }
      extraFilter.taskType = { $in: normalizedTaskTypeList };
    }

    if (normalizedBucket) {
      const bucketFilter = buildTaskBucketFilter(normalizedBucket);
      if (!bucketFilter && normalizedBucket !== 'all') {
        return res.status(400).json({ success: false, error: 'Invalid task bucket' });
      }
      if (bucketFilter) {
        Object.assign(extraFilter, bucketFilter);
      }
    }

    if (parsedDueFrom || parsedDueTo) {
      extraFilter.dueAt = {};
      if (parsedDueFrom) extraFilter.dueAt.$gte = parsedDueFrom;
      if (parsedDueTo) extraFilter.dueAt.$lte = parsedDueTo;
    }

    if (normalizedAssignedTo) {
      extraFilter.assignedTo = normalizedAssignedTo;
    }

    if (normalizedContactId) {
      if (!toObjectIdIfValid(normalizedContactId)) {
        return res.status(400).json({ success: false, error: 'Invalid contactId' });
      }
      extraFilter.contactId = normalizedContactId;
    }

    const summary = await buildTaskSummary(req, mergeFiltersWithAnd(extraFilter, searchFilter || {}));

    res.json({ success: true, data: summary });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/tasks/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid task id' });
    }

    const task = await LeadTask.findOne(buildScopedFilter(req, { _id: id }));
    if (!task) {
      return res.status(404).json({ success: false, error: 'Task not found' });
    }

    const previousTask = {
      title: task.title,
      description: task.description,
      taskType: task.taskType,
      dueAt: task.dueAt,
      reminderAt: task.reminderAt,
      priority: task.priority,
      status: task.status,
      assignedTo: task.assignedTo,
      completedBy: task.completedBy,
      recurrence: task.recurrence
    };

    if (req.body?.title !== undefined) {
      const nextTitle = toCleanString(req.body.title);
      if (!nextTitle) {
        return res.status(400).json({ success: false, error: 'Task title cannot be empty' });
      }
      task.title = nextTitle;
    }

    if (req.body?.description !== undefined) {
      task.description = toCleanString(req.body.description);
    }

    if (req.body?.taskType !== undefined) {
      const nextTaskType = toCleanString(req.body.taskType).toLowerCase();
      if (!TASK_TYPES.includes(nextTaskType)) {
        return res.status(400).json({ success: false, error: 'Invalid task type' });
      }
      task.taskType = nextTaskType;
    }

    if (req.body?.dueAt !== undefined) {
      if (!req.body.dueAt) {
        task.dueAt = null;
      } else {
        const parsedDueAt = safeDate(req.body.dueAt);
        if (!parsedDueAt) {
          return res.status(400).json({ success: false, error: 'Invalid dueAt date' });
        }
        task.dueAt = parsedDueAt;
      }
    }

    if (req.body?.reminderAt !== undefined) {
      if (!req.body.reminderAt) {
        task.reminderAt = null;
      } else {
        const parsedReminderAt = safeDate(req.body.reminderAt);
        if (!parsedReminderAt) {
          return res.status(400).json({ success: false, error: 'Invalid reminderAt date' });
        }
        task.reminderAt = parsedReminderAt;
      }
    }

    if (req.body?.priority !== undefined) {
      const nextPriority = toCleanString(req.body.priority).toLowerCase();
      if (!TASK_PRIORITIES.includes(nextPriority)) {
        return res.status(400).json({ success: false, error: 'Invalid task priority' });
      }
      task.priority = nextPriority;
    }

    if (req.body?.status !== undefined) {
      const nextStatus = toCleanString(req.body.status).toLowerCase();
      if (!TASK_STATUSES.includes(nextStatus)) {
        return res.status(400).json({ success: false, error: 'Invalid task status' });
      }
      task.status = nextStatus;
      task.completedAt = nextStatus === 'completed' ? new Date() : null;
      task.completedBy = nextStatus === 'completed' ? req.user.id || null : null;
    }

    if (req.body?.assignedTo !== undefined) {
      task.assignedTo = toCleanString(req.body.assignedTo) || null;
    }

    if (req.body?.recurrence !== undefined) {
      task.recurrence = normalizeTaskRecurrence(req.body.recurrence);
    }

    await task.save();

    let nextRecurringTask = null;
    const normalizedRecurrence = normalizeTaskRecurrence(task.recurrence);
    const taskWasCompletedNow =
      previousTask.status !== 'completed' && String(task.status || '').toLowerCase() === 'completed';

    if (taskWasCompletedNow && normalizedRecurrence.frequency !== 'none') {
      const nextDueAt = buildNextRecurringDate(task.dueAt, normalizedRecurrence);
      const nextReminderAt = buildNextRecurringDate(task.reminderAt, normalizedRecurrence);

      if (nextDueAt || nextReminderAt) {
        nextRecurringTask = await LeadTask.create({
          userId: task.userId,
          companyId: task.companyId || null,
          contactId: task.contactId,
          conversationId: task.conversationId || null,
          title: task.title,
          description: task.description,
          taskType: task.taskType,
          dueAt: nextDueAt,
          reminderAt: nextReminderAt,
          priority: task.priority,
          status: 'pending',
          assignedTo: task.assignedTo,
          createdBy: req.user.id || null,
          recurrence: normalizedRecurrence,
          comments: []
        });

        await logLeadActivity({
          req,
          contactId: nextRecurringTask.contactId,
          conversationId: nextRecurringTask.conversationId,
          type: 'task_created',
          meta: {
            taskId: String(nextRecurringTask._id),
            title: nextRecurringTask.title,
            taskType: nextRecurringTask.taskType,
            dueAt: nextRecurringTask.dueAt,
            reminderAt: nextRecurringTask.reminderAt,
            priority: nextRecurringTask.priority,
            status: nextRecurringTask.status,
            assignedTo: nextRecurringTask.assignedTo,
            recurrence: nextRecurringTask.recurrence,
            source: 'recurring_task'
          }
        });
      }
    }

    await logLeadActivity({
      req,
      contactId: task.contactId,
      conversationId: task.conversationId,
      type: task.status === 'completed' ? 'task_completed' : 'task_updated',
      meta: {
        taskId: String(task._id),
        previousTask,
        nextTask: {
          title: task.title,
          description: task.description,
          taskType: task.taskType,
          dueAt: task.dueAt,
          reminderAt: task.reminderAt,
          priority: task.priority,
          status: task.status,
          assignedTo: task.assignedTo,
          completedBy: task.completedBy,
          recurrence: task.recurrence
        }
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'task',
      action: task.status === 'completed' ? 'completed' : 'updated',
      contactId: String(task.contactId || '').trim(),
      taskId: String(task._id || '').trim()
    });

    if (nextRecurringTask) {
      emitCrmRealtimeEvent(req, {
        entity: 'task',
        action: 'created',
        contactId: String(nextRecurringTask.contactId || '').trim(),
        taskId: String(nextRecurringTask._id || '').trim()
      });
    }

    res.json({
      success: true,
      data: {
        ...(typeof task.toObject === 'function' ? task.toObject() : task),
        nextRecurringTask:
          nextRecurringTask && typeof nextRecurringTask.toObject === 'function'
            ? nextRecurringTask.toObject()
            : nextRecurringTask
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/tasks/:id/comments', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid task id' });
    }

    const text = toCleanString(req.body?.text);
    if (!text) {
      return res.status(400).json({ success: false, error: 'Comment text is required' });
    }

    const task = await LeadTask.findOne(buildScopedFilter(req, { _id: id }));
    if (!task) {
      return res.status(404).json({ success: false, error: 'Task not found' });
    }

    const nextComment = {
      text,
      createdBy: req.user.id || null,
      createdAt: new Date()
    };
    task.comments = Array.isArray(task.comments) ? [...task.comments, nextComment] : [nextComment];
    await task.save();

    await logLeadActivity({
      req,
      contactId: task.contactId,
      conversationId: task.conversationId,
      type: 'task_comment_added',
      meta: {
        taskId: String(task._id),
        title: task.title,
        comment: text
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'task',
      action: 'comment_added',
      contactId: String(task.contactId || '').trim(),
      taskId: String(task._id || '').trim()
    });

    res.status(201).json({ success: true, data: task });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/tasks/bulk', async (req, res) => {
  try {
    const taskIds = Array.isArray(req.body?.taskIds) ? req.body.taskIds : [];
    const action = toCleanString(req.body?.action).toLowerCase();
    const validTaskIds = taskIds.filter((taskId) => toObjectIdIfValid(taskId));

    if (validTaskIds.length === 0) {
      return res.status(400).json({ success: false, error: 'At least one valid task id is required' });
    }

    if (!['complete', 'cancel', 'delete', 'reschedule', 'assign'].includes(action)) {
      return res.status(400).json({ success: false, error: 'Invalid bulk action' });
    }

    const tasks = await LeadTask.find(
      buildScopedFilter(req, { _id: { $in: validTaskIds } })
    );

    if (!tasks.length) {
      return res.status(404).json({ success: false, error: 'No tasks found for bulk action' });
    }

    const bulkDueAt = req.body?.dueAt !== undefined ? safeDate(req.body.dueAt) : null;
    const bulkReminderAt = req.body?.reminderAt !== undefined ? safeDate(req.body.reminderAt) : null;
    if (req.body?.dueAt !== undefined && req.body?.dueAt && !bulkDueAt) {
      return res.status(400).json({ success: false, error: 'Invalid dueAt date' });
    }
    if (req.body?.reminderAt !== undefined && req.body?.reminderAt && !bulkReminderAt) {
      return res.status(400).json({ success: false, error: 'Invalid reminderAt date' });
    }

    const updatedTaskIds = [];
    const deletedTaskIds = [];

    for (const task of tasks) {
      if (action === 'delete') {
        await LeadTask.deleteOne(buildScopedFilter(req, { _id: task._id }));
        deletedTaskIds.push(String(task._id));
        await logLeadActivity({
          req,
          contactId: task.contactId,
          conversationId: task.conversationId,
          type: 'task_deleted',
          meta: {
            taskId: String(task._id),
            title: task.title,
            source: 'bulk_action'
          }
        });
        continue;
      }

      if (action === 'complete') {
        task.status = 'completed';
        task.completedAt = new Date();
        task.completedBy = req.user.id || null;
      }
      if (action === 'cancel') {
        task.status = 'cancelled';
        task.completedAt = null;
        task.completedBy = null;
      }
      if (action === 'reschedule') {
        task.dueAt = req.body?.dueAt ? bulkDueAt : null;
        task.reminderAt = req.body?.reminderAt ? bulkReminderAt : null;
      }
      if (action === 'assign') {
        task.assignedTo = toCleanString(req.body?.assignedTo) || null;
      }

      await task.save();
      updatedTaskIds.push(String(task._id));

      await logLeadActivity({
        req,
        contactId: task.contactId,
        conversationId: task.conversationId,
        type: action === 'complete' ? 'task_completed' : 'task_updated',
        meta: {
          taskId: String(task._id),
          title: task.title,
          bulkAction: action,
          assignedTo: task.assignedTo,
          dueAt: task.dueAt,
          reminderAt: task.reminderAt,
          status: task.status
        }
      });
    }

    res.json({
      success: true,
      data: {
        action,
        updatedCount: updatedTaskIds.length,
        deletedCount: deletedTaskIds.length,
        updatedTaskIds,
        deletedTaskIds
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'task',
      action: 'bulk_updated',
      contactIds: Array.from(
        new Set(
          tasks
            .map((task) => String(task?.contactId || '').trim())
            .filter(Boolean)
        )
      ),
      taskIds: [...updatedTaskIds, ...deletedTaskIds]
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.delete('/tasks/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid task id' });
    }

    const task = await LeadTask.findOne(buildScopedFilter(req, { _id: id }));
    if (!task) {
      return res.status(404).json({ success: false, error: 'Task not found' });
    }

    await LeadTask.deleteOne(buildScopedFilter(req, { _id: task._id }));

    await logLeadActivity({
      req,
      contactId: task.contactId,
      conversationId: task.conversationId,
      type: 'task_deleted',
      meta: {
        taskId: String(task._id),
        title: task.title,
        taskType: task.taskType,
        dueAt: task.dueAt,
        priority: task.priority
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'task',
      action: 'deleted',
      contactId: String(task.contactId || '').trim(),
      taskId: String(task._id || '').trim()
    });

    res.json({
      success: true,
      data: {
        _id: task._id,
        deleted: true
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/activities/:contactId', async (req, res) => {
  try {
    const { contactId } = req.params;
    if (!toObjectIdIfValid(contactId)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const limit = Math.min(Math.max(Number(req.query.limit) || 100, 1), 300);
    const merged = await loadMergedContactActivity(req, contactId, limit);

    res.json({ success: true, data: merged });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/meetings', async (req, res) => {
  try {
    const limit = normalizeActivityListLimit(req.query?.limit, 60, 250);
    const bucket = toCleanString(req.query?.bucket).toLowerCase() || 'all';
    const search = toCleanString(req.query?.search).toLowerCase();
    const contactId = toObjectIdIfValid(req.query?.contactId);
    const now = new Date();

    const baseFilter = buildActivityScopeFilter(req, { type: 'meeting_scheduled' });
    const activities = await LeadActivity.find(baseFilter)
      .populate({
        path: 'contactId',
        select: 'name phone stage ownerId leadScore'
      })
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean();

    const filtered = (Array.isArray(activities) ? activities : [])
      .map(buildMeetingListItem)
      .filter((item) => {
        if (contactId && item?.contact?._id !== String(contactId)) return false;

        const startDate = item?.start?.dateTime || item?.start || null;
        const parsedStart = startDate ? new Date(startDate) : null;
        const hasValidStart = parsedStart && !Number.isNaN(parsedStart.getTime());

        if (bucket === 'upcoming' && hasValidStart && parsedStart < now) return false;
        if (bucket === 'past' && (!hasValidStart || parsedStart >= now)) return false;

        if (!search) return true;

        return [
          item.summary,
          item.contact?.name,
          item.contact?.phone,
          item.meetingUrl
        ].some((value) => String(value || '').toLowerCase().includes(search));
      });

    const upcomingCount = filtered.filter((item) => {
      const startDate = item?.start?.dateTime || item?.start || null;
      const parsedStart = startDate ? new Date(startDate) : null;
      return !parsedStart || Number.isNaN(parsedStart.getTime()) || parsedStart >= now;
    }).length;

    const pastCount = filtered.length - upcomingCount;

    res.json({
      success: true,
      data: filtered,
      meta: {
        total: filtered.length,
        upcoming: upcomingCount,
        past: pastCount
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/notifications/owner', async (req, res) => {
  try {
    const limit = normalizeActivityListLimit(req.query?.limit, 40, 150);
    const status = toCleanString(req.query?.status).toLowerCase() || 'unread';
    const normalizedOwnerId = toCleanString(req.user?.id);
    const metaFilter = { 'meta.ownerId': normalizedOwnerId };

    if (status === 'unread') {
      metaFilter.$or = [{ 'meta.readAt': null }, { 'meta.readAt': { $exists: false } }];
    }

    const baseFilter = buildActivityScopeFilter(req, {
      type: 'owner_notified',
      ...metaFilter
    });

    const activities = await LeadActivity.find(baseFilter)
      .populate({
        path: 'contactId',
        select: 'name phone stage ownerId leadScore'
      })
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean();

    const items = (Array.isArray(activities) ? activities : []).map(buildOwnerNotificationItem);
    const unreadCount = items.filter((item) => !item.isRead).length;

    res.json({
      success: true,
      data: items,
      meta: {
        total: items.length,
        unread: unreadCount
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/notifications/owner/:id/read', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid notification id' });
    }

    const normalizedOwnerId = toCleanString(req.user?.id);
    const activity = await LeadActivity.findOne(
      buildActivityScopeFilter(req, {
        _id: id,
        type: 'owner_notified',
        'meta.ownerId': normalizedOwnerId
      })
    );

    if (!activity) {
      return res.status(404).json({ success: false, error: 'Notification not found' });
    }

    activity.meta = {
      ...(activity.meta && typeof activity.meta === 'object' ? activity.meta : {}),
      readAt: new Date().toISOString(),
      readBy: normalizedOwnerId
    };
    await activity.save();

    const populatedActivity = await LeadActivity.findOne(
      buildActivityScopeFilter(req, { _id: activity._id, type: 'owner_notified' })
    )
      .populate({
        path: 'contactId',
        select: 'name phone stage ownerId leadScore'
      })
      .lean();

    res.json({
      success: true,
      data: buildOwnerNotificationItem(populatedActivity || activity)
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/documents/:id/access', async (req, res) => {
  try {
    const { id } = req.params;
    const mode = toCleanString(req.query?.mode).toLowerCase() || 'view';

    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid document id' });
    }
    if (!DOCUMENT_ACCESS_MODES.includes(mode)) {
      return res.status(400).json({ success: false, error: 'Invalid access mode' });
    }

    const document = await ContactDocument.findOne(
      buildScopedFilter(req, {
        _id: id,
        status: { $ne: 'deleted' }
      })
    ).lean();

    if (!document) {
      return res.status(404).json({ success: false, error: 'Document not found' });
    }

    const payload = buildDocumentAccessPayload({ document, mode });
    if (!payload.url) {
      return res.status(404).json({ success: false, error: 'Document access URL is unavailable' });
    }

    res.json({
      success: true,
      data: {
        ...payload,
        mode,
        documentId: String(document._id),
        documentType: document.documentType
      }
    });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.delete('/documents/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid document id' });
    }

    const document = await ContactDocument.findOne(buildScopedFilter(req, { _id: id }));
    if (!document || document.status === 'deleted') {
      return res.status(404).json({ success: false, error: 'Document not found' });
    }

    if (document.attachment?.publicId || document.attachment?.secureUrl) {
      await deleteInboxAttachment({ attachment: document.attachment || {} });
    }

    document.status = 'deleted';
    document.attachment = {
      ...(document.attachment || {}),
      deletedAt: new Date(),
      deletedBy: req.user.id || null
    };
    await document.save();

    await logLeadActivity({
      req,
      contactId: document.contactId,
      conversationId: document.conversationId,
      type: 'document_deleted',
      meta: {
        documentId: String(document._id),
        title: document.title,
        documentType: document.documentType,
        fileName: document.attachment?.originalFileName || ''
      }
    });

    emitCrmRealtimeEvent(req, {
      entity: 'contact',
      action: 'document_deleted',
      contactId: String(document.contactId || '').trim()
    });

    res.json({
      success: true,
      data: {
        _id: document._id,
        status: document.status
      }
    });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.get('/reports/summary', async (req, res) => {
  try {
    const report = await getCrmReportsSummary({
      userId: req.user?.id || null,
      companyId: req.companyId || null
    });

    res.json({ success: true, data: report });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/reports/funnel', async (req, res) => {
  try {
    const report = await getCrmFunnelReport({
      userId: req.user?.id || null,
      companyId: req.companyId || null,
      from: req.query?.from || null,
      to: req.query?.to || null
    });

    res.json({ success: true, data: report });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/reports/cohort', async (req, res) => {
  try {
    const report = await getCrmCohortReport({
      userId: req.user?.id || null,
      companyId: req.companyId || null,
      from: req.query?.from || null,
      to: req.query?.to || null,
      granularity: req.query?.granularity || 'month'
    });

    res.json({ success: true, data: report });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/reports/owner-performance', async (req, res) => {
  try {
    const report = await getCrmOwnerPerformanceReport({
      userId: req.user?.id || null,
      companyId: req.companyId || null
    });

    res.json({ success: true, data: report });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/metrics', async (req, res) => {
  try {
    const contactFilter = buildScopedFilter(req);
    const now = new Date();
    const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const endOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999);

    const taskScope = buildScopedFilter(req);
    const [contactMetrics, openTasksCount, overdueTasksCount, dueTodayCount, completedTasksCount, todayCallsCount] = await Promise.all([
      Contact.aggregate([
        { $match: contactFilter },
        {
          $project: {
            stage: { $toLower: { $ifNull: ['$stage', 'new'] } },
            status: { $toLower: { $ifNull: ['$status', 'nurturing'] } },
            leadScore: { $ifNull: ['$leadScore', 0] },
            whatsappOptInStatus: { $toLower: { $ifNull: ['$whatsappOptInStatus', 'unknown'] } },
            normalizedTags: {
              $map: {
                input: { $ifNull: ['$tags', []] },
                as: 'tag',
                in: {
                  $toLower: {
                    $trim: {
                      input: { $toString: '$$tag' }
                    }
                  }
                }
              }
            }
          }
        },
        {
          $project: {
            stage: 1,
            status: 1,
            leadScore: 1,
            whatsappOptInStatus: 1,
            isQualified: {
              $or: [
                { $eq: ['$status', 'qualified'] },
                { $eq: ['$stage', 'qualified'] },
                { $in: ['qualified', '$normalizedTags'] }
              ]
            }
          }
        },
        {
          $facet: {
            summary: [
              {
                $group: {
                  _id: null,
                  total: { $sum: 1 },
                  qualified: {
                    $sum: {
                      $cond: ['$isQualified', 1, 0]
                    }
                  },
                  optedIn: {
                    $sum: {
                      $cond: [{ $eq: ['$whatsappOptInStatus', 'opted_in'] }, 1, 0]
                    }
                  },
                  averageLeadScore: { $avg: '$leadScore' }
                }
              }
            ],
            byStage: [
              {
                $group: {
                  _id: '$stage',
                  count: { $sum: 1 }
                }
              }
            ],
            byStatus: [
              {
                $group: {
                  _id: '$status',
                  count: { $sum: 1 }
                }
              }
            ]
          }
        }
      ]),
      LeadTask.countDocuments({
        $and: [taskScope, { status: { $in: ['pending', 'in_progress'] } }]
      }),
      LeadTask.countDocuments({
        $and: [
          taskScope,
          {
            status: { $in: ['pending', 'in_progress'] },
            dueAt: { $ne: null, $lt: now }
          }
        ]
      }),
      LeadTask.countDocuments({
        $and: [taskScope, { status: { $in: ['pending', 'in_progress'] }, dueAt: { $gte: startOfDay, $lte: endOfDay } }]
      }),
      LeadTask.countDocuments({
        $and: [taskScope, { status: 'completed' }]
      }),
      LeadTask.countDocuments({
        $and: [
          taskScope,
          {
            status: { $in: ['pending', 'in_progress'] },
            taskType: 'call',
            dueAt: { $gte: startOfDay, $lte: endOfDay }
          }
        ]
      })
    ]);

    const contactMetricsData = Array.isArray(contactMetrics) ? contactMetrics[0] || {} : {};
    const contactSummary = contactMetricsData.summary?.[0] || {};
    const byStage = (contactMetricsData.byStage || []).reduce((acc, item) => {
      const key = toCleanString(item?._id).toLowerCase() || 'new';
      acc[key] = Number(item?.count || 0);
      return acc;
    }, {});
    const byStatus = (contactMetricsData.byStatus || []).reduce((acc, item) => {
      const key = toCleanString(item?._id).toLowerCase() || 'nurturing';
      acc[key] = Number(item?.count || 0);
      return acc;
    }, {});
    const totalContacts = Number(contactSummary.total || 0);

    res.json({
      success: true,
      data: {
        contacts: {
          total: totalContacts,
          qualified: Number(contactSummary.qualified || 0),
          optedIn: Number(contactSummary.optedIn || 0),
          byStage,
          byStatus,
          averageLeadScore: totalContacts > 0 ? Number(Number(contactSummary.averageLeadScore || 0).toFixed(2)) : 0
        },
        tasks: {
          open: openTasksCount,
          overdue: overdueTasksCount,
          dueToday: dueTodayCount,
          completed: completedTasksCount,
          todayCalls: todayCallsCount,
          completionRate:
            openTasksCount + completedTasksCount > 0
              ? Number(((completedTasksCount / (openTasksCount + completedTasksCount)) * 100).toFixed(1))
              : 0
        },
        generatedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/ops/owner-dashboard', async (req, res) => {
  try {
    const dashboard = await getCrmOwnerDashboard({
      userId: req.user?.id || null,
      companyId: req.companyId || null
    });

    res.json({ success: true, data: dashboard });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/ops/follow-up-automation', async (req, res) => {
  try {
    const dryRun =
      req.body?.dryRun === undefined ? true : String(req.body.dryRun).trim().toLowerCase() !== 'false';
    const limit = Math.min(Math.max(Number(req.body?.limit) || 60, 1), 300);

    const result = await runCrmFollowUpAutomation({
      userId: req.user?.id || null,
      companyId: req.companyId || null,
      dryRun,
      limit,
      automationActor: req.user?.id || 'system:manual'
    });

    emitCrmRealtimeEvent(req, {
      entity: 'automation',
      action: dryRun ? 'preview_completed' : 'executed'
    });

    res.json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/ops/history', async (req, res) => {
  try {
    const limit = Math.min(Math.max(Number(req.query?.limit) || 20, 1), 100);
    const normalizedStatus = toCleanString(req.query?.status).toLowerCase();
    const extraFilter = {};
    if (normalizedStatus) {
      if (!['success', 'error'].includes(normalizedStatus)) {
        return res.status(400).json({ success: false, error: 'Invalid automation history status' });
      }
      extraFilter.status = normalizedStatus;
    }
    const runs = await CrmAutomationRun.find(
      buildScopedFilter(req, extraFilter)
    )
      .sort({ generatedAt: -1, createdAt: -1 })
      .limit(limit)
      .lean();

    res.json({
      success: true,
      data: Array.isArray(runs) ? runs : []
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
