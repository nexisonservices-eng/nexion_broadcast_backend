const mongoose = require('mongoose');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Deal = require('../models/Deal');
const LeadActivity = require('../models/LeadActivity');
const LeadTask = require('../models/LeadTask');
const User = require('../models/User');
const CrmAutomationRun = require('../models/CrmAutomationRun');
const { getLeadScoringSettings } = require('./leadScoringService');
const { isSmtpConfigured, sendAppEmail } = require('./emailService');

const OPEN_TASK_STATUSES = ['pending', 'in_progress'];
const LEAD_STAGE_ORDER = ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'won', 'lost'];
const DEFAULT_REPLY_SLA_HOURS = Number(process.env.CRM_REPLY_SLA_HOURS || 4);
const DEFAULT_AUTOMATION_LIMIT = Math.max(Number(process.env.CRM_AUTOMATION_BATCH_LIMIT || 60) || 60, 1);
const CRM_AUTOMATION_RULES = {
  OVERDUE_FOLLOW_UP: 'overdue_follow_up',
  REPLY_SLA_BREACH: 'reply_sla_breach',
  DEAL_CLOSE_RISK: 'deal_close_risk',
  OPT_IN_TO_NURTURING: 'opt_in_to_nurturing',
  LEAD_SCORE_STAGE_ADVANCE: 'lead_score_stage_advance',
  LEAD_SCORE_THRESHOLD: 'lead_score_threshold'
};

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) =>
  mongoose.Types.ObjectId.isValid(value) ? new mongoose.Types.ObjectId(value) : null;
const getLeadStageRank = (stage) => LEAD_STAGE_ORDER.indexOf(toCleanString(stage).toLowerCase());

const buildScopedFilter = ({ userId = null, companyId = null } = {}, extra = {}) => {
  const conditions = [];

  const normalizedUserId = toObjectIdIfValid(userId);
  const normalizedCompanyId = toObjectIdIfValid(companyId);

  if (normalizedCompanyId) {
    conditions.push({ companyId: normalizedCompanyId });
  } else if (normalizedUserId) {
    conditions.push({ userId: normalizedUserId });
  } else {
    conditions.push({ _id: { $exists: false } });
  }

  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }

  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const getDayRange = (value = new Date()) => {
  const current = value instanceof Date ? value : new Date(value);
  return {
    startOfDay: new Date(
      current.getFullYear(),
      current.getMonth(),
      current.getDate(),
      0,
      0,
      0,
      0
    ),
    endOfDay: new Date(
      current.getFullYear(),
      current.getMonth(),
      current.getDate(),
      23,
      59,
      59,
      999
    )
  };
};

const buildNeedsReplyCondition = (replyCutoff = null) => {
  const inboundCondition = replyCutoff
    ? { $ne: null, $lte: replyCutoff }
    : { $ne: null };

  return {
    lastInboundMessageAt: inboundCondition,
    $or: [
      { lastContactAt: null },
      { lastContactAt: { $exists: false } },
      { $expr: { $gt: ['$lastInboundMessageAt', '$lastContactAt'] } }
    ]
  };
};

const resolveOwnerNameMap = async (ownerIds = []) => {
  const validOwnerIds = Array.from(
    new Set(
      ownerIds
        .map((ownerId) => toCleanString(ownerId))
        .filter(Boolean)
        .filter((ownerId) => mongoose.Types.ObjectId.isValid(ownerId))
    )
  );

  if (validOwnerIds.length === 0) return new Map();

  const users = await User.find({ _id: { $in: validOwnerIds } })
    .select('_id name email')
    .lean();

  return new Map(
    (users || []).map((user) => [
      String(user?._id || '').trim(),
      String(user?.name || user?.email || '').trim() || 'Team Member'
    ])
  );
};

const aggregateOwnerContacts = async (scope = {}, now = new Date(), replySlaHours = DEFAULT_REPLY_SLA_HOURS) => {
  const { startOfDay, endOfDay } = getDayRange(now);
  const replyCutoff = new Date(now.getTime() - replySlaHours * 60 * 60 * 1000);

  return Contact.aggregate([
    { $match: buildScopedFilter(scope) },
    {
      $project: {
        ownerId: {
          $cond: [
            {
              $or: [
                { $eq: ['$ownerId', null] },
                { $eq: ['$ownerId', ''] }
              ]
            },
            '__unassigned__',
            '$ownerId'
          ]
        },
        nextFollowUpAt: 1,
        lastInboundMessageAt: 1,
        lastContactAt: 1
      }
    },
    {
      $project: {
        ownerId: 1,
        isOverdueFollowUp: {
          $cond: [
            {
              $and: [
                { $ne: ['$nextFollowUpAt', null] },
                { $lt: ['$nextFollowUpAt', now] }
              ]
            },
            1,
            0
          ]
        },
        isDueToday: {
          $cond: [
            {
              $and: [
                { $gte: ['$nextFollowUpAt', startOfDay] },
                { $lte: ['$nextFollowUpAt', endOfDay] }
              ]
            },
            1,
            0
          ]
        },
        needsReply: {
          $cond: [
            {
              $and: [
                { $ne: ['$lastInboundMessageAt', null] },
                {
                  $or: [
                    { $eq: ['$lastContactAt', null] },
                    { $gt: ['$lastInboundMessageAt', '$lastContactAt'] }
                  ]
                }
              ]
            },
            1,
            0
          ]
        },
        responseSlaBreaches: {
          $cond: [
            {
              $and: [
                { $ne: ['$lastInboundMessageAt', null] },
                { $lte: ['$lastInboundMessageAt', replyCutoff] },
                {
                  $or: [
                    { $eq: ['$lastContactAt', null] },
                    { $gt: ['$lastInboundMessageAt', '$lastContactAt'] }
                  ]
                }
              ]
            },
            1,
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: '$ownerId',
        contactCount: { $sum: 1 },
        overdueFollowUps: { $sum: '$isOverdueFollowUp' },
        dueTodayFollowUps: { $sum: '$isDueToday' },
        needsReply: { $sum: '$needsReply' },
        responseSlaBreaches: { $sum: '$responseSlaBreaches' }
      }
    }
  ]);
};

const aggregateOwnerDeals = async (scope = {}) =>
  Deal.aggregate([
    { $match: buildScopedFilter(scope) },
    {
      $project: {
        ownerId: {
          $cond: [
            {
              $or: [
                { $eq: ['$ownerId', null] },
                { $eq: ['$ownerId', ''] }
              ]
            },
            '__unassigned__',
            '$ownerId'
          ]
        },
        status: { $toLower: { $ifNull: ['$status', 'open'] } },
        value: { $ifNull: ['$value', 0] }
      }
    },
    {
      $group: {
        _id: '$ownerId',
        openDeals: {
          $sum: {
            $cond: [{ $eq: ['$status', 'open'] }, 1, 0]
          }
        },
        wonDeals: {
          $sum: {
            $cond: [{ $eq: ['$status', 'won'] }, 1, 0]
          }
        },
        pipelineValue: {
          $sum: {
            $cond: [{ $eq: ['$status', 'open'] }, '$value', 0]
          }
        },
        wonValue: {
          $sum: {
            $cond: [{ $eq: ['$status', 'won'] }, '$value', 0]
          }
        }
      }
    }
  ]);

const aggregateOwnerTasks = async (scope = {}, now = new Date()) => {
  const { startOfDay, endOfDay } = getDayRange(now);

  return LeadTask.aggregate([
    { $match: buildScopedFilter(scope) },
    {
      $project: {
        assignedTo: {
          $cond: [
            {
              $or: [
                { $eq: ['$assignedTo', null] },
                { $eq: ['$assignedTo', ''] }
              ]
            },
            '__unassigned__',
            '$assignedTo'
          ]
        },
        status: { $toLower: { $ifNull: ['$status', 'pending'] } },
        dueAt: 1
      }
    },
    {
      $project: {
        assignedTo: 1,
        openTasks: {
          $cond: [{ $in: ['$status', OPEN_TASK_STATUSES] }, 1, 0]
        },
        overdueTasks: {
          $cond: [
            {
              $and: [
                { $in: ['$status', OPEN_TASK_STATUSES] },
                { $ne: ['$dueAt', null] },
                { $lt: ['$dueAt', now] }
              ]
            },
            1,
            0
          ]
        },
        dueTodayTasks: {
          $cond: [
            {
              $and: [
                { $in: ['$status', OPEN_TASK_STATUSES] },
                { $gte: ['$dueAt', startOfDay] },
                { $lte: ['$dueAt', endOfDay] }
              ]
            },
            1,
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: '$assignedTo',
        openTasks: { $sum: '$openTasks' },
        overdueTasks: { $sum: '$overdueTasks' },
        dueTodayTasks: { $sum: '$dueTodayTasks' }
      }
    }
  ]);
};

const getCrmOwnerDashboard = async ({ userId = null, companyId = null } = {}) => {
  const now = new Date();
  const { startOfDay, endOfDay } = getDayRange(now);
  const replySlaHours = DEFAULT_REPLY_SLA_HOURS;
  const replyCutoff = new Date(now.getTime() - replySlaHours * 60 * 60 * 1000);
  const scope = { userId, companyId };

  const [
    myLeads,
    unassignedLeads,
    overdueFollowUps,
    dueTodayFollowUps,
    needsReply,
    responseSlaBreaches,
    openTasks,
    overdueTasks,
    dueTodayTasks,
    openDeals,
    contactOwnerRows,
    dealOwnerRows,
    taskOwnerRows
  ] = await Promise.all([
    userId ? Contact.countDocuments(buildScopedFilter(scope, { ownerId: userId })) : 0,
    Contact.countDocuments(
      buildScopedFilter(scope, {
        $or: [{ ownerId: null }, { ownerId: '' }, { ownerId: { $exists: false } }]
      })
    ),
    Contact.countDocuments(
      buildScopedFilter(scope, {
        nextFollowUpAt: { $ne: null, $lt: now }
      })
    ),
    Contact.countDocuments(
      buildScopedFilter(scope, { nextFollowUpAt: { $gte: startOfDay, $lte: endOfDay } })
    ),
    Contact.countDocuments(buildScopedFilter(scope, buildNeedsReplyCondition())),
    Contact.countDocuments(buildScopedFilter(scope, buildNeedsReplyCondition(replyCutoff))),
    LeadTask.countDocuments(buildScopedFilter(scope, { status: { $in: OPEN_TASK_STATUSES } })),
    LeadTask.countDocuments(
      buildScopedFilter(scope, {
        status: { $in: OPEN_TASK_STATUSES },
        dueAt: { $ne: null, $lt: now }
      })
    ),
    LeadTask.countDocuments(
      buildScopedFilter(scope, {
        status: { $in: OPEN_TASK_STATUSES },
        dueAt: { $gte: startOfDay, $lte: endOfDay }
      })
    ),
    Deal.countDocuments(buildScopedFilter(scope, { status: 'open' })),
    aggregateOwnerContacts(scope, now, replySlaHours),
    aggregateOwnerDeals(scope),
    aggregateOwnerTasks(scope, now)
  ]);

  const ownerPerformance = new Map();
  const applyOwnerValues = (ownerKey, patch = {}) => {
    const normalizedOwnerKey = toCleanString(ownerKey) || '__unassigned__';
    const existing = ownerPerformance.get(normalizedOwnerKey) || {
      ownerId: normalizedOwnerKey === '__unassigned__' ? '' : normalizedOwnerKey,
      ownerName: normalizedOwnerKey === '__unassigned__' ? 'Unassigned' : normalizedOwnerKey,
      contactCount: 0,
      overdueFollowUps: 0,
      dueTodayFollowUps: 0,
      needsReply: 0,
      responseSlaBreaches: 0,
      openDeals: 0,
      wonDeals: 0,
      pipelineValue: 0,
      wonValue: 0,
      openTasks: 0,
      overdueTasks: 0,
      dueTodayTasks: 0
    };

    ownerPerformance.set(normalizedOwnerKey, {
      ...existing,
      ...patch
    });
  };

  (contactOwnerRows || []).forEach((row) => {
    applyOwnerValues(row?._id, {
      contactCount: Number(row?.contactCount || 0),
      overdueFollowUps: Number(row?.overdueFollowUps || 0),
      dueTodayFollowUps: Number(row?.dueTodayFollowUps || 0),
      needsReply: Number(row?.needsReply || 0),
      responseSlaBreaches: Number(row?.responseSlaBreaches || 0)
    });
  });

  (dealOwnerRows || []).forEach((row) => {
    applyOwnerValues(row?._id, {
      openDeals: Number(row?.openDeals || 0),
      wonDeals: Number(row?.wonDeals || 0),
      pipelineValue: Number(row?.pipelineValue || 0),
      wonValue: Number(row?.wonValue || 0)
    });
  });

  (taskOwnerRows || []).forEach((row) => {
    applyOwnerValues(row?._id, {
      openTasks: Number(row?.openTasks || 0),
      overdueTasks: Number(row?.overdueTasks || 0),
      dueTodayTasks: Number(row?.dueTodayTasks || 0)
    });
  });

  const ownerNameMap = await resolveOwnerNameMap(
    Array.from(ownerPerformance.values()).map((owner) => owner.ownerId)
  );

  const owners = Array.from(ownerPerformance.values())
    .map((owner) => ({
      ...owner,
      ownerName:
        owner.ownerId && ownerNameMap.has(owner.ownerId)
          ? ownerNameMap.get(owner.ownerId)
          : owner.ownerName
    }))
    .sort((left, right) => {
      const scoreLeft =
        left.responseSlaBreaches * 5 + left.overdueFollowUps * 4 + left.pipelineValue / 1000;
      const scoreRight =
        right.responseSlaBreaches * 5 + right.overdueFollowUps * 4 + right.pipelineValue / 1000;
      return scoreRight - scoreLeft;
    });

  return {
    summary: {
      myLeads: Number(myLeads || 0),
      unassignedLeads: Number(unassignedLeads || 0),
      overdueFollowUps: Number(overdueFollowUps || 0),
      dueTodayFollowUps: Number(dueTodayFollowUps || 0),
      needsReply: Number(needsReply || 0),
      responseSlaBreaches: Number(responseSlaBreaches || 0),
      openTasks: Number(openTasks || 0),
      overdueTasks: Number(overdueTasks || 0),
      dueTodayTasks: Number(dueTodayTasks || 0),
      openDeals: Number(openDeals || 0)
    },
    owners,
    slaHours: replySlaHours,
    generatedAt: now.toISOString()
  };
};

const buildAutomationTaskPayload = ({
  contact,
  conversationId,
  automationRule,
  title,
  description,
  dueAt,
  reminderAt
}) => ({
  userId: contact.userId,
  companyId: contact.companyId || null,
  contactId: contact._id,
  conversationId: conversationId || null,
  title,
  description,
  taskType: automationRule === CRM_AUTOMATION_RULES.DEAL_CLOSE_RISK ? 'meeting' : 'follow_up',
  dueAt,
  reminderAt,
  priority:
    automationRule === CRM_AUTOMATION_RULES.REPLY_SLA_BREACH ||
    automationRule === CRM_AUTOMATION_RULES.DEAL_CLOSE_RISK
      ? 'high'
      : 'medium',
  status: 'pending',
  assignedTo: toCleanString(contact.ownerId) || null,
  createdBy: toCleanString(contact.userId) || null,
  automationRule,
  automationSource: 'crm_sla_automation',
  completedAt: null,
  completedBy: null
});

const resolveLatestConversationId = async (contact) => {
  const conversation = await Conversation.findOne(
    buildScopedFilter(
      {
        userId: contact?.userId || null,
        companyId: contact?.companyId || null
      },
      { contactId: contact?._id || null }
    )
  )
    .sort({ lastMessageTime: -1, updatedAt: -1 })
    .select('_id')
    .lean();

  return conversation?._id || null;
};

const ruleExistsForContact = async (contact, automationRule) =>
  LeadTask.findOne(
    buildScopedFilter(
      {
        userId: contact?.userId || null,
        companyId: contact?.companyId || null
      },
      {
        contactId: contact?._id || null,
        automationRule,
        status: { $in: OPEN_TASK_STATUSES }
      }
    )
  )
    .select('_id')
    .lean();

const incrementRuleCount = (target = {}, rule) => {
  const key = toCleanString(rule) || 'unknown';
  target[key] = Number(target[key] || 0) + 1;
  return target;
};

const toRunTriggerSource = ({ dryRun = false, automationActor = '' } = {}) => {
  if (toCleanString(automationActor).startsWith('system:')) return 'scheduler';
  return dryRun ? 'manual_preview' : 'manual_run';
};

const recordAutomationRun = async ({
  userId = null,
  companyId = null,
  triggerSource = 'manual_run',
  automationActor = null,
  dryRun = false,
  candidateCount = 0,
  createdCount = 0,
  byRule = {},
  tasks = [],
  contactUpdates = [],
  ownerNotifications = [],
  emailNotifications = {},
  slaHours = 0,
  generatedAt = null,
  status = 'success',
  errorMessage = ''
} = {}) => {
  try {
    await CrmAutomationRun.create({
      userId: toObjectIdIfValid(userId),
      companyId: toObjectIdIfValid(companyId),
      triggerSource,
      automationActor: toCleanString(automationActor),
      dryRun: Boolean(dryRun),
      candidateCount: Number(candidateCount || 0),
      createdCount: Number(createdCount || 0),
      byRule,
      tasksPreview: Array.isArray(tasks) ? tasks.slice(0, 25) : [],
      contactUpdatesPreview: Array.isArray(contactUpdates) ? contactUpdates.slice(0, 25) : [],
      ownerNotificationsPreview: Array.isArray(ownerNotifications)
        ? ownerNotifications.slice(0, 25)
        : [],
      emailNotifications: {
        attempted: Number(emailNotifications?.attempted || 0),
        delivered: Number(emailNotifications?.delivered || 0),
        skipped: Number(emailNotifications?.skipped || 0),
        failed: Number(emailNotifications?.failed || 0)
      },
      slaHours: Number(slaHours || 0),
      generatedAt: generatedAt ? new Date(generatedAt) : new Date(),
      status,
      errorMessage: toCleanString(errorMessage)
    });
  } catch (error) {
    console.error('CRM automation history log failed:', error.message);
  }
};

const sendOwnerNotificationEmails = async (items = [], ownerNameMap = new Map()) => {
  const summary = {
    attempted: 0,
    delivered: 0,
    skipped: 0,
    failed: 0
  };

  if (!Array.isArray(items) || items.length === 0) return summary;
  if (!isSmtpConfigured()) {
    summary.skipped = items.length;
    return summary;
  }

  const ownerIds = Array.from(
    new Set(items.map((item) => toCleanString(item?.ownerId)).filter(Boolean))
  ).filter((ownerId) => mongoose.Types.ObjectId.isValid(ownerId));

  const owners = ownerIds.length
    ? await User.find({ _id: { $in: ownerIds } }).select('_id email name').lean()
    : [];
  const ownerMap = new Map(
    (owners || []).map((owner) => [String(owner?._id || '').trim(), owner])
  );

  const deliveredKeys = new Set();

  for (const item of items) {
    const ownerId = toCleanString(item?.ownerId);
    const owner = ownerMap.get(ownerId);
    const to = toCleanString(owner?.email);
    const dedupeKey = `${ownerId}:${toCleanString(item?.contactId)}:${toCleanString(item?.automationRule)}`;

    if (!ownerId || !to || deliveredKeys.has(dedupeKey)) {
      summary.skipped += 1;
      continue;
    }

    deliveredKeys.add(dedupeKey);
    summary.attempted += 1;

    try {
      const ownerName = toCleanString(ownerNameMap.get(ownerId) || owner?.name || 'Team Member');
      const contactName = toCleanString(item?.contactName || item?.phone || 'Assigned lead');
      const ruleLabel = toCleanString(item?.automationRule).replace(/_/g, ' ') || 'CRM alert';
      const templateText = toCleanString(item?.recommendedTemplate);

      await sendAppEmail({
        to,
        subject: `CRM alert: ${contactName}`,
        text: [
          `Hi ${ownerName},`,
          '',
          `A CRM automation alert was generated for ${contactName}.`,
          `Rule: ${ruleLabel}`,
          item?.leadScore ? `Lead score: ${Number(item.leadScore)}` : '',
          templateText ? `Recommended template: ${templateText}` : '',
          '',
          'Open CRM Ops to review and act on this alert.'
        ]
          .filter(Boolean)
          .join('\n')
      });
      summary.delivered += 1;
    } catch (error) {
      summary.failed += 1;
      console.error('CRM owner notification email failed:', error.message);
    }
  }

  return summary;
};

const runCrmFollowUpAutomation = async ({
  userId = null,
  companyId = null,
  dryRun = false,
  limit = DEFAULT_AUTOMATION_LIMIT,
  automationActor = 'system:scheduler'
} = {}) => {
  const now = new Date();
  const replySlaHours = DEFAULT_REPLY_SLA_HOURS;
  const triggerSource = toRunTriggerSource({ dryRun, automationActor });

  try {
    const replyCutoff = new Date(now.getTime() - replySlaHours * 60 * 60 * 1000);
    const dealCloseCutoff = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    const scope = { userId, companyId };
    const candidateLimit = Math.max(Number(limit) || DEFAULT_AUTOMATION_LIMIT, 1);

    const scoringSettings = await getLeadScoringSettings({ userId, companyId }).catch(() => null);
    const scoringAutomation = scoringSettings?.automation || {};
    const ownerNotifications = [];

    const [overdueContacts, replySlaContacts, dealRiskDeals, optedInContacts, highScoreContacts] =
      await Promise.all([
        Contact.find(buildScopedFilter(scope, { nextFollowUpAt: { $ne: null, $lt: now } }))
          .sort({ nextFollowUpAt: 1 })
          .limit(candidateLimit)
          .lean(),
        Contact.find(buildScopedFilter(scope, buildNeedsReplyCondition(replyCutoff)))
          .sort({ lastInboundMessageAt: 1 })
          .limit(candidateLimit)
          .lean(),
        Deal.find(
          buildScopedFilter(scope, {
            status: 'open',
            expectedCloseAt: { $ne: null, $lte: dealCloseCutoff }
          })
        )
          .sort({ expectedCloseAt: 1, value: -1 })
          .limit(candidateLimit)
          .lean(),
        Contact.find(
          buildScopedFilter(scope, {
            whatsappOptInStatus: 'opted_in',
            stage: { $in: ['new', 'contacted'] }
          })
        )
          .sort({ updatedAt: -1, createdAt: -1 })
          .limit(candidateLimit)
          .lean(),
        scoringAutomation?.isEnabled
          ? Contact.find(
              buildScopedFilter(scope, {
                leadScore: { $gte: Number(scoringAutomation.taskThreshold || 0) }
              })
            )
              .sort({ leadScore: -1, lastLeadScoreAt: -1, updatedAt: -1 })
              .limit(candidateLimit)
              .lean()
          : Promise.resolve([])
      ]);

    const candidates = [];
    const contactUpdates = [];

    const maybeNotifyOwner = async ({ contact, automationRule, leadScore = 0, recommendedTemplate = '' }) => {
    const ownerId = toCleanString(contact?.ownerId);
    if (!ownerId || scoringAutomation?.ownerNotification !== true) return;

    ownerNotifications.push({
      contactId: String(contact?._id || ''),
      contactName: String(contact?.name || '').trim() || 'Unknown',
      phone: String(contact?.phone || '').trim(),
      ownerId,
      automationRule,
      leadScore: Number(leadScore || 0),
      recommendedTemplate: toCleanString(recommendedTemplate)
    });

    try {
      await LeadActivity.create({
        userId: contact.userId,
        companyId: contact.companyId || null,
        contactId: contact._id,
        conversationId: await resolveLatestConversationId(contact),
        type: 'owner_notified',
        meta: {
          ownerId,
          automationRule,
          leadScore: Number(leadScore || 0),
          recommendedTemplate: toCleanString(recommendedTemplate)
        },
        createdBy: automationActor
      });
    } catch (error) {
      console.error('CRM owner notification log failed:', error.message);
    }
    };

    const queueContactUpdate = ({ contact, rule, nextStage, meta = {} }) => {
    if (!contact?._id || !toCleanString(nextStage)) return;

    contactUpdates.push({
      contactId: String(contact._id),
      contactName: String(contact?.name || '').trim() || 'Unknown',
      phone: String(contact?.phone || '').trim(),
      previousStage: toCleanString(contact?.stage) || 'new',
      nextStage: toCleanString(nextStage),
      automationRule: rule,
      ...meta
    });
    };

    const queueCandidate = async ({
    contact,
    rule,
    title,
    description,
    dueAt,
    reminderAt,
    meta = {}
    }) => {
    if (!contact?._id) return;
    const hasExistingTask = await ruleExistsForContact(contact, rule);
    if (hasExistingTask) return;

    candidates.push({
      contactId: String(contact._id),
      contactName: String(contact?.name || '').trim() || 'Unknown',
      phone: String(contact?.phone || '').trim(),
      ownerId: toCleanString(contact?.ownerId),
      automationRule: rule,
      title,
      description,
      dueAt: dueAt ? new Date(dueAt).toISOString() : null,
      reminderAt: reminderAt ? new Date(reminderAt).toISOString() : null,
      ...meta
    });
    };

    for (const contact of overdueContacts || []) {
    await queueCandidate({
      contact,
      rule: CRM_AUTOMATION_RULES.OVERDUE_FOLLOW_UP,
      title: 'Overdue follow-up pending',
      description: 'Lead follow-up date passed without completion.',
      dueAt: now,
      reminderAt: now
    });
    }

    for (const contact of replySlaContacts || []) {
    await queueCandidate({
      contact,
      rule: CRM_AUTOMATION_RULES.REPLY_SLA_BREACH,
      title: 'Reply to inbound message',
      description: `Inbound lead has been waiting longer than ${replySlaHours} hours for a response.`,
      dueAt: now,
      reminderAt: now,
      meta: {
        responseSlaHours: replySlaHours
      }
    });
    }

    for (const deal of dealRiskDeals || []) {
    const contact = await Contact.findOne(
      buildScopedFilter(
        { userId: deal?.userId || null, companyId: deal?.companyId || null },
        { _id: deal?.contactId || null }
      )
    ).lean();
    if (!contact) continue;

    await queueCandidate({
      contact,
      rule: CRM_AUTOMATION_RULES.DEAL_CLOSE_RISK,
      title: `Deal follow-up: ${toCleanString(deal?.title) || 'Closing deal'}`,
      description: 'Open deal is due to close within 24 hours or is already overdue.',
      dueAt: deal?.expectedCloseAt || now,
      reminderAt: now,
      meta: {
        dealId: String(deal?._id || ''),
        dealTitle: toCleanString(deal?.title)
      }
    });
    }

    for (const contact of optedInContacts || []) {
    queueContactUpdate({
      contact,
      rule: CRM_AUTOMATION_RULES.OPT_IN_TO_NURTURING,
      nextStage: 'nurturing',
      meta: {
        reason: 'Lead completed WhatsApp opt-in.'
      }
    });
    }

    if (scoringAutomation?.isEnabled) {
    const stageTarget = toCleanString(scoringAutomation.stageOnThreshold).toLowerCase() || 'qualified';
    const stageThreshold = Number(scoringAutomation.stageThreshold || 0);
    const taskThreshold = Number(scoringAutomation.taskThreshold || 0);
    const recommendedTemplate = toCleanString(scoringAutomation.recommendedTemplate);

    for (const contact of highScoreContacts || []) {
      const contactStageRank = getLeadStageRank(contact?.stage);
      const targetStageRank = getLeadStageRank(stageTarget);

      if (
        Number(contact?.leadScore || 0) >= stageThreshold &&
        contactStageRank >= 0 &&
        targetStageRank >= 0 &&
        contactStageRank < targetStageRank
      ) {
        queueContactUpdate({
          contact,
          rule: CRM_AUTOMATION_RULES.LEAD_SCORE_STAGE_ADVANCE,
          nextStage: stageTarget,
          meta: {
            leadScore: Number(contact?.leadScore || 0),
            recommendedTemplate
          }
        });
      }

      if (Number(contact?.leadScore || 0) >= taskThreshold) {
        await queueCandidate({
          contact,
          rule: CRM_AUTOMATION_RULES.LEAD_SCORE_THRESHOLD,
          title: toCleanString(scoringAutomation.taskTitle) || 'High intent lead follow-up',
          description: recommendedTemplate
            ? `Lead score crossed automation threshold. Recommended template: ${recommendedTemplate}`
            : 'Lead score crossed automation threshold. Follow up with this contact.',
          dueAt: now,
          reminderAt: now,
          meta: {
            leadScore: Number(contact?.leadScore || 0),
            recommendedTemplate
          }
        });
      }
    }
    }

    const byRule = {};
    candidates.forEach((item) => incrementRuleCount(byRule, item.automationRule));
    contactUpdates.forEach((item) => incrementRuleCount(byRule, item.automationRule));
    const previewOwnerNotifications =
      scoringAutomation?.ownerNotification === true
        ? [
            ...candidates
              .filter((item) => toCleanString(item?.ownerId))
              .map((item) => ({
                contactId: item.contactId,
                contactName: item.contactName,
                phone: item.phone,
                ownerId: toCleanString(item.ownerId),
                automationRule: item.automationRule,
                leadScore: Number(item?.leadScore || 0),
                recommendedTemplate: toCleanString(item?.recommendedTemplate)
              })),
            ...contactUpdates
              .filter((item) => {
                const matchingContact = [...highScoreContacts, ...optedInContacts].find(
                  (contact) => String(contact?._id || '') === String(item?.contactId || '')
                );
                return toCleanString(matchingContact?.ownerId);
              })
              .map((item) => {
                const matchingContact = [...highScoreContacts, ...optedInContacts].find(
                  (contact) => String(contact?._id || '') === String(item?.contactId || '')
                );
                return {
                  contactId: item.contactId,
                  contactName: item.contactName,
                  phone: item.phone,
                  ownerId: toCleanString(matchingContact?.ownerId),
                  automationRule: item.automationRule,
                  leadScore: Number(item?.leadScore || 0),
                  recommendedTemplate: toCleanString(item?.recommendedTemplate)
                };
              })
          ]
        : [];

    if (dryRun || (candidates.length === 0 && contactUpdates.length === 0)) {
      const dryRunResult = {
        dryRun: Boolean(dryRun),
        createdCount: 0,
        candidateCount: candidates.length + contactUpdates.length,
        byRule,
        tasks: candidates.slice(0, candidateLimit),
        contactUpdates: contactUpdates.slice(0, candidateLimit),
        ownerNotifications: previewOwnerNotifications.slice(0, candidateLimit),
        slaHours: replySlaHours,
        leadScoring: scoringAutomation,
        generatedAt: now.toISOString()
      };

      await recordAutomationRun({
        userId,
        companyId,
        triggerSource,
        automationActor,
        dryRun,
        candidateCount: dryRunResult.candidateCount,
        createdCount: 0,
        byRule,
        tasks: dryRunResult.tasks,
        contactUpdates: dryRunResult.contactUpdates,
        ownerNotifications: dryRunResult.ownerNotifications,
        emailNotifications: { attempted: 0, delivered: 0, skipped: dryRunResult.ownerNotifications.length, failed: 0 },
        slaHours: replySlaHours,
        generatedAt: dryRunResult.generatedAt
      });

      return dryRunResult;
    }

    const createdTasks = [];
    const appliedContactUpdates = [];

    for (const update of contactUpdates.slice(0, candidateLimit)) {
    const scopedContact = await Contact.findOne(buildScopedFilter(scope, { _id: update.contactId }));
    if (!scopedContact) continue;

    const previousStage = toCleanString(scopedContact.stage) || 'new';
    scopedContact.stage = update.nextStage;
    if (update.automationRule === CRM_AUTOMATION_RULES.LEAD_SCORE_STAGE_ADVANCE && update.nextStage === 'qualified') {
      scopedContact.status = 'qualified';
    }
    scopedContact.lastStageChangedAt = new Date();
    await scopedContact.save();

    appliedContactUpdates.push({
      contactId: String(scopedContact._id),
      contactName: String(scopedContact?.name || '').trim() || 'Unknown',
      phone: String(scopedContact?.phone || '').trim(),
      previousStage,
      nextStage: scopedContact.stage,
      automationRule: update.automationRule,
      leadScore: Number(update?.leadScore || scopedContact?.leadScore || 0),
      recommendedTemplate: toCleanString(update?.recommendedTemplate)
    });

    try {
      await LeadActivity.create({
        userId: scopedContact.userId,
        companyId: scopedContact.companyId || null,
        contactId: scopedContact._id,
        conversationId: await resolveLatestConversationId(scopedContact),
        type: 'automation_rule_applied',
        meta: {
          automationRule: update.automationRule,
          previousStage,
          nextStage: scopedContact.stage,
          leadScore: Number(update?.leadScore || scopedContact?.leadScore || 0),
          recommendedTemplate: toCleanString(update?.recommendedTemplate)
        },
        createdBy: automationActor
      });
    } catch (error) {
      console.error('CRM automation stage log failed:', error.message);
    }

    await maybeNotifyOwner({
      contact: scopedContact,
      automationRule: update.automationRule,
      leadScore: Number(update?.leadScore || scopedContact?.leadScore || 0),
      recommendedTemplate: toCleanString(update?.recommendedTemplate)
    });
    }

    for (const candidate of candidates.slice(0, candidateLimit)) {
    const contact = await Contact.findById(candidate.contactId).lean();
    if (!contact) continue;

    const conversationId = await resolveLatestConversationId(contact);
    const task = await LeadTask.create(
      buildAutomationTaskPayload({
        contact,
        conversationId,
        automationRule: candidate.automationRule,
        title: candidate.title,
        description: candidate.description,
        dueAt: candidate.dueAt,
        reminderAt: candidate.reminderAt
      })
    );

    createdTasks.push({
      _id: String(task?._id || ''),
      contactId: String(contact?._id || ''),
      contactName: String(contact?.name || '').trim() || 'Unknown',
      phone: String(contact?.phone || '').trim(),
      automationRule: candidate.automationRule,
      title: task?.title,
      dueAt: task?.dueAt || null,
      leadScore: Number(candidate?.leadScore || 0),
      recommendedTemplate: toCleanString(candidate?.recommendedTemplate)
    });

    try {
      await LeadActivity.create({
        userId: contact.userId,
        companyId: contact.companyId || null,
        contactId: contact._id,
        conversationId: conversationId || null,
        type: 'task_created',
        meta: {
          taskId: String(task?._id || ''),
          title: task?.title,
          automationRule: candidate.automationRule,
          priority: task?.priority,
          status: task?.status,
          leadScore: Number(candidate?.leadScore || 0),
          recommendedTemplate: toCleanString(candidate?.recommendedTemplate)
        },
        createdBy: automationActor
      });
    } catch (error) {
      console.error('CRM automation activity log failed:', error.message);
    }

    await maybeNotifyOwner({
      contact,
      automationRule: candidate.automationRule,
      leadScore: Number(candidate?.leadScore || 0),
      recommendedTemplate: toCleanString(candidate?.recommendedTemplate)
    });
    }

    const ownerNameMap = await resolveOwnerNameMap(ownerNotifications.map((item) => item.ownerId));
    const emailNotifications = await sendOwnerNotificationEmails(ownerNotifications, ownerNameMap);

    const successResult = {
      dryRun: false,
      createdCount: createdTasks.length + appliedContactUpdates.length,
      candidateCount: candidates.length + contactUpdates.length,
      byRule,
      tasks: createdTasks,
      contactUpdates: appliedContactUpdates,
      ownerNotifications,
      slaHours: replySlaHours,
      leadScoring: scoringAutomation,
      emailNotifications,
      generatedAt: new Date().toISOString()
    };

    await recordAutomationRun({
      userId,
      companyId,
      triggerSource,
      automationActor,
      dryRun: false,
      candidateCount: successResult.candidateCount,
      createdCount: successResult.createdCount,
      byRule,
      tasks: createdTasks,
      contactUpdates: appliedContactUpdates,
      ownerNotifications,
      emailNotifications,
      slaHours: replySlaHours,
      generatedAt: successResult.generatedAt
    });

    return successResult;
  } catch (error) {
    await recordAutomationRun({
      userId,
      companyId,
      triggerSource,
      automationActor,
      dryRun,
      candidateCount: 0,
      createdCount: 0,
      byRule: {},
      tasks: [],
      contactUpdates: [],
      ownerNotifications: [],
      emailNotifications: { attempted: 0, delivered: 0, skipped: 0, failed: 0 },
      slaHours: replySlaHours,
      generatedAt: now.toISOString(),
      status: 'error',
      errorMessage: error?.message || 'CRM automation run failed'
    });
    throw error;
  }
};

module.exports = {
  CRM_AUTOMATION_RULES,
  getCrmOwnerDashboard,
  runCrmFollowUpAutomation
};
