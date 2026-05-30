require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const LeadActivity = require('../models/LeadActivity');
const LeadTask = require('../models/LeadTask');
const Deal = require('../models/Deal');
const { ContactDocument } = require('../models/ContactDocument');
const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');
const {
  buildContactPhoneLookupFilter,
  normalizePhoneDigits,
  normalizePhoneKey
} = require('../utils/contactIdentity');
const { invalidateInboxScope } = require('../utils/teamInboxCache');

const readArg = (names = []) => {
  const args = process.argv.slice(2);
  for (const name of names) {
    const eqMatch = args.find((arg) => arg.startsWith(`${name}=`));
    if (eqMatch) return eqMatch.slice(name.length + 1);

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

const parseObjectId = (value, label) => {
  const normalized = String(value || '').trim();
  if (!normalized) return null;
  if (!mongoose.Types.ObjectId.isValid(normalized)) {
    throw new Error(`${label} must be a valid ObjectId`);
  }
  return new mongoose.Types.ObjectId(normalized);
};

const asDate = (value) => {
  const date = value ? new Date(value) : null;
  return date instanceof Date && !Number.isNaN(date.getTime()) ? date : null;
};

const maxDate = (...values) => {
  const dates = values.map(asDate).filter(Boolean);
  if (!dates.length) return null;
  return new Date(Math.max(...dates.map((date) => date.getTime())));
};

const buildScopeFilter = ({ companyId = null, userId = null } = {}) => {
  const filter = {};
  if (companyId) filter.companyId = companyId;
  if (userId) filter.userId = userId;
  return filter;
};

const getContactIdentityTokens = (contact = {}) => {
  const phoneDigits = normalizePhoneDigits(contact.phoneDigits || contact.phone || '');
  const phoneKey = normalizePhoneKey(contact.phoneKey || phoneDigits || contact.phone || '');
  return Array.from(new Set([phoneDigits, phoneKey].filter(Boolean)));
};

const chooseKeeper = (members = [], strategy = 'earliest') => {
  const sorted = members.slice().sort((left, right) => {
    const leftCreated = asDate(left.createdAt)?.getTime() || 0;
    const rightCreated = asDate(right.createdAt)?.getTime() || 0;
    const leftUpdated = asDate(left.updatedAt)?.getTime() || 0;
    const rightUpdated = asDate(right.updatedAt)?.getTime() || 0;
    if (strategy === 'latest') {
      return rightUpdated - leftUpdated || rightCreated - leftCreated || String(right._id).localeCompare(String(left._id));
    }
    return leftCreated - rightCreated || leftUpdated - rightUpdated || String(left._id).localeCompare(String(right._id));
  });
  return sorted[0] || null;
};

const getLatestValue = (members = [], field) => {
  const sorted = members.slice().sort((left, right) => {
    const leftTime = asDate(left.updatedAt || left.lastContact || left.createdAt)?.getTime() || 0;
    const rightTime = asDate(right.updatedAt || right.lastContact || right.createdAt)?.getTime() || 0;
    return rightTime - leftTime;
  });
  const match = sorted.find((item) => String(item?.[field] || '').trim());
  return match ? String(match[field] || '').trim() : '';
};

const getEarliestValue = (members = [], field) => {
  const sorted = members.slice().sort((left, right) => {
    const leftTime = asDate(left.createdAt || left.updatedAt)?.getTime() || 0;
    const rightTime = asDate(right.createdAt || right.updatedAt)?.getTime() || 0;
    return leftTime - rightTime;
  });
  const match = sorted.find((item) => String(item?.[field] || '').trim());
  return match ? String(match[field] || '').trim() : '';
};

const updateContactReferences = async ({ Model, duplicateIds, keeperId, dryRun, statsKey, stats }) => {
  if (!Model || !duplicateIds.length) return;
  const filter = { contactId: { $in: duplicateIds } };
  if (dryRun) {
    stats[statsKey] += await Model.countDocuments(filter);
    return;
  }
  const result = await Model.updateMany(filter, { $set: { contactId: keeperId } });
  stats[statsKey] += Number(result?.modifiedCount || result?.nModified || 0);
};

const mergeContactGroup = async ({ members, keepStrategy, dryRun, stats, touchedScopes }) => {
  if (members.length <= 1) return;

  const keeper = chooseKeeper(members, keepStrategy);
  if (!keeper?._id) return;

  const keeperId = keeper._id;
  const duplicateMembers = members.filter((member) => String(member._id) !== String(keeperId));
  const duplicateIds = duplicateMembers.map((member) => member._id);
  const latestAssignedTo = getLatestValue(members, 'assignedTo') || getLatestValue(members, 'assignedAgent');
  const latestAssignedAgent = getLatestValue(members, 'assignedAgent') || latestAssignedTo;
  const latestOwnerId = getLatestValue(members, 'ownerId') || latestAssignedTo;
  const latestCreatedBy = getLatestValue(members, 'createdBy') || keeper.createdBy || keeper.userId || null;
  const mergedTags = Array.from(
    new Set(
      members
        .flatMap((member) => (Array.isArray(member.tags) ? member.tags : []))
        .map((tag) => String(tag || '').trim())
        .filter(Boolean)
    )
  );
  const latestLastContact = maxDate(
    ...members.flatMap((member) => [member.lastContact, member.lastContactAt, member.lastInboundMessageAt, member.updatedAt])
  );

  touchedScopes.add(
    JSON.stringify({
      companyId: String(keeper.companyId || ''),
      userId: String(keeper.userId || '')
    })
  );

  if (!dryRun) {
    await Contact.updateOne(
      { _id: keeperId },
      {
        $set: {
          userId: keeper.userId || members.find((member) => member.userId)?.userId || null,
          companyId: keeper.companyId || members.find((member) => member.companyId)?.companyId || null,
          createdBy: latestCreatedBy,
          assignedTo: latestAssignedTo || keeper.assignedTo || null,
          assignedAgent: latestAssignedAgent || keeper.assignedAgent || null,
          ownerId: latestOwnerId || keeper.ownerId || null,
          name: keeper.name || getLatestValue(members, 'name'),
          email: keeper.email || getLatestValue(members, 'email'),
          phone: keeper.phone || getLatestValue(members, 'phone'),
          phoneDigits: keeper.phoneDigits || normalizePhoneDigits(keeper.phone || getLatestValue(members, 'phone')),
          phoneKey: keeper.phoneKey || normalizePhoneKey(keeper.phoneDigits || keeper.phone || getLatestValue(members, 'phone')),
          tags: mergedTags,
          source: keeper.source || getEarliestValue(members, 'source'),
          sourceType: keeper.sourceType || getEarliestValue(members, 'sourceType') || 'manual',
          lastContact: latestLastContact || keeper.lastContact || keeper.updatedAt || keeper.createdAt,
          lastContactAt: maxDate(...members.map((member) => member.lastContactAt)) || keeper.lastContactAt || null,
          lastInboundMessageAt:
            maxDate(...members.map((member) => member.lastInboundMessageAt)) || keeper.lastInboundMessageAt || null,
          updatedAt: new Date()
        }
      }
    );
  }

  await Promise.all([
    updateContactReferences({
      Model: Conversation,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'conversationsReassigned',
      stats
    }),
    updateContactReferences({
      Model: ConversationSummary,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'conversationSummariesReassigned',
      stats
    }),
    updateContactReferences({
      Model: LeadActivity,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'leadActivitiesReassigned',
      stats
    }),
    updateContactReferences({
      Model: LeadTask,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'leadTasksReassigned',
      stats
    }),
    updateContactReferences({
      Model: Deal,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'dealsReassigned',
      stats
    }),
    updateContactReferences({
      Model: ContactDocument,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'contactDocumentsReassigned',
      stats
    }),
    updateContactReferences({
      Model: WhatsAppConsentLog,
      duplicateIds,
      keeperId,
      dryRun,
      statsKey: 'consentLogsReassigned',
      stats
    })
  ]);

  if (!dryRun && duplicateIds.length) {
    await Contact.deleteMany({ _id: { $in: duplicateIds } });
  }

  stats.groupsMerged += 1;
  stats.duplicatesRemoved += duplicateIds.length;
};

const run = async () => {
  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const phone = readArg(['--phone']);
  const keepStrategy = String(readArg(['--keep']) || 'earliest').trim().toLowerCase();
  const apply = hasFlag(['--apply']);
  const dryRun = !apply || hasFlag(['--dry-run', '--dryRun']);

  await connectDB();

  const scopeFilter = buildScopeFilter({ companyId, userId });
  const phoneFilter = buildContactPhoneLookupFilter(phone);
  const filters = phoneFilter
    ? {
        $and: [
          scopeFilter,
          phoneFilter
        ].filter((item) => item && Object.keys(item).length > 0)
      }
    : scopeFilter;

  const contacts = await Contact.find(filters)
    .select(
      '_id userId companyId createdBy ownerId assignedTo assignedAgent name phone phoneDigits phoneKey email tags source sourceType lastContact lastContactAt lastInboundMessageAt createdAt updatedAt'
    )
    .sort({ companyId: 1, createdAt: 1, updatedAt: 1, _id: 1 })
    .lean();

  const stats = {
    dryRun,
    scanned: contacts.length,
    groupsCreated: 0,
    groupsMerged: 0,
    duplicatesRemoved: 0,
    conversationsReassigned: 0,
    conversationSummariesReassigned: 0,
    leadActivitiesReassigned: 0,
    leadTasksReassigned: 0,
    dealsReassigned: 0,
    contactDocumentsReassigned: 0,
    consentLogsReassigned: 0
  };

  const groupsByKey = new Map();
  for (const contact of contacts) {
    const companyScopeKey = String(contact.companyId || '');
    const userScopeKey = String(contact.userId || '');
    const tokens = getContactIdentityTokens(contact);
    if (!tokens.length) continue;
    const baseScopeKey = companyScopeKey || userScopeKey;
    const groupKey = `${baseScopeKey}::${tokens[tokens.length - 1]}`;
    if (!groupsByKey.has(groupKey)) {
      groupsByKey.set(groupKey, []);
      stats.groupsCreated += 1;
    }
    groupsByKey.get(groupKey).push(contact);
  }

  const touchedScopes = new Set();
  for (const members of groupsByKey.values()) {
    await mergeContactGroup({
      members,
      keepStrategy,
      dryRun,
      stats,
      touchedScopes
    });
  }

  if (!dryRun) {
    for (const scopeJson of touchedScopes) {
      const scope = JSON.parse(scopeJson);
      await invalidateInboxScope(scope);
    }
  }

  console.log(JSON.stringify({ event: 'contact_phone_duplicate_repair_complete', ...stats }, null, 2));
  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Contact duplicate repair failed:', error);
  try {
    await mongoose.connection.close();
  } catch {
    // ignore close failures
  }
  process.exitCode = 1;
});
