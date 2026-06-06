require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');
const { ContactDocument } = require('../models/ContactDocument');
const LeadTask = require('../models/LeadTask');
const BroadcastDispatch = require('../models/BroadcastDispatch');
const LeadActivity = require('../models/LeadActivity');
const {
  deleteConversationSummary,
  upsertConversationSummary
} = require('../services/conversationSummaryService');
const {
  buildConversationPhoneLookupFilter,
  getConversationIdentityTokens,
  mergeConversationRecords
} = require('../utils/conversationIdentity');
const { invalidateInboxScope } = require('../utils/teamInboxCache');
const { normalizePhoneDigits } = require('../utils/conversationIdentity');

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

const buildScopeFilter = ({ companyId = null, userId = null } = {}) => {
  const filter = {};
  if (companyId) filter.companyId = companyId;
  if (userId) filter.userId = userId;
  return filter;
};

const buildScopeKey = (conversation = {}, { companyWide = false } = {}) =>
  `${String(conversation?.companyId || '').trim()}::${
    companyWide ? '*' : String(conversation?.userId || '').trim()
  }`;

const parseScopeKey = (scopeKey = '') => {
  const [companyId = '', userId = ''] = String(scopeKey || '').split('::');
  return {
    companyId: companyId.trim(),
    userId: userId.trim() === '*' ? '' : userId.trim()
  };
};

const toPlainConversation = (conversation = {}) => ({
  ...conversation,
  unreadCount: Math.max(0, Number(conversation?.unreadCount || 0) || 0),
  lastMessageTime:
    conversation?.lastMessageTime || conversation?.updatedAt || conversation?.createdAt || null
});

const updateRelatedDocs = async ({
  Model,
  filter,
  keeperId,
  dryRun,
  statsKey,
  stats
}) => {
  if (!Model) return 0;

  if (dryRun) {
    const count = await Model.countDocuments(filter);
    stats[statsKey] += count;
    return count;
  }

  const result = await Model.updateMany(filter, { $set: { conversationId: keeperId } });
  const modified = Number(result?.modifiedCount || result?.nModified || 0);
  stats[statsKey] += modified;
  return modified;
};

const mergeGroupIntoKeeper = async ({ group, dryRun, stats }) => {
  const keeper = group.members[0];
  const keeperId = String(keeper?._id || '').trim();
  if (!keeperId) return { merged: false, affectedScopes: [] };

  const mergedConversation = group.members
    .map(toPlainConversation)
    .reduce((acc, item) => mergeConversationRecords(acc, item), {});
  mergedConversation.unreadCount = group.members.reduce(
    (sum, item) => sum + Math.max(0, Number(item?.unreadCount || 0) || 0),
    0
  );

  const duplicateMembers = group.members.slice(1);
  const affectedScopes = new Set();
  if (group.scopeKey) {
    affectedScopes.add(group.scopeKey);
  }

  if (!dryRun) {
    await Conversation.updateOne(
      { _id: keeperId },
      {
        $set: {
          contactId: mergedConversation.contactId || keeper.contactId || null,
          contactPhone: mergedConversation.contactPhone || keeper.contactPhone || '',
          contactPhoneDigits:
            mergedConversation.contactPhoneDigits ||
            normalizePhoneDigits(mergedConversation.contactPhone || keeper.contactPhone || ''),
          contactName: mergedConversation.contactName || keeper.contactName || '',
          status: mergedConversation.status || keeper.status || 'active',
          assignedTo: mergedConversation.assignedTo || keeper.assignedTo || null,
          assignedToId: mergedConversation.assignedToId || keeper.assignedToId || null,
          assignedAgent: mergedConversation.assignedAgent || keeper.assignedAgent || null,
          tags: Array.isArray(mergedConversation.tags) ? mergedConversation.tags : [],
          priority: mergedConversation.priority || keeper.priority || 'medium',
          lastMessageTime: mergedConversation.lastMessageTime || keeper.lastMessageTime || new Date(),
          lastMessage: mergedConversation.lastMessage || keeper.lastMessage || '',
          lastMessageMediaType: mergedConversation.lastMessageMediaType || keeper.lastMessageMediaType || '',
          lastMessageAttachmentName:
            mergedConversation.lastMessageAttachmentName || keeper.lastMessageAttachmentName || '',
          lastMessageAttachmentPages:
            mergedConversation.lastMessageAttachmentPages ?? keeper.lastMessageAttachmentPages ?? null,
          lastMessageFrom: mergedConversation.lastMessageFrom || keeper.lastMessageFrom || '',
          lastMessageWhatsappMessageId:
            mergedConversation.lastMessageWhatsappMessageId ||
            keeper.lastMessageWhatsappMessageId ||
            '',
          lastMessageStatus: mergedConversation.lastMessageStatus || keeper.lastMessageStatus || '',
          unreadCount: mergedConversation.unreadCount,
          notes: mergedConversation.notes || keeper.notes || '',
          resolvedAt: mergedConversation.resolvedAt || keeper.resolvedAt || null,
          updatedAt: new Date()
        }
      }
    );
    await upsertConversationSummary({
      ...mergedConversation,
      conversationId: keeperId,
      userId: keeper.userId,
      companyId: keeper.companyId
    });
  }

  if (!duplicateMembers.length) {
    stats.keepersUpdated += 1;
    return { merged: false, affectedScopes: Array.from(affectedScopes) };
  }

  for (const duplicate of duplicateMembers) {
    const duplicateId = String(duplicate?._id || '').trim();
    if (!duplicateId) continue;

    await Promise.all([
      updateRelatedDocs({
        Model: Message,
        filter: { conversationId: duplicateId },
        keeperId,
        dryRun,
        statsKey: 'messagesReassigned',
        stats
      }),
      updateRelatedDocs({
        Model: BroadcastDispatch,
        filter: { conversationId: duplicateId },
        keeperId,
        dryRun,
        statsKey: 'broadcastDispatchesReassigned',
        stats
      }),
      updateRelatedDocs({
        Model: LeadActivity,
        filter: { conversationId: duplicateId },
        keeperId,
        dryRun,
        statsKey: 'leadActivitiesReassigned',
        stats
      }),
      updateRelatedDocs({
        Model: LeadTask,
        filter: { conversationId: duplicateId },
        keeperId,
        dryRun,
        statsKey: 'leadTasksReassigned',
        stats
      }),
      updateRelatedDocs({
        Model: ContactDocument,
        filter: { conversationId: duplicateId },
        keeperId,
        dryRun,
        statsKey: 'contactDocumentsReassigned',
        stats
      })
    ]);

    if (!dryRun) {
      await deleteConversationSummary(duplicateId);
      await Conversation.deleteOne({ _id: duplicateId });
    }

    stats.duplicatesRemoved += 1;
  }

  stats.keepersUpdated += 1;
  stats.groupsMerged += 1;
  return { merged: true, affectedScopes: Array.from(affectedScopes) };
};

const run = async () => {
  const batchSize = parsePositiveInt(
    readArg(['--batch-size', '--batchSize']) || process.env.CONVERSATION_THREAD_REPAIR_BATCH_SIZE,
    500
  );
  const limit = parsePositiveInt(readArg(['--limit']), 0);
  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const phone = readArg(['--phone']);
  const keepStrategy = String(readArg(['--keep']) || 'latest').trim().toLowerCase();
  const companyWide = hasFlag(['--company-wide', '--companyWide']);
  const apply = hasFlag(['--apply']);
  const dryRun = !apply || hasFlag(['--dry-run', '--dryRun']);

  await connectDB();

  const scopeFilters = buildScopeFilter({ companyId, userId });
  const phoneFilter = buildConversationPhoneLookupFilter(phone);
  const filters = phoneFilter
    ? {
        $and: [
          scopeFilters,
          phoneFilter
        ].filter((item) => item && Object.keys(item).length > 0)
      }
    : scopeFilters;
  const conversationSort =
    keepStrategy === 'earliest'
      ? { createdAt: 1, updatedAt: 1, lastMessageTime: 1, _id: 1 }
      : { lastMessageTime: -1, updatedAt: -1, createdAt: -1, _id: -1 };
  const cursor = Conversation.find(filters)
    .select(
      '_id userId companyId contactId contactPhone contactPhoneDigits contactName status assignedTo assignedToId assignedAgent tags priority lastMessageTime lastMessage lastMessageMediaType lastMessageAttachmentName lastMessageAttachmentPages lastMessageFrom lastMessageWhatsappMessageId lastMessageStatus unreadCount notes resolvedAt createdAt updatedAt'
    )
    .sort(conversationSort)
    .lean()
    .cursor({ batchSize });

  const stats = {
    scanned: 0,
    groupsCreated: 0,
    groupsMerged: 0,
    keepersUpdated: 0,
    duplicatesRemoved: 0,
    messagesReassigned: 0,
    broadcastDispatchesReassigned: 0,
    leadActivitiesReassigned: 0,
    leadTasksReassigned: 0,
    contactDocumentsReassigned: 0
  };

  const groupsByKeeperId = new Map();
  const tokenToKeeperId = new Map();
  const touchedScopes = new Set();
  let groupSequence = 0;

  const findGroupIndexesForTokens = (tokens = []) => {
    const keeperIds = [];
    for (const token of tokens) {
      const keeperId = tokenToKeeperId.get(token);
      if (!keeperId) continue;
      if (!keeperIds.includes(keeperId)) keeperIds.push(keeperId);
    }
    return keeperIds;
  };

  const registerGroupTokens = (keeperId) => {
    const group = groupsByKeeperId.get(keeperId);
    if (!group) return;
    for (const token of group.tokens) {
      tokenToKeeperId.set(token, keeperId);
    }
  };

  for await (const conversation of cursor) {
    stats.scanned += 1;
    if (limit > 0 && stats.scanned > limit) break;

    const scopeKey = buildScopeKey(conversation, { companyWide });
    const tokens = getConversationIdentityTokens(conversation).map((token) => `${scopeKey}::${token}`);
    if (!tokens.length) continue;

    const matchedKeeperIds = findGroupIndexesForTokens(tokens);
    if (!matchedKeeperIds.length) {
      const keeperId = String(conversation?._id || '').trim();
      if (!keeperId) continue;

      groupsByKeeperId.set(keeperId, {
        keeper: conversation,
        members: [conversation],
        tokens: new Set(tokens),
        rank: groupSequence += 1,
        scopeKey
      });
      registerGroupTokens(keeperId);
      stats.groupsCreated += 1;
      continue;
    }

    const winnerId = matchedKeeperIds
      .slice()
      .sort((left, right) => (groupsByKeeperId.get(left)?.rank || 0) - (groupsByKeeperId.get(right)?.rank || 0))[0];
    if (!winnerId) continue;

    const winner = groupsByKeeperId.get(winnerId);
    if (!winner) continue;

    winner.keeper = mergeConversationRecords(winner.keeper, conversation);
    if (!winner.members.some((item) => String(item?._id || '') === String(conversation?._id || ''))) {
      winner.members.push(conversation);
    }
    tokens.forEach((token) => winner.tokens.add(token));
    registerGroupTokens(winnerId);

    for (const loserId of matchedKeeperIds) {
      if (loserId === winnerId) continue;
      const loser = groupsByKeeperId.get(loserId);
      if (!loser) continue;

      winner.keeper = mergeConversationRecords(winner.keeper, loser.keeper);
      for (const member of loser.members) {
        if (!winner.members.some((item) => String(item?._id || '') === String(member?._id || ''))) {
          winner.members.push(member);
        }
      }
      for (const token of loser.tokens) {
        winner.tokens.add(token);
      }
      groupsByKeeperId.delete(loserId);
    }

    registerGroupTokens(winnerId);
  }

  for (const group of groupsByKeeperId.values()) {
    if (group.members.length <= 1) continue;
    const result = await mergeGroupIntoKeeper({ group, dryRun, stats });
    if (result?.affectedScopes) {
      result.affectedScopes.forEach((scopeJson) => touchedScopes.add(scopeJson));
    }
  }

  if (!dryRun) {
    for (const scopeJson of touchedScopes) {
      const scope = parseScopeKey(scopeJson);
      await invalidateInboxScope({
        companyId: scope.companyId || '',
        userId: scope.userId || ''
      });
    }
  }

  console.log(
    JSON.stringify(
      {
        event: 'conversation_thread_repair_complete',
        dryRun,
        scanned: stats.scanned,
        groupsCreated: stats.groupsCreated,
        groupsMerged: stats.groupsMerged,
        keepersUpdated: stats.keepersUpdated,
        duplicatesRemoved: stats.duplicatesRemoved,
        messagesReassigned: stats.messagesReassigned,
        broadcastDispatchesReassigned: stats.broadcastDispatchesReassigned,
        leadActivitiesReassigned: stats.leadActivitiesReassigned,
        leadTasksReassigned: stats.leadTasksReassigned,
        contactDocumentsReassigned: stats.contactDocumentsReassigned
      },
      null,
      2
    )
  );

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Conversation thread repair failed:', error);
  try {
    await mongoose.connection.close();
  } catch {
    // ignore close failures
  }
  process.exitCode = 1;
});
