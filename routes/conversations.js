const express = require('express');
const router = express.Router();
const conversationController = require('../controllers/conversationController');
const mongoose = require('mongoose');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const Contact = require('../models/Contact');
const User = require('../models/User');
const LeadTask = require('../models/LeadTask');
const { upsertConversationSummary } = require('../services/conversationSummaryService');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');
const {
  buildThreadPageResponse,
  buildMessageCursorFilter,
  decodeMessageCursor,
  encodeMessageCursor,
  formatThreadPageResponse,
  normalizePageLimit
} = require('../utils/threadPagination');
const {
  CACHE_TTL_SECONDS,
  getInboxScopeVariants,
  getOrSetCachedJson,
  invalidateInboxConversation
} = require('../utils/teamInboxCache');
const {
  resolveRelatedConversationIds
} = require('../utils/conversationThreadLookup');
const {
  normalizeRole,
  isTenantWideRole
} = require('../utils/accessControl');
const { createRedisRateLimiter } = require('../middleware/redisRateLimit');

const setInboxNoCacheHeaders = (res) => {
  res.set({
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0, private',
    Pragma: 'no-cache',
    Expires: '0'
  });
};

const inboxListRateLimit = createRedisRateLimiter({
  namespace: 'inbox-conversation-list',
  windowMs: 60_000,
  max: 120,
  message: 'Conversation list is being refreshed too quickly.'
});

const inboxThreadRateLimit = createRedisRateLimiter({
  namespace: 'inbox-thread-read',
  windowMs: 60_000,
  max: 240,
  message: 'Thread history is being requested too quickly.'
});

const TEAM_INBOX_MUTATION_FIELDS = [
  '_id',
  'conversationId',
  'userId',
  'companyId',
  'contactId',
  'contactPhone',
  'contactName',
  'channel',
  'status',
  'leadStatus',
  'assignedTo',
  'assignedToId',
  'assignedAgent',
  'assignedToName',
  'assignedAgentName',
  'assigneeName',
  'ownerName',
  'lastMessageTime',
  'lastMessage',
  'lastMessageMediaType',
  'lastMessageAttachmentName',
  'lastMessageAttachmentPages',
  'lastMessageFrom',
  'lastMessageWhatsappMessageId',
  'lastMessageStatus',
  'unreadCount',
  'important',
  'followupAt',
  'notes',
  'internalNotes',
  'resolvedAt',
  'createdAt',
  'updatedAt'
].join(' ');

const TEAM_INBOX_CONTACT_SNAPSHOT_FIELDS =
  '_id name stage ownerId assignedTo assignedAgent assignedToName assignedAgentName assigneeName ownerName leadScore';

const buildScopedMessageFilters = (req, extra = {}, options = {}) => {
  const normalizedRole = normalizeRole(
    req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role
  );
  const normalizedScope = String(options?.scope || req?.query?.scope || '').trim().toLowerCase();
  const isTeamScope = normalizedScope === 'team';
  const normalizedCompanyId = String(req?.companyId || req?.user?.companyId || '').trim();
  const filters = {
    ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
    ...extra
  };

  if (!isTenantWideRole(normalizedRole) && !isTeamScope) {
    filters.userId = req.user.id;
  }

  return filters;
};

const toCleanString = (value = '') => String(value || '').trim();
const toObjectIdIfValid = (value) => (mongoose.Types.ObjectId.isValid(String(value || '').trim()) ? String(value).trim() : null);
const resolveAssigneeDisplayName = async (assignedTo = '') => {
  const normalizedAssignedTo = toCleanString(assignedTo);
  if (!normalizedAssignedTo) return '';

  try {
    const user = await User.findById(normalizedAssignedTo).select('_id name displayName fullName email').lean();
    return toCleanString(user?.name || user?.displayName || user?.fullName || user?.email || normalizedAssignedTo);
  } catch {
    return normalizedAssignedTo;
  }
};

const buildAssigneeNamePatch = async (assignedTo = '') => {
  const normalizedAssignedTo = toCleanString(assignedTo);
  if (!normalizedAssignedTo) {
    return {
      assignedToName: null,
      assignedAgentName: null,
      assigneeName: null,
      ownerName: null
    };
  }

  const displayName = await resolveAssigneeDisplayName(normalizedAssignedTo);
  return {
    assignedToName: displayName || normalizedAssignedTo,
    assignedAgentName: displayName || normalizedAssignedTo,
    assigneeName: displayName || normalizedAssignedTo,
    ownerName: displayName || normalizedAssignedTo
  };
};

const attachConversationContactSnapshot = async (conversation = {}, req = {}) => {
  const contactId = toCleanString(conversation?.contactId);
  if (!contactId || !mongoose.Types.ObjectId.isValid(contactId)) {
    return conversation;
  }

  const contact = await Contact.findOne({
    _id: contactId,
    ...(req?.companyId ? { companyId: req.companyId } : {})
  })
    .select(TEAM_INBOX_CONTACT_SNAPSHOT_FIELDS)
    .lean();

  if (!contact) {
    return conversation;
  }

  return {
    ...conversation,
    contactId: contact
  };
};

const invalidateConversationForUsers = async ({
  companyId = '',
  conversationId = '',
  userIds = []
} = {}) => {
  const normalizedUserIds = Array.from(
    new Set(
      (Array.isArray(userIds) ? userIds : [userIds])
        .map((userId) => toCleanString(userId))
        .filter(Boolean)
    )
  );

  if (!normalizedUserIds.length) return;

  await Promise.all(
    normalizedUserIds.map((userId) =>
      invalidateInboxConversation({
        companyId,
        userId,
        conversationId
      })
    )
  );
};

const getInboxRole = (req) =>
  normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);

const isAgentWorkspaceUser = (req) => !isTenantWideRole(getInboxRole(req));

const buildConversationOwnershipFilter = (req) => {
  const userId = toCleanString(req?.user?.id);
  if (!userId) return {};
  return {
    $or: [
      { userId },
      { assignedTo: userId },
      { assignedToId: userId },
      { assignedAgent: userId }
    ]
  };
};

const buildConversationAccessFilter = (req, extra = {}) => {
  const companyId = toCleanString(req?.companyId || req?.user?.companyId);
  const normalizedView = toCleanString(req?.query?.view || req?.query?.inboxView || extra?.view).toLowerCase();
  const normalizedStatus = toCleanString(req?.query?.status || extra?.status).toLowerCase();
  const normalizedAssignedTo = toCleanString(req?.query?.assignedTo || extra?.assignedTo);
  const baseFilters = {
    ...(companyId ? { companyId } : {}),
    ...extra
  };
  delete baseFilters.view;
  delete baseFilters.inboxView;
  delete baseFilters.status;
  delete baseFilters.assignedTo;

  const view = normalizedView || 'all';
  const ownershipFilter = isAgentWorkspaceUser(req) ? buildConversationOwnershipFilter(req) : {};
  const activeStatusFilter = { status: { $in: ['active', 'pending'] } };
  const closedFilter = { $or: [{ status: { $in: ['resolved', 'archived'] } }, { leadStatus: 'closed' }] };

  const viewFilter = (() => {
    if (isAgentWorkspaceUser(req)) {
      if (view === 'closed') return closedFilter;
      if (view === 'followups') {
        return { followupAt: { $ne: null } };
      }
      if (view === 'important') return { important: true };
      return activeStatusFilter;
    }

    switch (view) {
      case 'unassigned':
        return {
          $or: [
            { assignedTo: { $in: [null, ''] } },
            { assignedTo: { $exists: false } },
            { assignedToId: null },
            { assignedToId: { $exists: false } }
          ]
        };
      case 'assigned':
        return { assignedTo: { $nin: [null, ''] } };
      case 'my':
        return ownershipFilter;
      case 'closed':
        return closedFilter;
      case 'important':
        return { important: true };
      case 'followups':
        return { followupAt: { $ne: null } };
      case 'team':
        return activeStatusFilter;
      case 'all':
      default:
        return {};
    }
  })();

  const filters = {
    ...baseFilters
  };

  if (normalizedStatus) {
    filters.status = normalizedStatus;
  }
  if (normalizedAssignedTo) {
    filters.assignedTo = normalizedAssignedTo;
  }

  if (isAgentWorkspaceUser(req)) {
    const agentScope = ownershipFilter;
    return {
      ...filters,
      ...(Object.keys(agentScope).length ? { $and: [agentScope, viewFilter].filter(Boolean) } : {}),
      ...(Object.keys(agentScope).length === 0 ? viewFilter : {})
    };
  }

  return {
    ...filters,
    ...viewFilter
  };
};

const buildConversationRecordFilter = (req, conversationId) => {
  const companyId = toCleanString(req?.companyId || req?.user?.companyId);
  const baseFilter = {
    _id: conversationId,
    ...(companyId ? { companyId } : {})
  };

  if (isAgentWorkspaceUser(req)) {
    return {
      ...baseFilter,
      ...buildConversationOwnershipFilter(req)
    };
  }

  return baseFilter;
};

const loadConversationRecordForMutation = async (req, conversationId) => {
  const filter = buildConversationRecordFilter(req, conversationId);
  const conversation = await Conversation.findOne(filter).select([
    '_id',
    'conversationId',
    'userId',
    'companyId',
    'contactId',
    'contactPhone',
    'contactName',
    'channel',
    'status',
    'leadStatus',
    'assignedTo',
    'assignedToId',
    'assignedAgent',
    'assignedToName',
    'assignedAgentName',
    'assigneeName',
    'ownerName',
    'lastMessageTime',
    'lastMessage',
    'lastMessageMediaType',
    'lastMessageAttachmentName',
    'lastMessageAttachmentPages',
    'lastMessageFrom',
    'lastMessageWhatsappMessageId',
    'lastMessageStatus',
    'unreadCount',
    'important',
    'followupAt',
    'notes',
    'internalNotes',
    'resolvedAt',
    'createdAt',
    'updatedAt'
  ].join(' ')).lean();
  if (conversation) {
    return {
      conversation: await attachConversationContactSnapshot(conversation, req),
      source: 'conversation'
    };
  }

  const summaryConversation = await ConversationSummary.findOne(filter).select([
    '_id',
    'conversationId',
    'userId',
    'companyId',
    'contactId',
    'contactPhone',
    'contactName',
    'channel',
    'status',
    'leadStatus',
    'assignedTo',
    'assignedToId',
    'assignedAgent',
    'assignedToName',
    'assignedAgentName',
    'assigneeName',
    'ownerName',
    'lastMessageTime',
    'lastMessage',
    'lastMessageMediaType',
    'lastMessageAttachmentName',
    'lastMessageAttachmentPages',
    'lastMessageFrom',
    'lastMessageWhatsappMessageId',
    'lastMessageStatus',
    'unreadCount',
    'important',
    'followupAt',
    'notes',
    'internalNotes',
    'resolvedAt',
    'createdAt',
    'updatedAt'
  ].join(' ')).lean();
  if (summaryConversation) {
    return {
      conversation: await attachConversationContactSnapshot(summaryConversation, req),
      source: 'summary'
    };
  }

  return null;
};

const normalizeLeadStatus = (value = '') => {
  const normalized = toCleanString(value).toLowerCase();
  return ['new_lead', 'interested', 'follow_up', 'proposal_sent', 'converted', 'closed'].includes(normalized)
    ? normalized
    : '';
};

const emitInboxRealtimeUpdate = (req, payload = {}, targetUserId = '') => {
  const sendToUser = req?.app?.locals?.sendToUser;
  const companyId = toCleanString(req?.companyId || req?.user?.companyId);
  const normalizedPayload = {
    type: payload?.type || 'inbox_conversation_updated',
    scope: 'crm',
    timestamp: new Date().toISOString(),
    companyId,
    ...payload
  };

  if (typeof sendToUser === 'function') {
    const normalizedTargetUserId = toCleanString(targetUserId);
    if (normalizedTargetUserId) {
      sendToUser(normalizedTargetUserId, normalizedPayload);
    }
    if (companyId) {
      sendToUser(toCleanString(req?.user?.id || ''), normalizedPayload);
    }
  }
};

const syncConversationAndContact = async ({ req, conversation, patch = {} }) => {
  if (!conversation?._id) return conversation;

  const contactId = toCleanString(conversation?.contactId);
  let updatedContact = null;

  if (contactId) {
    try {
      const contact = await Contact.findOne({
        _id: contactId,
        ...(req.companyId ? { companyId: req.companyId } : {})
      });

      if (contact) {
        if (patch.assignedTo !== undefined) {
          contact.assignedTo = patch.assignedTo || null;
          contact.assignedAgent = patch.assignedTo || null;
          contact.ownerId = patch.assignedTo || null;
        }
        if (patch.leadStatus !== undefined) {
          contact.leadStatus = patch.leadStatus || 'new_lead';
          contact.status = patch.leadStatus || contact.status || 'new';
        }
        if (patch.followupAt !== undefined) {
          contact.followupDate = patch.followupAt || null;
        }
        if (Array.isArray(patch.tags)) {
          contact.tags = patch.tags;
        }
        if (patch.notes !== undefined) {
          contact.notes = patch.notes || '';
        }
        if (Array.isArray(patch.internalNotes)) {
          contact.internalNotes = patch.internalNotes;
        }
        await contact.save();
        updatedContact = contact;
      }
    } catch (contactError) {
      console.error('Conversation contact sync skipped:', contactError?.message || contactError);
    }
  }

  const nextPatch = {
    ...patch,
    ...(patch.assignedTo !== undefined ? { assignedTo: patch.assignedTo || null, assignedAgent: patch.assignedTo || null } : {}),
    ...(patch.leadStatus !== undefined ? { leadStatus: patch.leadStatus || 'new_lead' } : {}),
    ...(patch.followupAt !== undefined ? { followupAt: patch.followupAt || null } : {}),
    ...(patch.important !== undefined ? { important: Boolean(patch.important) } : {}),
    ...(patch.status !== undefined ? { status: patch.status } : {})
  };

  const mutationFilter = { _id: conversation._id, ...(req.companyId ? { companyId: req.companyId } : {}) };
  const mutationPatch = {
    ...(nextPatch.assignedTo !== undefined
      ? {
          assignedTo: nextPatch.assignedTo,
          assignedAgent: nextPatch.assignedAgent || nextPatch.assignedTo,
          assignedToId: toObjectIdIfValid(nextPatch.assignedTo) || null,
          ...(await buildAssigneeNamePatch(nextPatch.assignedTo))
        }
      : {}),
    ...(nextPatch.leadStatus !== undefined ? { leadStatus: nextPatch.leadStatus } : {}),
    ...(nextPatch.followupAt !== undefined ? { followupAt: nextPatch.followupAt } : {}),
    ...(nextPatch.important !== undefined ? { important: nextPatch.important } : {}),
    ...(nextPatch.status !== undefined
      ? {
          status: nextPatch.status,
          ...(nextPatch.status === 'resolved'
            ? { resolvedAt: new Date() }
            : nextPatch.status === 'active'
              ? { resolvedAt: null }
              : {})
        }
      : {}),
    ...(nextPatch.notes !== undefined ? { notes: nextPatch.notes } : {}),
    ...(nextPatch.internalNotes !== undefined ? { internalNotes: nextPatch.internalNotes } : {}),
    ...(Array.isArray(nextPatch.tags) ? { tags: nextPatch.tags } : {})
  };

  let updatedConversation = null;
  let updateError = null;

  try {
    updatedConversation = await Conversation.findOneAndUpdate(mutationFilter, mutationPatch, {
      new: true,
      runValidators: true
    }).lean();
  } catch (error) {
    updateError = error;
  }

  if (!updatedConversation) {
    try {
      const conversationUpdateResult = await Conversation.updateOne(mutationFilter, mutationPatch, {
        runValidators: false
      });
      if (conversationUpdateResult?.matchedCount > 0 || conversationUpdateResult?.modifiedCount > 0) {
        updatedConversation = await Conversation.findOne(mutationFilter).lean();
      }
    } catch (error) {
      updateError = error;
    }
  }

  if (!updatedConversation) {
    try {
      updatedConversation = await ConversationSummary.findOneAndUpdate(mutationFilter, mutationPatch, {
        new: true,
        runValidators: true
      }).lean();
    } catch (error) {
      updateError = error;
    }
  }

  if (!updatedConversation) {
    try {
      const summaryUpdateResult = await ConversationSummary.updateOne(mutationFilter, mutationPatch, {
        runValidators: false
      });
      if (summaryUpdateResult?.matchedCount > 0 || summaryUpdateResult?.modifiedCount > 0) {
        updatedConversation = await ConversationSummary.findOne(mutationFilter).lean();
      }
    } catch (error) {
      updateError = error;
    }
  }

  if (!updatedConversation) {
    throw updateError || new Error('Failed to update conversation');
  }

  try {
    await upsertConversationSummary(updatedConversation);
  } catch (summaryError) {
    console.error('Conversation summary sync skipped:', summaryError?.message || summaryError);
  }

  return {
    conversation: updatedConversation,
    contact: updatedContact
  };
};

const applyConversationMutationSafely = async ({ req, conversation, patch = {} }) => {
  if (!conversation?._id) {
    throw new Error('Failed to update conversation');
  }

  const mutationFilter = {
    _id: conversation._id,
    ...(req.companyId ? { companyId: req.companyId } : {})
  };

  const mutationPatch = {
    ...(patch.assignedTo !== undefined
      ? {
          assignedTo: patch.assignedTo || null,
          assignedAgent: patch.assignedAgent || patch.assignedTo || null,
          assignedToId: toObjectIdIfValid(patch.assignedTo) || null
        }
      : {}),
    ...(patch.assignedTo !== undefined ? await buildAssigneeNamePatch(patch.assignedTo) : {}),
    ...(patch.important !== undefined ? { important: Boolean(patch.important) } : {}),
    ...(patch.status !== undefined
      ? {
          status: patch.status,
          ...(patch.status === 'resolved'
            ? { resolvedAt: new Date() }
            : patch.status === 'active'
              ? { resolvedAt: null }
              : {})
        }
      : {}),
    ...(patch.leadStatus !== undefined ? { leadStatus: patch.leadStatus || 'new_lead' } : {}),
    ...(patch.followupAt !== undefined ? { followupAt: patch.followupAt || null } : {}),
    updatedAt: new Date()
  };

  let updatedConversation = null;

  try {
    await Conversation.updateOne(mutationFilter, { $set: mutationPatch }, { runValidators: false });
  } catch (error) {
    console.error('Conversation direct update failed:', error?.message || error);
  }

  try {
    await ConversationSummary.updateOne(mutationFilter, { $set: mutationPatch }, { runValidators: false });
  } catch (error) {
    console.error('Conversation summary direct update failed:', error?.message || error);
  }

  try {
    updatedConversation = await Conversation.findOne(mutationFilter).lean();
  } catch (error) {
    console.error('Conversation refetch failed:', error?.message || error);
  }

  if (!updatedConversation) {
    try {
      updatedConversation = await ConversationSummary.findOne(mutationFilter).lean();
    } catch (error) {
      console.error('Conversation summary refetch failed:', error?.message || error);
    }
  }

  if (!updatedConversation) {
    throw new Error('Failed to update conversation');
  }

  try {
    await upsertConversationSummary(updatedConversation);
  } catch (summaryError) {
    console.error('Conversation summary sync skipped:', summaryError?.message || summaryError);
  }

  return { conversation: updatedConversation };
};

router.use(auth);
router.use(
  requireTenantPolicy({
    requiredFeatures: ['teamInbox', 'contacts'],
    auditEvent: 'conversation_policy'
  })
);

// Get all conversations with optional filters
router.get('/', inboxListRateLimit, (req, res) => {
  setInboxNoCacheHeaders(res);
  return conversationController.getConversations(req, res);
});

// Get all contacts with optional filters
router.get('/contacts', (req, res) => conversationController.getContacts(req, res));

// Get unique contacts from conversations (for broadcast)
router.get('/contacts/unique', (req, res) => conversationController.getConversationContacts(req, res));

// Get unread conversation count for hub badges
router.get('/unread-count', (req, res) => conversationController.getUnreadConversationCount(req, res));

// Get inbox overview counts for Team Inbox badges
router.get('/overview', (req, res) => conversationController.getInboxOverview(req, res));

// Create a new contact
router.post('/contacts', (req, res) => conversationController.createContact(req, res));

// Update an existing contact
router.put('/contacts/:id', (req, res) => conversationController.updateContact(req, res));

// Delete a contact
router.delete('/contacts/:id', (req, res) => conversationController.deleteContact(req, res));

// Get a single contact by ID
router.get('/contacts/:id', (req, res) => conversationController.getContactById(req, res));

// Delete all conversations
router.delete('/delete-all', (req, res) => conversationController.deleteAllConversations(req, res));

// Delete selected conversations
router.delete('/delete-selected', (req, res) => conversationController.deleteSelectedConversations(req, res));

router.post('/bulk-assign', async (req, res) => {
  try {
    const normalizedRole = normalizeRole(
      req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role
    );
    if (!isTenantWideRole(normalizedRole)) {
      return res.status(403).json({
        success: false,
        error: 'Only admin workspace users can bulk assign conversations'
      });
    }

    const conversationIds = Array.isArray(req.body?.conversationIds)
      ? Array.from(
          new Set(
            req.body.conversationIds
              .map((conversationId) => toCleanString(conversationId))
              .filter((conversationId) => conversationId && mongoose.Types.ObjectId.isValid(conversationId))
          )
        )
      : [];
    const nextAssignedTo = toCleanString(req.body?.assignedTo || req.body?.assignedAgent);

    if (conversationIds.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Conversation IDs array is required'
      });
    }

    if (!nextAssignedTo) {
      return res.status(400).json({
        success: false,
        error: 'assignedTo is required'
      });
    }

    const updatedConversations = [];
    const missingConversationIds = [];
    const assigneeNamePatch = await buildAssigneeNamePatch(nextAssignedTo);

    for (const conversationId of conversationIds) {
      const lookup = await loadConversationRecordForMutation(req, conversationId);
      const conversation = lookup?.conversation || null;
      if (!conversation) {
        missingConversationIds.push(conversationId);
        continue;
      }

      const result = await syncConversationAndContact({
        req,
        conversation,
        patch: {
          assignedTo: nextAssignedTo,
          assignedAgent: nextAssignedTo,
          assignedToId: toObjectIdIfValid(nextAssignedTo) || null,
          ...assigneeNamePatch
        }
      });

      if (result?.conversation) {
        updatedConversations.push(result.conversation);
      }

      emitInboxRealtimeUpdate(
        req,
        {
          type: 'inbox_assignment_updated',
          conversationId,
          assignedTo: nextAssignedTo,
          contactId: toCleanString(conversation.contactId),
          conversation: result?.conversation || null
        },
        nextAssignedTo
      );

      await invalidateConversationForUsers({
        companyId: req.companyId || '',
        conversationId,
        userIds: [
          req.user?.id,
          nextAssignedTo,
          conversation?.assignedTo,
          conversation?.assignedToId,
          conversation?.assignedAgent
        ]
      });
    }

    return res.json({
      success: true,
      data: {
        assignedTo: nextAssignedTo,
        updatedCount: updatedConversations.length,
        missingCount: missingConversationIds.length,
        missingConversationIds,
        conversations: updatedConversations
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

// Get paged messages for a conversation (compatibility path used by Team Inbox clients)
router.get('/:id/messages', inboxThreadRateLimit, async (req, res) => {
  try {
    setInboxNoCacheHeaders(res);
    const conversationId = String(req.params.id || '').trim();
    if (!conversationId) {
      return res.status(400).json({
        success: false,
        error: 'conversationId is required'
      });
    }

    const limit = normalizePageLimit(req.query?.limit);
    const cursor = decodeMessageCursor(req.query?.cursor);
    const normalizedCompanyId = String(req.companyId || req.user?.companyId || '').trim();
    const scopeVariants = getInboxScopeVariants({
      companyId: normalizedCompanyId,
      userId: req.user?.id || ''
    });
    const scope = scopeVariants[scopeVariants.length - 1] || scopeVariants[0] || '';
    const queryCursor = cursor ? buildMessageCursorFilter(cursor) : {};
    const conversationLookup = await loadConversationRecordForMutation(req, conversationId);
    const conversationRecord = conversationLookup?.conversation || null;
    const loadMessages = async (filters) =>
      Message.find(filters)
        .select(
          '_id conversationId sender senderRole senderName senderId text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment attachments deliveredTo readBy replyTo replyToMessageId errorMessage'
        )
        .sort({ timestamp: -1, _id: -1 })
        .limit(limit + 1)
        .lean();

    let relatedConversationIds = [conversationId];
    try {
      const resolvedConversationIds = await resolveRelatedConversationIds({
        Conversation,
        ConversationSummary,
        req,
        conversation: conversationRecord || { _id: conversationId },
        includeAllIdentityMatches: true
      });
      if (Array.isArray(resolvedConversationIds) && resolvedConversationIds.length > 0) {
        relatedConversationIds = resolvedConversationIds;
      }
    } catch (error) {
      console.error('Failed to resolve related inbox conversations:', error?.message || error);
    }
    const familyScopeKey = relatedConversationIds
      .map((id) => String(id || '').trim())
      .filter(Boolean)
      .sort()
      .join('|');
    const threadScope = scope ? `${scope}:${conversationId}:${familyScopeKey}` : '';

    const loadScopedMessages = async () => {
      const scopedMessages = await loadMessages({
        ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
        ...(relatedConversationIds.length > 0
          ? { conversationId: { $in: relatedConversationIds } }
          : { conversationId }),
        ...queryCursor
      });

      if (scopedMessages.length > 0 || !normalizedCompanyId) {
        return scopedMessages;
      }

      const companyWideMessages = await loadMessages({
        ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
        ...(relatedConversationIds.length > 0
          ? { conversationId: { $in: relatedConversationIds } }
          : { conversationId }),
        ...queryCursor
      });

      if (companyWideMessages.length > 0) {
        return companyWideMessages;
      }

      return loadMessages({
        ...(relatedConversationIds.length > 0
          ? { conversationId: { $in: relatedConversationIds } }
          : { conversationId }),
        ...queryCursor,
        $or: [
          { companyId: { $exists: false } },
          { companyId: null }
        ]
      });
    };

    const hydrateReplyContext = async (messages = []) => {
      if (!Array.isArray(messages) || !messages.some((message) => String(message?.replyTo || '').trim())) {
        return messages;
      }

      try {
        await Message.populate(messages, {
          path: 'replyTo',
          select: '_id text sender senderRole senderName senderId whatsappMessageId mediaType mediaCaption timestamp attachment'
        });
      } catch (error) {
        console.error('Failed to populate reply context for inbox messages:', error?.message || error);
      }

      return messages;
    };

    const cachedResponse = threadScope
      ? await getOrSetCachedJson({
          namespace: 'messages',
          scope: threadScope,
          versionGroup: 'thread',
          keyParts: [String(limit), String(req.query?.cursor || '').trim()],
          ttlSeconds: CACHE_TTL_SECONDS.messages,
          loader: async () => {
            const messages = await hydrateReplyContext(await loadScopedMessages());

            return buildThreadPageResponse({
              documents: messages,
              limit,
              encodeCursor: encodeMessageCursor
            });
          }
        })
      : null;

    if (cachedResponse) {
      const cachedPage = formatThreadPageResponse(cachedResponse);
      if (cachedPage.messages.length > 0) {
        return res.json(cachedPage);
      }

      await invalidateInboxConversation({
        companyId: req.companyId || '',
        userId: req.user?.id || '',
        conversationId
      });
    }

    const messages = await hydrateReplyContext(await loadScopedMessages());

    return res.json(
      buildThreadPageResponse({
        documents: messages,
        limit,
        encodeCursor: encodeMessageCursor
      })
    );
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Delete a single conversation (must come after specific routes)
router.get('/:id', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid conversation id'
      });
    }

    const lookup = await loadConversationRecordForMutation(req, conversationId);
    const conversation = lookup?.conversation || null;

    if (!conversation) {
      return res.status(404).json({
        success: false,
        error: 'Conversation not found'
      });
    }

    return res.json({ success: true, data: conversation });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

router.patch('/:id/assign', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    const nextAssignedTo = toCleanString(req.body?.assignedTo || req.body?.assignedAgent);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }
    if (!nextAssignedTo) {
      return res.status(400).json({ success: false, error: 'assignedTo is required' });
    }

    const lookup = await loadConversationRecordForMutation(req, conversationId);
    const conversation = lookup?.conversation || null;
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const assigneeNamePatch = await buildAssigneeNamePatch(nextAssignedTo);
    const result = await syncConversationAndContact({
      req,
      conversation,
      patch: {
        assignedTo: nextAssignedTo,
        assignedAgent: nextAssignedTo,
        assignedToId: toObjectIdIfValid(nextAssignedTo) || null,
        ...assigneeNamePatch
      }
    });

    emitInboxRealtimeUpdate(
      req,
      {
        type: 'inbox_assignment_updated',
        conversationId,
        assignedTo: nextAssignedTo,
        contactId: toCleanString(conversation.contactId),
        conversation: result?.conversation || null
      },
      nextAssignedTo
    );

    await invalidateConversationForUsers({
      companyId: req.companyId || '',
      conversationId,
      userIds: [
        req.user?.id,
        nextAssignedTo,
        conversation?.assignedTo,
        conversation?.assignedToId,
        conversation?.assignedAgent
      ]
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/:id/lead-status', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    const leadStatus = normalizeLeadStatus(req.body?.leadStatus || req.body?.status);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }
    if (!leadStatus) {
      return res.status(400).json({ success: false, error: 'Invalid lead status' });
    }

    const conversation = await Conversation.findOne(buildConversationRecordFilter(req, conversationId))
      .select(TEAM_INBOX_MUTATION_FIELDS)
      .lean();
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const patch = {
      leadStatus,
      status: leadStatus === 'closed' ? 'resolved' : conversation.status,
      followupAt: undefined
    };

    if (leadStatus === 'follow_up' && req.body?.followupAt !== undefined) {
      const parsedFollowup = new Date(req.body.followupAt);
      if (Number.isNaN(parsedFollowup.getTime())) {
        return res.status(400).json({ success: false, error: 'Invalid follow-up date' });
      }
      patch.followupAt = parsedFollowup;
    }

    const result = await syncConversationAndContact({ req, conversation, patch });
    emitInboxRealtimeUpdate(req, {
      type: 'inbox_lead_status_updated',
      conversationId,
      leadStatus,
      conversation: result?.conversation || null
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/:id/close', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }

    const conversation = await Conversation.findOne(buildConversationRecordFilter(req, conversationId))
      .select(TEAM_INBOX_MUTATION_FIELDS)
      .lean();
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const result = await applyConversationMutationSafely({
      req,
      conversation,
      patch: {
        status: 'resolved',
        leadStatus: 'closed',
        followupAt: null
      }
    });

    emitInboxRealtimeUpdate(req, {
      type: 'inbox_conversation_closed',
      conversationId,
      conversation: result?.conversation || null
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/:id/reopen', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }

    const conversation = await Conversation.findOne(buildConversationRecordFilter(req, conversationId))
      .select(TEAM_INBOX_MUTATION_FIELDS)
      .lean();
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const result = await syncConversationAndContact({
      req,
      conversation,
      patch: {
        status: 'active',
        leadStatus: conversation.leadStatus === 'closed' ? 'new_lead' : conversation.leadStatus
      }
    });

    emitInboxRealtimeUpdate(req, {
      type: 'inbox_conversation_reopened',
      conversationId,
      conversation: result?.conversation || null
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/:id/important', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }

    const conversation = await Conversation.findOne(buildConversationRecordFilter(req, conversationId))
      .select(TEAM_INBOX_MUTATION_FIELDS)
      .lean();
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const important = Boolean(req.body?.important);
    const result = await applyConversationMutationSafely({
      req,
      conversation,
      patch: { important }
    });

    emitInboxRealtimeUpdate(req, {
      type: 'inbox_conversation_flagged',
      conversationId,
      important,
      conversation: result?.conversation || null
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/notes', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    const noteText = toCleanString(req.body?.text || req.body?.note);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }
    if (!noteText) {
      return res.status(400).json({ success: false, error: 'Note text is required' });
    }

    const conversation = await Conversation.findOne(buildConversationRecordFilter(req, conversationId))
      .select(TEAM_INBOX_MUTATION_FIELDS)
      .lean();
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const nextNotes = [
      ...(Array.isArray(conversation.internalNotes) ? conversation.internalNotes : []),
      {
        text: noteText,
        createdBy: toCleanString(req.user?.id),
        createdAt: new Date()
      }
    ];

    const result = await syncConversationAndContact({
      req,
      conversation,
      patch: {
        notes: noteText,
        internalNotes: nextNotes
      }
    });

    emitInboxRealtimeUpdate(req, {
      type: 'inbox_internal_note_added',
      conversationId,
      conversation: result?.conversation || null
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/followups', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    const followupAtValue = req.body?.followupAt || req.body?.dueDate || req.body?.dueAt;
    const followupAt = followupAtValue ? new Date(followupAtValue) : null;
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }
    if (followupAtValue && Number.isNaN(followupAt.getTime())) {
      return res.status(400).json({ success: false, error: 'Invalid follow-up date' });
    }

    const conversation = await Conversation.findOne(buildConversationRecordFilter(req, conversationId));
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const result = await syncConversationAndContact({
      req,
      conversation,
      patch: {
        followupAt
      }
    });

    const contactId = toCleanString(conversation.contactId);
    if (contactId && followupAt) {
      await LeadTask.create({
        userId: req.user.id,
        companyId: req.companyId || null,
        contactId,
        conversationId: conversation._id,
        title: toCleanString(req.body?.title) || 'Follow up with customer',
        description: toCleanString(req.body?.description),
        taskType: 'follow_up',
        dueAt: followupAt,
        dueDate: followupAt,
        priority: toCleanString(req.body?.priority).toLowerCase() || 'medium',
        status: 'pending',
        assignedTo: toCleanString(req.body?.assignedTo) || toCleanString(req.user?.id),
        createdBy: toCleanString(req.user?.id)
      });
    }

    emitInboxRealtimeUpdate(req, {
      type: 'inbox_followup_updated',
      conversationId,
      conversation: result?.conversation || null
    });

    return res.json({ success: true, data: result?.conversation || conversation });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/tasks', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    const title = toCleanString(req.body?.title);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({ success: false, error: 'Invalid conversation id' });
    }
    if (!title) {
      return res.status(400).json({ success: false, error: 'Task title is required' });
    }

    const conversation = await Conversation.findOne(
      buildConversationAccessFilter(req, { _id: conversationId })
    )
      .select(['_id', 'contactId'].join(' '))
      .lean();
    if (!conversation) {
      return res.status(404).json({ success: false, error: 'Conversation not found' });
    }

    const dueDateValue = req.body?.dueDate || req.body?.dueAt;
    const dueDate = dueDateValue ? new Date(dueDateValue) : null;
    if (dueDateValue && Number.isNaN(dueDate.getTime())) {
      return res.status(400).json({ success: false, error: 'Invalid due date' });
    }

    const task = await LeadTask.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId: conversation.contactId,
      conversationId: conversation._id,
      title,
      description: toCleanString(req.body?.description),
      taskType: toCleanString(req.body?.taskType).toLowerCase() || 'follow_up',
      dueAt: dueDate,
      dueDate,
      reminderAt: req.body?.reminderAt ? new Date(req.body.reminderAt) : null,
      priority: toCleanString(req.body?.priority).toLowerCase() || 'medium',
      status: 'pending',
      assignedTo: toCleanString(req.body?.assignedTo) || toCleanString(req.user?.id),
      createdBy: toCleanString(req.user?.id)
    });

    emitInboxRealtimeUpdate(req, {
      type: 'inbox_task_created',
      conversationId,
      taskId: String(task?._id || ''),
      conversation: {
        _id: conversation._id,
        contactId: conversation.contactId
      }
    });

    return res.status(201).json({ success: true, data: task });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.put('/:id/read', async (req, res) => {
  try {
    const conversationId = toCleanString(req.params.id);
    if (!conversationId || !mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid conversation id'
      });
    }

    const lookup = await loadConversationRecordForMutation(req, conversationId);
    const conversation = lookup?.conversation || null;
    if (!conversation) {
      return res.status(404).json({
        success: false,
        error: 'Conversation not found'
      });
    }

    let relatedConversationIds = [conversationId];
    try {
      const resolvedConversationIds = await resolveRelatedConversationIds({
        Conversation,
        ConversationSummary,
        req,
        conversation
      });
      if (Array.isArray(resolvedConversationIds) && resolvedConversationIds.length > 0) {
        relatedConversationIds = resolvedConversationIds;
      }
    } catch (error) {
      console.error('Failed to resolve related inbox conversations for read:', error?.message || error);
    }

    const companyId = toCleanString(req.companyId || req.user?.companyId);
    const userId = toCleanString(req.user?.id);
    const conversationUserId = toCleanString(conversation?.userId || userId);

    if (Number(conversation?.unreadCount || 0) > 0 || relatedConversationIds.length > 1) {
      await Message.updateMany(
        {
          conversationId: { $in: relatedConversationIds },
          ...(companyId ? { companyId } : {}),
          ...(conversationUserId ? { userId: conversationUserId } : {}),
          sender: 'contact',
          status: 'received'
        },
        { $set: { status: 'read', updatedAt: new Date() } }
      );

      await Conversation.updateMany(
        {
          _id: { $in: relatedConversationIds },
          ...(companyId ? { companyId } : {}),
          ...(conversationUserId ? { userId: conversationUserId } : {})
        },
        { $set: { unreadCount: 0, updatedAt: new Date() } }
      );

      await ConversationSummary.updateMany(
        {
          conversationId: { $in: relatedConversationIds }
        },
        { $set: { unreadCount: 0, updatedAt: new Date() } }
      );

      const updatedConversation = await Conversation.findOne({
        _id: conversationId,
        ...(companyId ? { companyId } : {}),
        ...(conversationUserId ? { userId: conversationUserId } : {})
      }).lean();

      if (updatedConversation) {
        await upsertConversationSummary({
          ...updatedConversation,
          unreadCount: 0
        });
      }

      await Promise.all(
        relatedConversationIds.map((relatedConversationId) =>
          invalidateInboxConversation({
            companyId,
            userId,
            conversationId: relatedConversationId
          })
        )
      );

      emitInboxRealtimeUpdate(
        req,
        {
          type: 'conversation_read',
          conversationId,
          unreadCount: 0,
          conversation: updatedConversation || {
            ...conversation,
            unreadCount: 0
          },
          relatedConversationIds
        },
        userId
      );

      return res.json({
        success: true,
        data: {
          ...(updatedConversation || conversation),
          unreadCount: 0
        }
      });
    }

    return res.json({
      success: true,
      data: {
        ...conversation,
        unreadCount: 0
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

router.delete('/:id', (req, res) => conversationController.deleteConversation(req, res));

module.exports = router;
