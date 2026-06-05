const mongoose = require('mongoose');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const Contact = require('../models/Contact');
const Message = require('../models/Message');
const {
  normalizeRole,
  isTenantWideRole
} = require('../utils/accessControl');
const {
  CACHE_TTL_SECONDS,
  getInboxScopeVariants,
  getOrSetCachedJson,
  invalidateInboxConversation,
  invalidateInboxScope
} = require('../utils/teamInboxCache');
const {
  getWhatsAppMessagingPolicy,
  applyContactOptIn,
  applyContactOptOut,
  toCleanString
} = require('../services/whatsappOutreach/policy');
const {
  deleteConversationSummary,
  upsertConversationSummaries
} = require('../services/conversationSummaryService');
const { buildInboxSearchPlan } = require('../utils/inboxSearchPlan');
const { buildContactSearchPlan } = require('../utils/contactSearchPlan');

const TEAM_INBOX_CONTACT_LIST_FIELDS =
  '_id name ownerId assignedTo assignedAgent assignedToName assignedAgentName assigneeName ownerName leadScore';
const TEAM_INBOX_CONTACT_DETAIL_FIELDS =
  '_id name phone tags status stage leadStatus assignedAgent followupDate internalNotes leadScore leadScoreBreakdown isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptOutAt lastInboundMessageAt serviceWindowClosesAt';
const CONTACT_LIST_FIELDS =
  '_id name phone email tags stage status leadStatus source ownerId assignedAgent sourceType lastContact lastContactAt nextFollowUpAt followupDate isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata whatsappMarketingWindowStartedAt whatsappMarketingSendCount whatsappMarketingLastSentAt whatsappOptOutAt lastInboundMessageAt serviceWindowClosesAt leadScore internalNotes createdAt updatedAt';

const encodeContactCursor = (contact = {}) => {
  const payload = {
    lastContact: contact?.lastContact || contact?.createdAt || null,
    createdAt: contact?.createdAt || null,
    id: String(contact?._id || '').trim()
  };

  if (!payload.id || !payload.lastContact) {
    return '';
  }

  return Buffer.from(JSON.stringify(payload)).toString('base64url');
};

const decodeContactCursor = (cursor = '') => {
  const normalizedCursor = String(cursor || '').trim();
  if (!normalizedCursor) return null;

  try {
    const decoded = JSON.parse(Buffer.from(normalizedCursor, 'base64url').toString('utf8'));
    const lastContact = new Date(decoded?.lastContact || '');
    const createdAt = new Date(decoded?.createdAt || decoded?.lastContact || '');
    const id = String(decoded?.id || '').trim();
    if (!id || Number.isNaN(lastContact.getTime())) return null;
    return {
      lastContact,
      createdAt: Number.isNaN(createdAt.getTime()) ? null : createdAt,
      id
    };
  } catch {
    const fallbackDate = new Date(normalizedCursor);
    if (Number.isNaN(fallbackDate.getTime())) return null;
    return {
      lastContact: fallbackDate,
      createdAt: null,
      id: ''
    };
  }
};

const buildContactCursorFilter = (cursor) => {
  if (!cursor?.lastContact) return {};

  const cursorLastContact = new Date(cursor.lastContact);
  if (Number.isNaN(cursorLastContact.getTime())) return {};

  const cursorCreatedAt =
    cursor.createdAt instanceof Date && !Number.isNaN(cursor.createdAt.getTime())
      ? cursor.createdAt
      : cursor.createdAt
        ? new Date(cursor.createdAt)
        : null;
  const cursorId = String(cursor.id || '').trim();

  return {
    $or: [
      { lastContact: { $lt: cursorLastContact } },
      cursorCreatedAt
        ? {
            lastContact: cursorLastContact,
            $or: [
              { createdAt: { $lt: cursorCreatedAt } },
              {
                createdAt: cursorCreatedAt,
                _id: cursorId ? { $lt: cursorId } : { $exists: true }
              }
            ]
          }
        : {
            lastContact: cursorLastContact,
            _id: cursorId ? { $lt: cursorId } : { $exists: true }
          }
    ]
  };
};

const TEAM_INBOX_CONVERSATION_FIELDS = [
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
  'assignedAgent',
<<<<<<< Updated upstream
  'assignedToName',
  'assignedAgentName',
  'assigneeName',
  'ownerName',
=======
>>>>>>> Stashed changes
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

const encodeConversationCursor = (conversation = {}) => {
  const payload = {
    lastMessageTime:
      conversation?.lastMessageTime ||
      conversation?.updatedAt ||
      conversation?.createdAt ||
      null,
    id: String(conversation?._id || '').trim()
  };

  if (!payload.id || !payload.lastMessageTime) {
    return '';
  }

  return Buffer.from(JSON.stringify(payload)).toString('base64url');
};

const getConversationSortValue = (conversation = {}) => {
  const timestamp = new Date(
    conversation?.lastMessageTime || conversation?.updatedAt || conversation?.createdAt || 0
  ).valueOf();
  return Number.isFinite(timestamp) ? timestamp : 0;
};

const compareConversationRowsForList = (left = {}, right = {}) => {
  const leftTimestamp = getConversationSortValue(left);
  const rightTimestamp = getConversationSortValue(right);
  if (leftTimestamp !== rightTimestamp) {
    return rightTimestamp - leftTimestamp;
  }

  const leftId = String(left?._id || '').trim();
  const rightId = String(right?._id || '').trim();
  if (leftId === rightId) return 0;
  return rightId.localeCompare(leftId);
};

const decodeConversationCursor = (cursor = '') => {
  const normalizedCursor = String(cursor || '').trim();
  if (!normalizedCursor) return null;

  try {
    const decoded = JSON.parse(Buffer.from(normalizedCursor, 'base64url').toString('utf8'));
    const lastMessageTime = new Date(decoded?.lastMessageTime || '');
    const id = String(decoded?.id || '').trim();
    if (!id || Number.isNaN(lastMessageTime.getTime())) {
      return null;
    }

    return {
      lastMessageTime,
      id
    };
  } catch {
    const fallbackDate = new Date(normalizedCursor);
    if (Number.isNaN(fallbackDate.getTime())) return null;
    return {
      lastMessageTime: fallbackDate,
      id: ''
    };
  }
};

const buildConversationCursorFilter = (cursor, direction = 'next') => {
  if (!cursor?.lastMessageTime) return {};

  const cursorTime = new Date(cursor.lastMessageTime);
  if (Number.isNaN(cursorTime.getTime())) return {};

  const normalizedDirection = String(direction || 'next').trim().toLowerCase();
  const isPreviousPage = normalizedDirection === 'prev';
  const comparisonOperator = isPreviousPage ? '$gt' : '$lt';

  return {
    $or: [
      { lastMessageTime: { [comparisonOperator]: cursorTime } },
      {
        lastMessageTime: cursorTime,
        _id: cursor.id
          ? { [comparisonOperator]: new mongoose.Types.ObjectId(cursor.id) }
          : { $exists: true }
      }
    ]
  };
};

const buildScopedFilters = (req, extra = {}, options = {}) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const normalizedScope = String(options?.scope || req?.query?.scope || '').trim().toLowerCase();
  const isTeamScope = normalizedScope === 'team';
  const filters = {
    companyId: req.companyId,
    ...extra
  };

  if (!isTenantWideRole(normalizedRole) && !isTeamScope) {
    filters.userId = req.user.id;
  }

  return filters;
};

const toObjectIdIfValid = (value = '') => {
  const normalizedValue = toCleanString(value);
  return mongoose.Types.ObjectId.isValid(normalizedValue)
    ? new mongoose.Types.ObjectId(normalizedValue)
    : null;
};

const attachContactSnapshotsToConversations = async (
  conversations = [],
  req,
  { fields = TEAM_INBOX_CONTACT_LIST_FIELDS } = {}
) => {
  const safeConversations = Array.isArray(conversations) ? conversations : [];
  const contactIds = safeConversations
    .map((conversation) => String(conversation?.contactId || '').trim())
    .filter((contactId) => mongoose.Types.ObjectId.isValid(contactId));

  if (!contactIds.length) {
    return safeConversations.map((conversation) => ({
      ...conversation,
      unreadCount: Math.max(0, Number(conversation?.unreadCount || 0) || 0)
    }));
  }

  const uniqueContactIds = Array.from(new Set(contactIds));
  const contacts = await Contact.find({
    _id: { $in: uniqueContactIds },
    ...buildScopedFilters(req, {}, { scope: req.query?.scope })
  })
    .select(String(fields || TEAM_INBOX_CONTACT_LIST_FIELDS).trim() || TEAM_INBOX_CONTACT_LIST_FIELDS)
    .lean();

  const contactById = new Map(
    contacts.map((contact) => [String(contact?._id || '').trim(), contact])
  );

  const normalizeContactOwnershipSnapshot = (contact = null) => {
    if (!contact || typeof contact !== 'object') return contact;

    const normalizedContact = { ...contact };
    ['ownerId', 'assignedTo', 'assignedAgent'].forEach((field) => {
      normalizedContact[field] = String(normalizedContact[field] || '').trim() || null;
    });
    return normalizedContact;
  };

  return safeConversations.map((conversation) => {
    const contactId = String(conversation?.contactId || '').trim();
    const contact = normalizeContactOwnershipSnapshot(contactById.get(contactId));
    return {
      ...conversation,
      contactId: contact || conversation?.contactId || null,
      unreadCount: Math.max(0, Number(conversation?.unreadCount || 0) || 0)
    };
  });
};

const loadConversationSummaryRows = async ({
  finalFilters,
  limit,
  queryHint = null,
  cursorDirection = 'next'
}) => {
  const normalizedDirection = String(cursorDirection || 'next').trim().toLowerCase();
  const isPreviousPage = normalizedDirection === 'prev';
  const cursorSort = isPreviousPage ? 1 : -1;
  let query = ConversationSummary.find(finalFilters)
    .select(TEAM_INBOX_CONVERSATION_FIELDS)
    .sort({ lastMessageTime: cursorSort, _id: cursorSort })
    .lean();

  if (queryHint) {
    query = query.hint(queryHint);
  }

  if (limit > 0) {
    query = query.limit(limit + 1);
  }

  let conversations;
  try {
    conversations = await query;
  } catch (error) {
    const errorMessage = String(error?.message || '').toLowerCase();
    const isHintError =
      Boolean(queryHint) &&
      (error?.code === 2 ||
        error?.codeName === 'BadValue' ||
        errorMessage.includes('hint') ||
        errorMessage.includes('bad value'));

    if (!isHintError) {
      throw error;
    }

    let fallbackQuery = ConversationSummary.find(finalFilters)
      .select(TEAM_INBOX_CONVERSATION_FIELDS)
      .sort({ lastMessageTime: cursorSort, _id: cursorSort })
      .lean();

    if (limit > 0) {
      fallbackQuery = fallbackQuery.limit(limit + 1);
    }

    conversations = await fallbackQuery;
  }

  let hasMore = false;
  if (limit > 0 && conversations.length > limit) {
    hasMore = true;
    const pageConversations = conversations.slice(0, limit);
    return {
      conversations: isPreviousPage ? pageConversations.reverse() : pageConversations,
      hasMore,
      nextCursor: encodeConversationCursor(pageConversations[pageConversations.length - 1]),
      previousCursor: encodeConversationCursor(pageConversations[0])
    };
  }

  const pageConversations = isPreviousPage ? conversations.slice().reverse() : conversations;
  return {
    conversations: pageConversations,
    hasMore,
    nextCursor: pageConversations.length
      ? encodeConversationCursor(pageConversations[pageConversations.length - 1])
      : null,
    previousCursor: pageConversations.length ? encodeConversationCursor(pageConversations[0]) : null
  };
};

const normalizeConversationFilter = (value = '') => {
  const normalizedValue = String(value || '').trim().toLowerCase();
  if (normalizedValue === 'unread' || normalizedValue === 'read') {
    return normalizedValue;
  }
  return 'all';
};

const buildConversationUnreadFilterClause = (conversationFilter = 'all') => {
  const normalizedFilter = normalizeConversationFilter(conversationFilter);
  if (normalizedFilter === 'unread') {
    return { unreadCount: { $gt: 0 } };
  }
  if (normalizedFilter === 'read') {
    return {
      $or: [
        { unreadCount: { $lte: 0 } },
        { unreadCount: { $exists: false } }
      ]
    };
  }
  return null;
};

const normalizeInboxView = (value = '') => {
  const normalized = String(value || '').trim().toLowerCase();
  const allowedViews = new Set([
    'all',
    'team',
    'unassigned',
    'assigned',
    'my',
    'closed',
    'archived',
    'whatsapp',
    'broadcast-replies',
    'instagram',
    'facebook',
    'groups',
    'important',
    'followups',
    'assigned-leads'
  ]);
  return allowedViews.has(normalized) ? normalized : 'all';
};

const buildConversationStatusFilter = (view = 'all', { userId = '' } = {}) => {
  const normalizedView = normalizeInboxView(view);
  const normalizedUserId = String(userId || '').trim();
<<<<<<< Updated upstream
  const normalizedUserObjectId = toObjectIdIfValid(normalizedUserId);
  const userIdentifier = normalizedUserObjectId || normalizedUserId;
=======
>>>>>>> Stashed changes

  const activeStatuses = ['active', 'pending'];
  const closedStatuses = ['resolved'];

  switch (normalizedView) {
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
      return {
        $and: [
          {
            $or: [
              { assignedTo: { $nin: [null, ''] } },
              { assignedToId: { $ne: null } }
            ]
          },
          { status: { $in: activeStatuses } }
        ]
      };
    case 'my':
      return normalizedUserId
        ? {
            $and: [
              {
                $or: [
<<<<<<< Updated upstream
                  { userId: userIdentifier },
                  { assignedTo: normalizedUserId },
                  { assignedToId: userIdentifier },
                  { assignedAgent: normalizedUserId }
=======
                  { assignedTo: normalizedUserId },
                  { assignedToId: normalizedUserId }
>>>>>>> Stashed changes
                ]
              },
              { status: { $in: activeStatuses } }
            ]
          }
        : { status: { $in: activeStatuses } };
    case 'closed':
      return {
        $or: [
          { status: { $in: closedStatuses } },
          { leadStatus: 'closed' }
        ]
      };
    case 'archived':
      return { status: 'archived' };
    case 'whatsapp':
      return { channel: 'whatsapp' };
    case 'broadcast-replies':
      return { channel: 'broadcast_reply' };
    case 'instagram':
      return { channel: 'instagram' };
    case 'facebook':
      return { channel: 'facebook' };
    case 'groups':
      return { channel: 'group' };
    case 'important':
      return { important: true };
      case 'followups':
        return { followupAt: { $ne: null } };
    case 'assigned-leads':
      return normalizedUserId
        ? {
            $or: [
<<<<<<< Updated upstream
              { userId: userIdentifier },
              { assignedTo: normalizedUserId },
              { assignedToId: userIdentifier },
              { assignedAgent: normalizedUserId }
=======
              { assignedTo: normalizedUserId },
              { assignedToId: normalizedUserId }
>>>>>>> Stashed changes
            ]
          }
        : { assignedTo: { $nin: [null, ''] } };
    case 'team':
      return { status: { $in: activeStatuses } };
    case 'all':
    default:
      return {};
  }
};

const buildConversationViewFilters = (req, extra = {}) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const isAgent = !isTenantWideRole(normalizedRole);
<<<<<<< Updated upstream
  const normalizedView = normalizeInboxView(extra?.view || req?.query?.view || req?.query?.inboxView);
  const normalizedUserId = String(req?.user?.id || '').trim();
  const normalizedUserObjectId = toObjectIdIfValid(normalizedUserId);
  const normalizedCompanyId = toObjectIdIfValid(req?.companyId || req?.user?.companyId);
  const userIdentifier = normalizedUserObjectId || normalizedUserId;
  const baseFilters = {
    ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
=======
  const normalizedView = normalizeInboxView(req?.query?.view || req?.query?.inboxView || extra?.view);
  const normalizedUserId = String(req?.user?.id || '').trim();
  const baseFilters = {
    companyId: req.companyId,
>>>>>>> Stashed changes
    ...extra
  };
  delete baseFilters.view;
  delete baseFilters.inboxView;

  const viewFilter = buildConversationStatusFilter(
    isAgent ? (normalizedView === 'all' ? 'my' : normalizedView) : normalizedView,
<<<<<<< Updated upstream
    { userId: userIdentifier }
  );

  if (isAgent) {
    const ownershipFilter = userIdentifier
      ? {
          $or: [
            { userId: userIdentifier },
            { assignedTo: normalizedUserId },
            { assignedToId: userIdentifier },
            { assignedAgent: normalizedUserId }
=======
    { userId: normalizedUserId }
  );

  if (isAgent) {
    const ownershipFilter = normalizedUserId
      ? {
          $or: [
            { assignedTo: normalizedUserId },
            { assignedToId: normalizedUserId }
>>>>>>> Stashed changes
          ]
        }
      : {};

    return {
      ...baseFilters,
      ...(Object.keys(ownershipFilter).length ? { $and: [ownershipFilter, viewFilter].filter(Boolean) } : {}),
      ...(Object.keys(ownershipFilter).length === 0 ? viewFilter : {})
    };
  }

  return {
    ...baseFilters,
    ...viewFilter
  };
};

const buildContactViewFilters = (req, extra = {}) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const isAgent = !isTenantWideRole(normalizedRole);
<<<<<<< Updated upstream
  const normalizedView = normalizeInboxView(extra?.view || req?.query?.view || req?.query?.inboxView);
  const normalizedUserId = String(req?.user?.id || '').trim();
  const normalizedUserObjectId = toObjectIdIfValid(normalizedUserId);
  const normalizedCompanyId = toObjectIdIfValid(req?.companyId || req?.user?.companyId);
  const userIdentifier = normalizedUserObjectId || normalizedUserId;
  const baseFilters = {
    ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
=======
  const normalizedView = normalizeInboxView(req?.query?.view || req?.query?.inboxView || extra?.view);
  const normalizedUserId = String(req?.user?.id || '').trim();
  const baseFilters = {
    companyId: req.companyId,
>>>>>>> Stashed changes
    ...extra
  };
  delete baseFilters.view;
  delete baseFilters.inboxView;

  const leadStatusFilter = (() => {
    switch (isAgent ? (normalizedView === 'all' ? 'my' : normalizedView) : normalizedView) {
      case 'closed':
        return { leadStatus: 'closed' };
      case 'followups':
        return {
          $or: [
            { followupDate: { $ne: null } },
            { nextFollowUpAt: { $ne: null } }
          ]
        };
      case 'important':
        return { important: true };
      case 'assigned':
      case 'assigned-leads':
      case 'my':
<<<<<<< Updated upstream
        return userIdentifier
          ? {
              $or: [
                { assignedTo: normalizedUserId },
                { assignedToId: userIdentifier },
                { assignedAgent: normalizedUserId },
                { userId: userIdentifier }
=======
        return normalizedUserId
          ? {
              $or: [
                { assignedTo: normalizedUserId },
                { assignedToId: normalizedUserId }
>>>>>>> Stashed changes
              ]
            }
          : { assignedTo: { $ne: null } };
      case 'unassigned':
        return {
          $or: [
            { assignedTo: { $in: [null, ''] } },
            { assignedTo: { $exists: false } },
            { assignedToId: null },
            { assignedToId: { $exists: false } }
          ]
        };
      default:
        return {};
    }
  })();

  if (isAgent) {
<<<<<<< Updated upstream
    const ownershipFilter = userIdentifier
      ? {
          $or: [
            { userId: userIdentifier },
            { assignedTo: normalizedUserId },
            { assignedToId: userIdentifier },
            { assignedAgent: normalizedUserId }
=======
    const ownershipFilter = normalizedUserId
      ? {
          $or: [
            { assignedTo: normalizedUserId },
            { assignedToId: normalizedUserId }
>>>>>>> Stashed changes
          ]
        }
      : {};
    return {
      ...baseFilters,
      ...(Object.keys(ownershipFilter).length
        ? { $and: [ownershipFilter, leadStatusFilter].filter(Boolean) }
        : {}),
      ...(Object.keys(ownershipFilter).length === 0 ? leadStatusFilter : {})
    };
  }

  return {
    ...baseFilters,
    ...leadStatusFilter
  };
};

const encodeConversationCursorCacheKey = (cursor = null, direction = 'next') => {
  if (!cursor?.lastMessageTime) return '';

  const lastMessageTime = new Date(cursor.lastMessageTime);
  if (Number.isNaN(lastMessageTime.getTime())) return '';

  return `${String(direction || 'next').trim().toLowerCase()}::${lastMessageTime.toISOString()}::${String(cursor.id || '').trim()}`;
};

const loadConversationFallbackRows = async ({
  finalFilters,
  limit,
  skip = 0,
  cursorDirection = 'next'
}) => {
  const normalizedDirection = String(cursorDirection || 'next').trim().toLowerCase();
  const isPreviousPage = normalizedDirection === 'prev';
  const cursorSort = isPreviousPage ? 1 : -1;
  let query = Conversation.find(finalFilters)
    .select(TEAM_INBOX_CONVERSATION_FIELDS)
    .sort({ lastMessageTime: cursorSort, _id: cursorSort })
    .lean();

  if (skip > 0) {
    query = query.skip(skip);
  }

  if (limit > 0) {
    query = query.limit(limit + 1);
  }

  const rawConversations = await query;
  const hasMore = limit > 0 && rawConversations.length > limit;
  const rawItems = hasMore ? rawConversations.slice(0, limit) : rawConversations;

  if (rawItems.length) {
    // Keep the response path fast; summary backfill is important but not required
    // before the client can render the current page of conversations.
    void upsertConversationSummaries(rawItems).catch((error) => {
      console.error('Failed to backfill conversation summaries:', error);
    });
  }

  return {
    conversations: isPreviousPage ? rawItems.reverse() : rawItems,
    hasMore,
    nextCursor: hasMore ? encodeConversationCursor(rawItems[rawItems.length - 1]) : null,
    previousCursor: rawItems.length ? encodeConversationCursor(rawItems[0]) : null
  };
};

const loadConversationSummaryPage = async ({
  summaryFilters,
  fallbackFilters,
  limit,
  req,
  scope,
  cacheKeyParts,
  queryHint = null,
  cursorDirection = 'next'
}) => {
  const fetchLimit = limit > 0 ? Math.max(limit + 1, limit) : 0;
  const normalizedDirection = String(cursorDirection || 'next').trim().toLowerCase();
  const mergeConversationRows = (primaryRows = [], secondaryRows = []) => {
    const mergedById = new Map();

    const addRows = (rows = []) => {
      (Array.isArray(rows) ? rows : []).forEach((conversation) => {
        const conversationId = String(conversation?._id || '').trim();
        if (!conversationId || mergedById.has(conversationId)) return;
        mergedById.set(conversationId, conversation);
      });
    };

    addRows(primaryRows);
    addRows(secondaryRows);

    return Array.from(mergedById.values()).sort(
      compareConversationRowsForList
    );
  };

  const loadMergedRows = async () => {
    const summaryRows = await loadConversationSummaryRows({
      finalFilters: summaryFilters,
      limit: fetchLimit,
      queryHint,
      cursorDirection: normalizedDirection
    });

    const shouldLoadFallback =
      !Array.isArray(summaryRows?.conversations) || summaryRows.conversations.length === 0;

    const fallbackRows = shouldLoadFallback
      ? await loadConversationFallbackRows({
          finalFilters: fallbackFilters,
          limit: fetchLimit,
          cursorDirection: normalizedDirection
        })
      : { conversations: [], hasMore: false, nextCursor: null };

    const mergedConversations = mergeConversationRows(
      summaryRows?.conversations || [],
      fallbackRows?.conversations || []
    );
    const pagedConversations = limit > 0 ? mergedConversations.slice(0, limit) : mergedConversations;

    const summaryConversationIds = new Set(
      (Array.isArray(summaryRows?.conversations) ? summaryRows.conversations : [])
        .map((conversation) => String(conversation?._id || '').trim())
        .filter(Boolean)
    );
    const fallbackConversationIds = (Array.isArray(fallbackRows?.conversations) ? fallbackRows.conversations : [])
      .map((conversation) => String(conversation?._id || '').trim())
      .filter(Boolean);
    const shouldBackfillSummaries = fallbackConversationIds.some(
      (conversationId) => !summaryConversationIds.has(conversationId)
    );

    if (
      shouldBackfillSummaries &&
      String(process.env.INBOX_BACKFILL_SUMMARIES_ON_READ || '').trim().toLowerCase() === 'true'
    ) {
      void upsertConversationSummaries(fallbackRows.conversations).catch((error) => {
        console.error('Failed to backfill conversation summaries from merged page:', error);
      });
    }

    const pageHasMore =
      Boolean(summaryRows?.hasMore) ||
      Boolean(fallbackRows?.hasMore) ||
      (limit > 0 && mergedConversations.length > limit);

    return {
      conversations: pagedConversations,
      hasMore: pageHasMore && pagedConversations.length > 0,
      nextCursor:
        pageHasMore && pagedConversations.length > 0
          ? encodeConversationCursor(pagedConversations[pagedConversations.length - 1])
          : null,
      previousCursor:
        pagedConversations.length > 0 ? encodeConversationCursor(pagedConversations[0]) : null
    };
  };

  const cachedSummaryRows = scope
    ? await getOrSetCachedJson({
        namespace: 'conversations',
        scope,
        versionGroup: 'summaryPages',
        keyParts: cacheKeyParts,
        ttlSeconds: CACHE_TTL_SECONDS.summaryPages,
        loader: loadMergedRows
      })
    : await loadMergedRows();

  const conversations = await attachContactSnapshotsToConversations(
    cachedSummaryRows?.conversations || [],
    req
  );

  return {
    conversations,
    hasMore: Boolean(cachedSummaryRows?.hasMore),
    nextCursor: cachedSummaryRows?.nextCursor || null
  };
};

const loadInboxOverviewSnapshot = async (req, { isAgent = false, filters = {}, scope = '' } = {}) => {
  const viewKeys = isAgent
    ? ['my', 'assigned-leads', 'followups', 'closed', 'archived', 'whatsapp']
    : [
        'all',
        'unassigned',
        'assigned',
        'closed',
        'archived',
        'my',
        'team',
        'important',
        'followups',
        'assigned-leads',
        'whatsapp',
        'broadcast-replies',
        'instagram',
        'facebook',
        'groups'
      ];

  const normalizeCount = (aggregation = {}, key = '') =>
    Number(aggregation?.[key]?.[0]?.count || 0);

  const getSnapshot = async () => {
    const facets = viewKeys.reduce((acc, view) => {
      acc[view] = [
        { $match: buildConversationViewFilters(req, { view }) },
        { $count: 'count' }
      ];
      return acc;
    }, {});

    facets.unread = [
      { $match: { ...filters, unreadCount: { $gt: 0 } } },
      { $count: 'count' }
    ];

    const [aggregation = {}] = await ConversationSummary.aggregate([{ $facet: facets }]).allowDiskUse(true);

    if (isAgent) {
      return {
        myChats: normalizeCount(aggregation, 'my'),
        assignedLeads: normalizeCount(aggregation, 'assigned-leads'),
        followups: normalizeCount(aggregation, 'followups'),
        closedChats: normalizeCount(aggregation, 'closed'),
        archivedChats: normalizeCount(aggregation, 'archived'),
        whatsappChats: normalizeCount(aggregation, 'whatsapp'),
        unreadConversations: normalizeCount(aggregation, 'unread')
      };
    }

    return {
      allChats: normalizeCount(aggregation, 'all'),
      unassignedChats: normalizeCount(aggregation, 'unassigned'),
      assignedChats: normalizeCount(aggregation, 'assigned'),
      closedChats: normalizeCount(aggregation, 'closed'),
      archivedChats: normalizeCount(aggregation, 'archived'),
      myChats: normalizeCount(aggregation, 'my'),
      teamInbox: normalizeCount(aggregation, 'team'),
      importantChats: normalizeCount(aggregation, 'important'),
      followups: normalizeCount(aggregation, 'followups'),
      assignedLeads: normalizeCount(aggregation, 'assigned-leads'),
      whatsappChats: normalizeCount(aggregation, 'whatsapp'),
      broadcastRepliesChats: normalizeCount(aggregation, 'broadcast-replies'),
      instagramChats: normalizeCount(aggregation, 'instagram'),
      facebookChats: normalizeCount(aggregation, 'facebook'),
      groupChats: normalizeCount(aggregation, 'groups'),
      unreadConversations: normalizeCount(aggregation, 'unread')
    };
  };

  if (!scope) {
    return getSnapshot();
  }

  return getOrSetCachedJson({
    namespace: 'conversations',
    scope,
    versionGroup: 'overviewSnapshot',
    keyParts: ['overview-snapshot', String(isAgent), JSON.stringify(filters)],
    ttlSeconds: CACHE_TTL_SECONDS.summaryPages,
    loader: getSnapshot
  });
};

class ConversationController {
  async getConversations(req, res) {
    try {
      const { status, assignedTo, search } = req.query;
      const conversationFilter = normalizeConversationFilter(
        req.query?.filter || req.query?.conversationFilter || ''
      );
      const filters = buildConversationViewFilters(req, {});
      const scopeVariants = getInboxScopeVariants({
        companyId: filters.companyId,
        userId: req.user?.id || ''
      });
      const scope = isTenantWideRole(
        normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
      )
        ? scopeVariants[0]
        : scopeVariants[scopeVariants.length - 1];

      if (status) filters.status = String(status).trim().toLowerCase();
      if (assignedTo) filters.assignedTo = assignedTo;

      const parsedLimit = Number(req.query?.limit);
      const limit = Number.isFinite(parsedLimit) ? Math.max(1, Math.min(parsedLimit, 200)) : 0;
      const cursor = decodeConversationCursor(req.query?.cursor);
      const cursorDirection = String(req.query?.cursorDirection || req.query?.direction || 'next')
        .trim()
        .toLowerCase() === 'prev'
        ? 'prev'
        : 'next';
      const normalizedSearch = String(search || '').trim();
      const normalizedSearchLower = normalizedSearch.toLowerCase();
      const searchPlan = buildInboxSearchPlan(normalizedSearch);
      const cacheKeyParts = [
        String(req.query?.view || req.query?.inboxView || '').trim(),
        String(status || '').trim(),
        String(assignedTo || '').trim(),
        conversationFilter,
        normalizedSearchLower,
        String(limit || 0),
        encodeConversationCursorCacheKey(cursor, cursorDirection)
      ];
      const summaryFilters = { ...filters };
      const fallbackFilters = { ...filters };
      const summaryFilterClauses = [];
      const fallbackFilterClauses = [];
      const queryHint = searchPlan.hint;
      const unreadFilterClause = buildConversationUnreadFilterClause(conversationFilter);
      if (cursor) {
        const cursorFilter = buildConversationCursorFilter(cursor, cursorDirection);
        summaryFilterClauses.push(cursorFilter);
        fallbackFilterClauses.push(cursorFilter);
      }
      if (unreadFilterClause) {
        summaryFilterClauses.push(unreadFilterClause);
        fallbackFilterClauses.push(unreadFilterClause);
      }
      if (normalizedSearch) {
        summaryFilterClauses.push(searchPlan.summaryClause);
        fallbackFilterClauses.push(searchPlan.fallbackClause);
      }
      if (summaryFilterClauses.length === 1) {
        Object.assign(summaryFilters, summaryFilterClauses[0]);
      } else if (summaryFilterClauses.length > 1) {
        summaryFilters.$and = summaryFilterClauses;
      }
      if (fallbackFilterClauses.length === 1) {
        Object.assign(fallbackFilters, fallbackFilterClauses[0]);
      } else if (fallbackFilterClauses.length > 1) {
        fallbackFilters.$and = fallbackFilterClauses;
      }

      const cachedResponse = scope
        ? await getOrSetCachedJson({
            namespace: 'conversations',
            scope,
            versionGroup: 'list',
            keyParts: cacheKeyParts,
            ttlSeconds: CACHE_TTL_SECONDS.conversations,
            loader: async () => {
              const summaryPage = await loadConversationSummaryPage({
                summaryFilters,
                fallbackFilters,
                limit,
                req,
                scope,
                cacheKeyParts,
                queryHint,
                cursorDirection
              });

              return {
                success: true,
                data: summaryPage.conversations,
                meta: {
                  limit: limit || null,
                  hasMore: summaryPage.hasMore,
                  nextCursor: summaryPage.nextCursor,
                  previousCursor: summaryPage.previousCursor || null
                }
              };
            }
          })
        : null;

      if (cachedResponse) {
        const hydratedResponse = await getOrSetCachedJson({
          namespace: 'conversations',
          scope,
          versionGroup: 'hydratedPages',
          keyParts: [...cacheKeyParts, 'hydrated'],
          ttlSeconds: CACHE_TTL_SECONDS.conversations,
          loader: async () => {
            const conversations = await attachContactSnapshotsToConversations(
              cachedResponse.data || [],
              req,
              { fields: TEAM_INBOX_CONTACT_LIST_FIELDS }
            );

            return {
              success: true,
              data: conversations,
              meta: cachedResponse.meta || {
                limit: limit || null,
                hasMore: false,
                nextCursor: null,
                previousCursor: null
              }
            };
          }
        });

        return res.json(hydratedResponse || cachedResponse);
      }

      return res.json({
        success: true,
        data: [],
        meta: {
          limit: limit || null,
          hasMore: false,
          nextCursor: null,
          previousCursor: null
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getContacts(req, res) {
    try {
      const { search, tags } = req.query;
      const wantsMarketingEligible =
        String(req.query?.marketingEligible || '').trim().toLowerCase() === 'true';
      const wantsRecentlyInteracted =
        String(req.query?.recentlyInteractedOnly || req.query?.repliedOnly || '')
          .trim()
          .toLowerCase() === 'true';
      const requestedOptInStatus = toCleanString(req.query?.whatsappOptInStatus || '').toLowerCase();
      const requestedSourceType = toCleanString(req.query?.sourceType || '').toLowerCase();
      const limit = Math.max(1, Math.min(500, Number(req.query?.limit || 100)));
      const cursor = decodeContactCursor(req.query?.cursor);
      const filters = buildContactViewFilters(req, {});
      const searchPlan = buildContactSearchPlan(search);

      if (searchPlan.summaryClause) {
        filters.$and = filters.$and ? [...filters.$and, searchPlan.summaryClause] : [searchPlan.summaryClause];
      }

      if (tags) {
        const tagArray = Array.isArray(tags) ? tags : tags.split(',');
        filters.tags = { $in: tagArray };
      }

      if (requestedOptInStatus) {
        filters.whatsappOptInStatus = requestedOptInStatus;
      }

      if (requestedSourceType) {
        filters.sourceType = requestedSourceType;
      }

      const includeTotalCount =
        String(req.query?.includeTotalCount ?? 'true').trim().toLowerCase() !== 'false';
      const totalCount = includeTotalCount ? await Contact.countDocuments(filters) : null;
      const cursorFilters = cursor ? { ...filters, ...buildContactCursorFilter(cursor) } : filters;
      let contactQuery = Contact.find(cursorFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort({ lastContact: -1, createdAt: -1, _id: -1 });

      if (searchPlan.hint) {
        contactQuery = contactQuery.hint(searchPlan.hint);
      }

      let contacts = await contactQuery.limit(limit + 1).lean();

      const hasMore = contacts.length > limit;
      if (hasMore) {
        contacts = contacts.slice(0, limit);
      }

      const filteredContacts = (Array.isArray(contacts) ? contacts : []).filter((contact) => {
        const policy = getWhatsAppMessagingPolicy(contact);
        if (wantsMarketingEligible && !policy.marketingTemplateAllowed) {
          return false;
        }
        if (wantsRecentlyInteracted && !policy.serviceWindowOpen) {
          return false;
        }
        return true;
      });
      
      if (includeTotalCount) {
        res.setHeader('x-total-count', String(totalCount));
      }
      res.json({
        success: true,
        data: filteredContacts,
        meta: {
          limit,
          hasMore,
          nextCursor: hasMore ? encodeContactCursor(filteredContacts[filteredContacts.length - 1]) : null,
          totalCount: includeTotalCount && Number.isFinite(totalCount) ? totalCount : null
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getContactById(req, res) {
    try {
      const { id } = req.params;
      
      const contact = await Contact.findOne(
        buildScopedFilters(req, { _id: id }, { scope: req.query?.scope })
      ).lean();
      if (!contact) {
        return res.status(404).json({ 
          success: false, 
          error: 'Contact not found' 
        });
      }
      
      res.json({ success: true, data: contact });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getConversationContacts(req, res) {
    try {
      // Read from the summary model so contact lookups stay index-friendly and compact.
      const conversations = await ConversationSummary.find({
        ...buildConversationViewFilters(req, {}),
        status: { $in: ['active', 'pending'] }
      })
        .select('contactPhone contactName lastMessageTime lastMessage status')
        .sort({ lastMessageTime: -1 })
        .lean();

      const uniqueContacts = [];
      const seenPhones = new Set();

      conversations.forEach((conv) => {
        const phone = String(conv?.contactPhone || '').trim();
        if (!phone || seenPhones.has(phone)) {
          return;
        }

        uniqueContacts.push({
          phone,
          name: conv.contactName || phone,
          lastMessageTime: conv.lastMessageTime,
          lastMessage: conv.lastMessage,
          status: conv.status
        });
        seenPhones.add(phone);
      });

      res.json({ success: true, data: uniqueContacts });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getUnreadConversationCount(req, res) {
    try {
      const filters = buildConversationViewFilters(req, {});
      const summaryFilters = {
        ...filters,
        unreadCount: { $gt: 0 },
      };
      const scopeVariants = getInboxScopeVariants({
        companyId: filters.companyId,
        userId: req.user?.id || ''
      });
      const scope = isTenantWideRole(
        normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
      )
        ? scopeVariants[0]
        : scopeVariants[scopeVariants.length - 1];

      const unreadConversationCount = scope
        ? await getOrSetCachedJson({
            namespace: 'conversations',
            scope,
            versionGroup: 'unreadCount',
            keyParts: ['unreadCount', JSON.stringify(summaryFilters)],
            ttlSeconds: CACHE_TTL_SECONDS.summaryPages,
            loader: async () => ConversationSummary.countDocuments(summaryFilters)
          })
        : await ConversationSummary.countDocuments(summaryFilters);

      res.json({
        success: true,
        data: {
          unreadConversationCount: Number(unreadConversationCount || 0),
        },
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getInboxOverview(req, res) {
    try {
      const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
      const isAgent = !isTenantWideRole(normalizedRole);
<<<<<<< Updated upstream
      const filters = buildConversationViewFilters(req, {
        view: isAgent ? 'my' : 'all'
      });
=======
      const filters = buildConversationViewFilters(req, {});
>>>>>>> Stashed changes
      const scopeVariants = getInboxScopeVariants({
        companyId: filters.companyId,
        userId: req.user?.id || ''
      });
      const scope = isTenantWideRole(normalizedRole)
        ? scopeVariants[0]
        : scopeVariants[scopeVariants.length - 1];
      const data = await loadInboxOverviewSnapshot(req, {
        isAgent,
        filters,
        scope
      });

      res.json({
        success: true,
        data
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async createContact(req, res) {
    try {
      const { name, phone, email, tags, notes, stage, status, source, ownerId, nextFollowUpAt } = req.body;
      
      // Check if contact already exists
      const existingContact = await Contact.findOne(
        buildScopedFilters(req, { phone }, { scope: req.query?.scope })
      );
      if (existingContact) {
        return res.status(400).json({ 
          success: false, 
          error: 'Contact with this phone number already exists' 
        });
      }
      
      const contact = await Contact.create({
        userId: req.user.id,
        companyId: req.companyId,
        name,
        phone,
        email,
        tags: tags || [],
        notes,
        stage: stage || 'new',
        status: status || 'nurturing',
        source: source || '',
        ownerId: ownerId || null,
        nextFollowUpAt: nextFollowUpAt || null,
        sourceType: 'manual'
      });
      
      res.status(201).json({ success: true, data: contact });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async updateContact(req, res) {
    try {
      const { id } = req.params;
      const {
        name,
        phone,
        email,
        tags,
        notes,
        isBlocked,
        stage,
        status,
        source,
        ownerId,
        nextFollowUpAt,
        lastContactAt,
        customFields
      } = req.body;
      
      // Find contact by ID
      const contact = await Contact.findOne(
        buildScopedFilters(req, { _id: id }, { scope: req.query?.scope })
      );
      if (!contact) {
        return res.status(404).json({ 
          success: false, 
          error: 'Contact not found' 
        });
      }
      
      // Check if phone number is being changed and if it conflicts with existing contact
      if (phone && phone !== contact.phone) {
        const existingContact = await Contact.findOne(
          buildScopedFilters(req, { phone, _id: { $ne: id } })
        );
        if (existingContact) {
          return res.status(400).json({ 
            success: false, 
            error: 'Another contact with this phone number already exists' 
          });
        }
      }
      
      // Update contact fields
      const updateData = {};
      if (name !== undefined) updateData.name = name;
      if (phone !== undefined) updateData.phone = phone;
      if (email !== undefined) updateData.email = email;
      if (tags !== undefined) updateData.tags = tags;
      if (notes !== undefined) updateData.notes = notes;
      if (isBlocked !== undefined) updateData.isBlocked = isBlocked;
      if (stage !== undefined) updateData.stage = stage;
      if (status !== undefined) updateData.status = status;
      if (source !== undefined) updateData.source = source;
      if (ownerId !== undefined) updateData.ownerId = ownerId;
      if (nextFollowUpAt !== undefined) updateData.nextFollowUpAt = nextFollowUpAt || null;
      if (lastContactAt !== undefined) updateData.lastContactAt = lastContactAt || null;
      if (customFields !== undefined) {
        updateData.customFields = {
          ...(contact.customFields && typeof contact.customFields === 'object' ? contact.customFields : {}),
          ...(customFields && typeof customFields === 'object' ? customFields : {})
        };
      }
      
      const updatedContact = await Contact.findOneAndUpdate(
        buildScopedFilters(req, { _id: id }, { scope: req.query?.scope }),
        updateData, 
        { new: true, runValidators: true }
      );
      
      const conversationUpdate = {};
      if (name !== undefined && name !== contact.name) {
        conversationUpdate.contactName = name;
      }
      if (phone !== undefined && phone !== contact.phone) {
        conversationUpdate.contactPhone = phone;
      }

      // Keep conversation documents and the read model aligned with the contact record.
      if (Object.keys(conversationUpdate).length > 0) {
        await Conversation.updateMany(
          buildScopedFilters(req, { contactId: id }),
          conversationUpdate
        );
        const updatedConversations = await Conversation.find(
          buildScopedFilters(req, { contactId: id })
        )
          .select(TEAM_INBOX_CONVERSATION_FIELDS)
          .lean();
        await upsertConversationSummaries(updatedConversations);
        await invalidateInboxScope({
          companyId: req.companyId || '',
          userId: req.user.id || ''
        });
      }
      
      res.json({ success: true, data: updatedContact });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteContact(req, res) {
    try {
      const { id } = req.params;
      
      const contact = await Contact.findOne(
        buildScopedFilters(req, { _id: id }, { scope: req.query?.scope })
      );
      if (!contact) {
        return res.status(404).json({ 
          success: false, 
          error: 'Contact not found' 
        });
      }
      
      await Contact.deleteOne(buildScopedFilters(req, { _id: id }));
      
      res.json({ success: true, message: 'Contact deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteConversation(req, res) {
    try {
      const { id } = req.params;
      
      const conversation = await Conversation.findOne(
        buildScopedFilters(req, { _id: id }, { scope: req.query?.scope })
      );
      if (!conversation) {
        return res.status(404).json({ 
          success: false, 
          error: 'Conversation not found' 
        });
      }
      
      await Conversation.deleteOne(buildScopedFilters(req, { _id: id }));
      await deleteConversationSummary(id);
      await Message.deleteMany(buildScopedFilters(req, { conversationId: id }));
      await invalidateInboxConversation({
        companyId: req.companyId || '',
        userId: req.user.id || '',
        conversationId: id
      });
      
      res.json({ success: true, message: 'Conversation deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteAllConversations(req, res) {
    try {
      await Conversation.deleteMany(buildScopedFilters(req));
      await Message.deleteMany(buildScopedFilters(req));
      await invalidateInboxScope({
        companyId: req.companyId || '',
        userId: req.user.id || ''
      });
      res.json({ success: true, message: 'All conversations deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteSelectedConversations(req, res) {
    try {
      const { conversationIds } = req.body;
      
      if (!conversationIds || !Array.isArray(conversationIds)) {
        return res.status(400).json({ 
          success: false, 
          error: 'Conversation IDs array is required' 
        });
      }
      
      await Conversation.deleteMany(buildScopedFilters(req, { _id: { $in: conversationIds } }));
      await Message.deleteMany(buildScopedFilters(req, { conversationId: { $in: conversationIds } }));
      await invalidateInboxScope({
        companyId: req.companyId || '',
        userId: req.user.id || ''
      });
      
      res.json({ 
        success: true, 
        message: `${conversationIds.length} conversations deleted successfully` 
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
}

module.exports = new ConversationController();
