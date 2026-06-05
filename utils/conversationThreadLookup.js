const mongoose = require('mongoose');
const { buildConversationPhoneLookupFilter } = require('./conversationIdentity');
const { normalizeRole, isTenantWideRole } = require('./accessControl');

const toCleanString = (value = '') => String(value || '').trim();

const buildThreadAccessFilter = (req = {}) => {
  const companyId = toCleanString(req?.companyId || req?.user?.companyId);
  const normalizedRole = normalizeRole(
    req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role
  );
  const isAgent = !isTenantWideRole(normalizedRole);
  const userId = toCleanString(req?.user?.id);

  if (!companyId) {
    return {};
  }

  if (!isAgent) {
    return { companyId };
  }

  const ownershipClauses = [];
  if (userId) {
<<<<<<< Updated upstream
    ownershipClauses.push({ userId });
=======
>>>>>>> Stashed changes
    ownershipClauses.push({ assignedTo: userId });
    ownershipClauses.push({ assignedToId: userId });
    ownershipClauses.push({ assignedAgent: userId });
  }
  ownershipClauses.push({ assignedTo: { $in: [null, ''] } });
  ownershipClauses.push({ assignedTo: { $exists: false } });
  ownershipClauses.push({ assignedToId: { $exists: false } });
  ownershipClauses.push({ assignedToId: null });

  return {
    companyId,
    $or: ownershipClauses
  };
};

const buildThreadIdentityClauses = (conversation = {}) => {
  const contactId = toCleanString(conversation?.contactId?._id || conversation?.contactId);
  const contactPhone = toCleanString(conversation?.contactPhone);
  const phoneLookupFilter = buildConversationPhoneLookupFilter(contactPhone);

  const clauses = [];
  if (contactId && mongoose.Types.ObjectId.isValid(contactId)) {
    clauses.push({ contactId });
  }
  if (phoneLookupFilter) {
    clauses.push(phoneLookupFilter);
  }

  return clauses;
};

const resolveRelatedConversationIds = async ({
  Conversation,
  ConversationSummary,
  req = {},
  conversation = null,
  includeAllIdentityMatches = false
} = {}) => {
  const normalizedConversationId = toCleanString(conversation?._id);
  const ids = new Set();
  if (normalizedConversationId) {
    ids.add(normalizedConversationId);
  }

  if (!conversation || (!conversation?._id && !conversation?.contactPhone && !conversation?.contactId)) {
    return Array.from(ids);
  }

  const accessFilter = buildThreadAccessFilter(req);
  const identityClauses = buildThreadIdentityClauses(conversation);
  const hasIdentityClauses = identityClauses.length > 0;

  const buildQuery = (extra = {}) => {
    const base = {
      ...extra
    };

    if (Object.keys(accessFilter).length > 0) {
      if (Object.keys(base).length > 0) {
        return {
          $and: [accessFilter, base]
        };
      }
      return accessFilter;
    }

    return base;
  };

  const relatedConversationFilter = hasIdentityClauses
    ? buildQuery({
        $or: identityClauses
      })
    : buildQuery();

  const relatedSummaryFilter = hasIdentityClauses
    ? buildQuery({
        $or: identityClauses
      })
    : buildQuery();

  const [relatedConversations = [], relatedSummaries = []] = await Promise.all([
    Conversation && typeof Conversation.find === 'function'
      ? Conversation.find(relatedConversationFilter).select('_id').lean()
      : Promise.resolve([]),
    ConversationSummary && typeof ConversationSummary.find === 'function'
      ? ConversationSummary.find(relatedSummaryFilter).select('conversationId').lean()
      : Promise.resolve([])
  ]);

  const companyScopeOnlyFilter = includeAllIdentityMatches && hasIdentityClauses
    ? {
        companyId: accessFilter?.companyId || req?.companyId || req?.user?.companyId || undefined,
        $or: identityClauses
      }
    : null;

  const [companyWideConversations = [], companyWideSummaries = []] = includeAllIdentityMatches &&
    hasIdentityClauses &&
    companyScopeOnlyFilter
    ? await Promise.all([
        Conversation && typeof Conversation.find === 'function'
          ? Conversation.find(companyWideConversationsFilter(companyScopeOnlyFilter)).select('_id').lean()
          : Promise.resolve([]),
        ConversationSummary && typeof ConversationSummary.find === 'function'
          ? ConversationSummary.find(companyWideConversationsFilter(companyScopeOnlyFilter))
              .select('conversationId')
              .lean()
          : Promise.resolve([])
      ])
    : [[], []];

  relatedConversations.forEach((row) => {
    const conversationId = toCleanString(row?._id);
    if (conversationId) ids.add(conversationId);
  });

  relatedSummaries.forEach((row) => {
    const conversationId = toCleanString(row?.conversationId);
    if (conversationId) ids.add(conversationId);
  });

  companyWideConversations.forEach((row) => {
    const conversationId = toCleanString(row?._id);
    if (conversationId) ids.add(conversationId);
  });

  companyWideSummaries.forEach((row) => {
    const conversationId = toCleanString(row?.conversationId);
    if (conversationId) ids.add(conversationId);
  });

  return Array.from(ids);
};

const companyWideConversationsFilter = (filter = {}) => {
  const nextFilter = { ...(filter || {}) };
  const companyId = toCleanString(nextFilter.companyId);
  if (!companyId) {
    delete nextFilter.companyId;
  } else {
    nextFilter.companyId = companyId;
  }
  return nextFilter;
};

module.exports = {
  buildThreadAccessFilter,
  resolveRelatedConversationIds
};
