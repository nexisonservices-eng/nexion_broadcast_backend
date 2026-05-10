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

const TEAM_INBOX_CONTACT_SUMMARY_FIELDS =
  '_id name phone tags status stage leadScore leadScoreBreakdown isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptOutAt lastInboundMessageAt serviceWindowClosesAt';
const CONTACT_LIST_FIELDS =
  '_id name phone email tags stage status source ownerId sourceType lastContact lastContactAt nextFollowUpAt isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata whatsappMarketingWindowStartedAt whatsappMarketingSendCount whatsappMarketingLastSentAt whatsappOptOutAt lastInboundMessageAt serviceWindowClosesAt leadScore createdAt updatedAt';

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
  'userId',
  'companyId',
  'contactId',
  'contactPhone',
  'contactName',
  'status',
  'assignedTo',
  'assignedToId',
  'tags',
  'priority',
  'lastMessageTime',
  'lastMessage',
  'lastMessageMediaType',
  'lastMessageAttachmentName',
  'lastMessageAttachmentPages',
  'lastMessageFrom',
  'lastMessageWhatsappMessageId',
  'lastMessageStatus',
  'unreadCount',
  'notes',
  'createdAt',
  'updatedAt',
  'resolvedAt'
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

const buildConversationCursorFilter = (cursor) => {
  if (!cursor?.lastMessageTime) return {};

  const cursorTime = new Date(cursor.lastMessageTime);
  if (Number.isNaN(cursorTime.getTime())) return {};

  return {
    $or: [
      { lastMessageTime: { $lt: cursorTime } },
      {
        lastMessageTime: cursorTime,
        _id: cursor.id ? { $lt: new mongoose.Types.ObjectId(cursor.id) } : { $exists: true }
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

const attachContactSnapshotsToConversations = async (conversations = [], req) => {
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
    .select(TEAM_INBOX_CONTACT_SUMMARY_FIELDS)
    .lean();

  const contactById = new Map(
    contacts.map((contact) => [String(contact?._id || '').trim(), contact])
  );

  return safeConversations.map((conversation) => {
    const contactId = String(conversation?.contactId || '').trim();
    const contact = contactById.get(contactId);
    return {
      ...conversation,
      contactId: contact || conversation?.contactId || null,
      unreadCount: Math.max(0, Number(conversation?.unreadCount || 0) || 0)
    };
  });
};

const loadConversationSummaryRows = async ({ finalFilters, limit, queryHint = null }) => {
  let query = ConversationSummary.find(finalFilters)
    .select(TEAM_INBOX_CONVERSATION_FIELDS)
    .sort({ lastMessageTime: -1, _id: -1 })
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
      .sort({ lastMessageTime: -1, _id: -1 })
      .lean();

    if (limit > 0) {
      fallbackQuery = fallbackQuery.limit(limit + 1);
    }

    conversations = await fallbackQuery;
  }

  let hasMore = false;
  if (limit > 0 && conversations.length > limit) {
    hasMore = true;
    return {
      conversations: conversations.slice(0, limit),
      hasMore,
      nextCursor: encodeConversationCursor(conversations[limit - 1])
    };
  }

  return {
    conversations,
    hasMore,
    nextCursor: conversations.length
      ? encodeConversationCursor(conversations[conversations.length - 1])
      : null
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

const encodeConversationCursorCacheKey = (cursor = null) => {
  if (!cursor?.lastMessageTime) return '';

  const lastMessageTime = new Date(cursor.lastMessageTime);
  if (Number.isNaN(lastMessageTime.getTime())) return '';

  return `${lastMessageTime.toISOString()}::${String(cursor.id || '').trim()}`;
};

const loadConversationFallbackRows = async ({ finalFilters, limit }) => {
  let query = Conversation.find(finalFilters)
    .select(TEAM_INBOX_CONVERSATION_FIELDS)
    .sort({ lastMessageTime: -1, _id: -1 })
    .lean();

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
    conversations: rawItems,
    hasMore,
    nextCursor: hasMore ? encodeConversationCursor(rawItems[rawItems.length - 1]) : null
  };
};

const loadConversationSummaryPage = async ({
  summaryFilters,
  fallbackFilters,
  limit,
  req,
  scope,
  cacheKeyParts,
  queryHint = null
}) => {
  const loadSummaryRows = async () => {
    const summaryRows = await loadConversationSummaryRows({
      finalFilters: summaryFilters,
      limit,
      queryHint
    });

    if (summaryRows.conversations.length) {
      return summaryRows;
    }

    return loadConversationFallbackRows({
      finalFilters: fallbackFilters,
      limit
    });
  };

  const cachedSummaryRows = scope
    ? await getOrSetCachedJson({
        namespace: 'conversations',
        scope,
        versionGroup: 'summaryPages',
        keyParts: cacheKeyParts,
        ttlSeconds: CACHE_TTL_SECONDS.summaryPages,
        loader: loadSummaryRows
      })
    : await loadSummaryRows();

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

class ConversationController {
  async getConversations(req, res) {
    try {
      const { status, assignedTo, search } = req.query;
      const conversationFilter = normalizeConversationFilter(
        req.query?.filter || req.query?.conversationFilter || ''
      );
      const filters = buildScopedFilters(req, {}, { scope: req.query?.scope });
      const scopeVariants = getInboxScopeVariants({
        companyId: filters.companyId,
        userId: filters.userId || ''
      });
      const scope = filters.userId
        ? scopeVariants[scopeVariants.length - 1]
        : scopeVariants[0];

      if (status) filters.status = status;
      if (assignedTo) filters.assignedTo = assignedTo;

      const parsedLimit = Number(req.query?.limit);
      const limit = Number.isFinite(parsedLimit) ? Math.max(1, Math.min(parsedLimit, 200)) : 0;
      const cursor = decodeConversationCursor(req.query?.cursor);
      const normalizedSearch = String(search || '').trim();
      const normalizedSearchLower = normalizedSearch.toLowerCase();
      const searchPlan = buildInboxSearchPlan(normalizedSearch);
      const cacheKeyParts = [
        String(status || '').trim(),
        String(assignedTo || '').trim(),
        conversationFilter,
        normalizedSearchLower,
        String(limit || 0),
        encodeConversationCursorCacheKey(cursor)
      ];
      const summaryFilters = { ...filters };
      const fallbackFilters = { ...filters };
      const summaryFilterClauses = [];
      const fallbackFilterClauses = [];
      const queryHint = searchPlan.hint;
      const unreadFilterClause = buildConversationUnreadFilterClause(conversationFilter);
      if (cursor) {
        const cursorFilter = buildConversationCursorFilter(cursor);
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
                queryHint
              });

              return {
                success: true,
                data: summaryPage.conversations,
                meta: {
                  limit: limit || null,
                  hasMore: summaryPage.hasMore,
                  nextCursor: summaryPage.nextCursor
                }
              };
            }
          })
        : null;

      if (cachedResponse) {
        return res.json(cachedResponse);
      }

      return res.json({
        success: true,
        data: [],
        meta: {
          limit: limit || null,
          hasMore: false,
          nextCursor: null
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
      const filters = buildScopedFilters(req, {}, { scope: req.query?.scope });
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

      const totalCount = await Contact.countDocuments(filters);
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
      
      res.setHeader('x-total-count', String(totalCount));
      res.json({
        success: true,
        data: filteredContacts,
        meta: {
          limit,
          hasMore,
          nextCursor: hasMore ? encodeContactCursor(filteredContacts[filteredContacts.length - 1]) : null
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
      // Get all unique contacts from conversations
      const conversations = await Conversation.find({
        ...buildScopedFilters(req, {}, { scope: req.query?.scope }),
        status: { $in: ['active', 'pending'] }
      })
        .select('contactPhone contactName lastMessageTime lastMessage status')
        .sort({ lastMessageTime: -1 })
        .lean();
      
      // Extract unique contacts
      const uniqueContacts = [];
      const seenPhones = new Set();
      
      conversations.forEach(conv => {
        if (!seenPhones.has(conv.contactPhone)) {
          uniqueContacts.push({
            phone: conv.contactPhone,
            name: conv.contactName || conv.contactPhone,
            lastMessageTime: conv.lastMessageTime,
            lastMessage: conv.lastMessage,
            status: conv.status
          });
          seenPhones.add(conv.contactPhone);
        }
      });
      
      res.json({ success: true, data: uniqueContacts });
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
