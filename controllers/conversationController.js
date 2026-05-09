const mongoose = require('mongoose');
const Conversation = require('../models/Conversation');
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

const TEAM_INBOX_CONTACT_FIELDS =
  '_id name phone email notes tags status stage customFields nextFollowUpAt leadScore leadScoreBreakdown isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata whatsappMarketingWindowStartedAt whatsappMarketingSendCount whatsappMarketingLastSentAt whatsappOptOutAt lastInboundMessageAt serviceWindowClosesAt';
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

const buildScopedFilters = (req, extra = {}) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const filters = {
    companyId: req.companyId,
    ...extra
  };

  if (!isTenantWideRole(normalizedRole)) {
    filters.userId = req.user.id;
  }

  return filters;
};

class ConversationController {
  async getConversations(req, res) {
    try {
      const { status, assignedTo, search } = req.query;
      const filters = buildScopedFilters(req);
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
      const finalFilters = cursor
        ? { ...filters, ...buildConversationCursorFilter(cursor) }
        : filters;

      const cachedResponse = scope
        ? await getOrSetCachedJson({
            namespace: 'conversations',
            scope,
            versionGroup: 'list',
            keyParts: [
              String(status || '').trim(),
              String(assignedTo || '').trim(),
              String(search || '').trim().toLowerCase(),
              String(limit || 0),
              String(cursor || '').trim()
            ],
            ttlSeconds: CACHE_TTL_SECONDS.conversations,
            loader: async () => {
              let query = Conversation.find(finalFilters)
                .select(TEAM_INBOX_CONVERSATION_FIELDS)
                .populate('contactId', TEAM_INBOX_CONTACT_FIELDS)
                .sort({ lastMessageTime: -1, _id: -1 })
                .lean();

              if (limit > 0) {
                query = query.limit(limit + 1);
              }

              let conversations = await query;

              conversations = (Array.isArray(conversations) ? conversations : []).map((conv) => {
                const fromConversation = Number(conv.unreadCount || 0);
                conv.unreadCount = Number.isFinite(fromConversation) ? Math.max(0, fromConversation) : 0;
                return conv;
              });

              if (search) {
                const searchLower = String(search || '').toLowerCase();
                conversations = conversations.filter(
                  (conv) =>
                    conv.contactName?.toLowerCase().includes(searchLower) ||
                    conv.contactPhone?.includes(search) ||
                    conv.lastMessage?.toLowerCase().includes(searchLower)
                );
              }

              let hasMore = false;
              if (limit > 0 && conversations.length > limit) {
                hasMore = true;
                conversations = conversations.slice(0, limit);
              }

              return {
                success: true,
                data: conversations,
                meta: {
                  limit: limit || null,
                  hasMore,
                  nextCursor: hasMore ? encodeConversationCursor(conversations[conversations.length - 1]) : null
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
      const filters = buildScopedFilters(req);
      
      if (search) {
        filters.$or = [
          { name: { $regex: search, $options: 'i' } },
          { phone: { $regex: search, $options: 'i' } },
          { email: { $regex: search, $options: 'i' } }
        ];
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
      let contacts = await Contact.find(cursorFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort({ lastContact: -1, createdAt: -1, _id: -1 })
        .limit(limit + 1)
        .lean();

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
      
      const contact = await Contact.findOne(buildScopedFilters(req, { _id: id })).lean();
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
        ...buildScopedFilters(req),
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
      const existingContact = await Contact.findOne(buildScopedFilters(req, { phone }));
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
      const contact = await Contact.findOne(buildScopedFilters(req, { _id: id }));
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
        buildScopedFilters(req, { _id: id }),
        updateData, 
        { new: true, runValidators: true }
      );
      
      // If name was updated, also update all conversations for this contact
      if (name !== undefined && name !== contact.name) {
        await Conversation.updateMany(
          buildScopedFilters(req, { contactId: id }),
          { contactName: name }
        );
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
      
      const contact = await Contact.findOne(buildScopedFilters(req, { _id: id }));
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
      
      const conversation = await Conversation.findOne(buildScopedFilters(req, { _id: id }));
      if (!conversation) {
        return res.status(404).json({ 
          success: false, 
          error: 'Conversation not found' 
        });
      }
      
      await Conversation.deleteOne(buildScopedFilters(req, { _id: id }));
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
