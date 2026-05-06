const mongoose = require('mongoose');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const Message = require('../models/Message');
const {
  normalizeRole,
  isTenantWideRole
} = require('../utils/accessControl');
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

const enrichConversationsWithLatestMessage = async (req, conversations) => {
  const safeConversations = Array.isArray(conversations) ? conversations : [];
  if (!safeConversations.length) {
    return safeConversations;
  }

  const conversationIds = safeConversations
    .map((conversation) => String(conversation?._id || '').trim())
    .filter((conversationId) => Boolean(conversationId) && mongoose.Types.ObjectId.isValid(conversationId))
    .map((conversationId) => new mongoose.Types.ObjectId(conversationId));

  if (!conversationIds.length) {
    return safeConversations;
  }

  const latestMessages = await Message.aggregate([
    {
      $match: {
        ...buildScopedFilters(req, {
          conversationId: { $in: conversationIds }
        })
      }
    },
    { $sort: { timestamp: -1, _id: -1 } },
    {
      $group: {
        _id: '$conversationId',
        latestMessage: { $first: '$$ROOT' }
      }
    }
  ]);

  const latestMessageByConversationId = new Map(
    (Array.isArray(latestMessages) ? latestMessages : [])
      .map((entry) => [String(entry?._id || '').trim(), entry?.latestMessage || null])
      .filter(([conversationId, latestMessage]) => conversationId && latestMessage)
  );

  return safeConversations.map((conversation) => {
    const conversationId = String(conversation?._id || '').trim();
    const latestMessage = latestMessageByConversationId.get(conversationId);

    if (!latestMessage) {
      return conversation;
    }

    return {
      ...conversation,
      lastMessageFrom: String(latestMessage?.sender || conversation?.lastMessageFrom || '')
        .trim()
        .toLowerCase(),
      lastMessageStatus:
        String(latestMessage?.sender || '').trim().toLowerCase() === 'agent'
          ? String(latestMessage?.status || conversation?.lastMessageStatus || 'sent')
              .trim()
              .toLowerCase()
          : String(conversation?.lastMessageStatus || '').trim().toLowerCase(),
      lastMessageWhatsappMessageId:
        String(latestMessage?.whatsappMessageId || '').trim() ||
        String(conversation?.lastMessageWhatsappMessageId || '').trim(),
      lastMessageTime:
        latestMessage?.timestamp ||
        conversation?.lastMessageTime ||
        latestMessage?.createdAt ||
        conversation?.updatedAt
    };
  });
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
      
      if (status) filters.status = status;
      if (assignedTo) filters.assignedTo = assignedTo;
      
      let conversations = await Conversation.find(filters)
        .select(TEAM_INBOX_CONVERSATION_FIELDS)
        .populate('contactId', TEAM_INBOX_CONTACT_FIELDS)
        .sort({ lastMessageTime: -1 })
        .lean();

      conversations = await enrichConversationsWithLatestMessage(req, conversations);

      conversations = conversations.map((conv) => {
        const fromConversation = Number(conv.unreadCount || 0);
        conv.unreadCount = Number.isFinite(fromConversation) ? Math.max(0, fromConversation) : 0;
        return conv;
      });
      
      // Apply search filter if provided
      if (search) {
        const searchLower = search.toLowerCase();
        conversations = conversations.filter(conv => 
          conv.contactName?.toLowerCase().includes(searchLower) ||
          conv.contactPhone?.includes(search) ||
          conv.lastMessage?.toLowerCase().includes(searchLower)
        );
      }
      
      res.json({ success: true, data: conversations });
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
      const page = Math.max(1, Number(req.query?.page || 1));
      const skip = (page - 1) * limit;
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
      const contacts = await Contact.find(filters)
        .select(CONTACT_LIST_FIELDS)
        .sort({ lastContact: -1, lastInboundMessageAt: -1, createdAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean();

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
      res.json({ success: true, data: filteredContacts });
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
      
      res.json({ success: true, message: 'Conversation deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteAllConversations(req, res) {
    try {
      await Conversation.deleteMany(buildScopedFilters(req));
      await Message.deleteMany(buildScopedFilters(req));
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
