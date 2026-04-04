const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const Message = require('../models/Message');
const mongoose = require('mongoose');

class ConversationController {
  async getConversations(req, res) {
    try {
      const { status, assignedTo, search } = req.query;
      const filters = { userId: req.user.id, companyId: req.companyId };
      
      if (status) filters.status = status;
      if (assignedTo) filters.assignedTo = assignedTo;
      
      let conversations = await Conversation.find(filters)
        .populate('contactId')
        .sort({ lastMessageTime: -1 });

      const conversationIds = conversations.map((conv) => conv._id);
      const userMatchId = mongoose.Types.ObjectId.isValid(req.user.id)
        ? new mongoose.Types.ObjectId(req.user.id)
        : req.user.id;

      const unreadRows = await Message.aggregate([
        {
          $match: {
            userId: userMatchId,
            companyId: req.companyId,
            conversationId: { $in: conversationIds },
            sender: 'contact',
            status: 'received'
          }
        },
        {
          $group: {
            _id: '$conversationId',
            unreadCount: { $sum: 1 }
          }
        }
      ]);

      const unreadMap = new Map(
        unreadRows.map((row) => [String(row._id), Number(row.unreadCount) || 0])
      );

      conversations = conversations.map((conv) => {
        const fromMessages = unreadMap.get(String(conv._id));
        const fromConversation = Number(conv.unreadCount || 0);
        conv.unreadCount = Number.isFinite(fromMessages)
          ? Math.max(0, fromMessages)
          : Number.isFinite(fromConversation)
            ? Math.max(0, fromConversation)
            : 0;
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
      const filters = { userId: req.user.id, companyId: req.companyId };
      
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
      
      const contacts = await Contact.find(filters)
        .sort({ lastContact: -1, createdAt: -1 });
      
      res.json({ success: true, data: contacts });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async getContactById(req, res) {
    try {
      const { id } = req.params;
      
      const contact = await Contact.findOne({ _id: id, userId: req.user.id, companyId: req.companyId });
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
        userId: req.user.id,
        companyId: req.companyId,
        status: { $in: ['active', 'pending'] }
      })
        .select('contactPhone contactName')
        .sort({ lastMessageTime: -1 });
      
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
      const existingContact = await Contact.findOne({ phone, userId: req.user.id, companyId: req.companyId });
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
      const contact = await Contact.findOne({ _id: id, userId: req.user.id, companyId: req.companyId });
      if (!contact) {
        return res.status(404).json({ 
          success: false, 
          error: 'Contact not found' 
        });
      }
      
      // Check if phone number is being changed and if it conflicts with existing contact
      if (phone && phone !== contact.phone) {
        const existingContact = await Contact.findOne({ phone, _id: { $ne: id }, userId: req.user.id, companyId: req.companyId });
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
        { _id: id, userId: req.user.id, companyId: req.companyId },
        updateData, 
        { new: true, runValidators: true }
      );
      
      // If name was updated, also update all conversations for this contact
      if (name !== undefined && name !== contact.name) {
        await Conversation.updateMany(
          { contactId: id, userId: req.user.id },
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
      
      const contact = await Contact.findOne({ _id: id, userId: req.user.id, companyId: req.companyId });
      if (!contact) {
        return res.status(404).json({ 
          success: false, 
          error: 'Contact not found' 
        });
      }
      
      await Contact.deleteOne({ _id: id, userId: req.user.id });
      
      res.json({ success: true, message: 'Contact deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteConversation(req, res) {
    try {
      const { id } = req.params;
      
      const conversation = await Conversation.findOne({ _id: id, userId: req.user.id });
      if (!conversation) {
        return res.status(404).json({ 
          success: false, 
          error: 'Conversation not found' 
        });
      }
      
      await Conversation.deleteOne({ _id: id, userId: req.user.id });
      await Message.deleteMany({ conversationId: id, userId: req.user.id });
      
      res.json({ success: true, message: 'Conversation deleted successfully' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  async deleteAllConversations(req, res) {
    try {
      await Conversation.deleteMany({ userId: req.user.id });
      await Message.deleteMany({ userId: req.user.id });
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
      
      await Conversation.deleteMany({ _id: { $in: conversationIds }, userId: req.user.id });
      await Message.deleteMany({ conversationId: { $in: conversationIds }, userId: req.user.id });
      
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
