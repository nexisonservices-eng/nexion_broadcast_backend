const express = require('express');
const router = express.Router();
const conversationController = require('../controllers/conversationController');
const Message = require('../models/Message');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');

router.use(auth);
router.use(
  requireTenantPolicy({
    requiredFeatures: ['teamInbox', 'contacts'],
    auditEvent: 'conversation_policy'
  })
);

// Get all conversations with optional filters
router.get('/', (req, res) => conversationController.getConversations(req, res));

// Get all contacts with optional filters
router.get('/contacts', (req, res) => conversationController.getContacts(req, res));

// Get unique contacts from conversations (for broadcast)
router.get('/contacts/unique', (req, res) => conversationController.getConversationContacts(req, res));

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

// Get paged messages for a conversation (compatibility path used by Team Inbox clients)
router.get('/:id/messages', async (req, res) => {
  try {
    const conversationId = String(req.params.id || '').trim();
    if (!conversationId) {
      return res.status(400).json({
        success: false,
        error: 'conversationId is required'
      });
    }

    const parsedLimit = Number(req.query?.limit);
    const limit = Number.isFinite(parsedLimit)
      ? Math.max(1, Math.min(parsedLimit, 80))
      : 30;
    const cursor = String(req.query?.cursor || '').trim();

    const filters = {
      conversationId,
      companyId: req.companyId
    };

    const normalizedCursor = String(cursor || '').trim();
    if (normalizedCursor) {
      const cursorDate = new Date(normalizedCursor);
      if (!Number.isNaN(cursorDate.valueOf())) {
        filters.timestamp = { $lt: cursorDate };
      }
    }

    const messages = await Message.find(filters)
      .sort({ timestamp: -1, _id: -1 })
      .limit(limit + 1)
      .lean();

    const hasMore = messages.length > limit;
    const trimmed = hasMore ? messages.slice(0, limit) : messages;
    const chronologicalMessages = [...trimmed].reverse();
    const nextCursor = hasMore
      ? new Date(trimmed[trimmed.length - 1]?.timestamp || Date.now()).toISOString()
      : null;

    return res.json({
      data: chronologicalMessages,
      meta: {
        limit,
        hasMore,
        nextCursor
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Delete a single conversation (must come after specific routes)
router.delete('/:id', (req, res) => conversationController.deleteConversation(req, res));

module.exports = router;
