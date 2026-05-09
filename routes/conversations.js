const express = require('express');
const router = express.Router();
const conversationController = require('../controllers/conversationController');
const Message = require('../models/Message');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');
const {
  buildChronologicalPage,
  buildMessageCursorFilter,
  decodeMessageCursor,
  encodeMessageCursor,
  normalizePageLimit
} = require('../utils/threadPagination');
const {
  CACHE_TTL_SECONDS,
  getInboxScopeVariants,
  getOrSetCachedJson,
  invalidateInboxConversation
} = require('../utils/teamInboxCache');
const {
  normalizeRole,
  isTenantWideRole
} = require('../utils/accessControl');

const buildScopedMessageFilters = (req, extra = {}) => {
  const normalizedRole = normalizeRole(
    req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role
  );
  const normalizedCompanyId = String(req?.companyId || req?.user?.companyId || '').trim();
  const filters = {
    ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
    ...extra
  };

  if (!isTenantWideRole(normalizedRole)) {
    filters.userId = req.user.id;
  }

  return filters;
};

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

    const limit = normalizePageLimit(req.query?.limit);
    const cursor = decodeMessageCursor(req.query?.cursor);
    const normalizedCompanyId = String(req.companyId || req.user?.companyId || '').trim();
    const scopeVariants = getInboxScopeVariants({
      companyId: normalizedCompanyId,
      userId: req.user?.id || ''
    });
    const scope = scopeVariants[scopeVariants.length - 1] || scopeVariants[0] || '';
    const threadScope = scope ? `${scope}:${conversationId}` : '';
    const queryCursor = cursor ? buildMessageCursorFilter(cursor) : {};
    const queryFilters = buildScopedMessageFilters(req, {
      conversationId,
      ...queryCursor
    });

    const loadMessages = async (filters) =>
      Message.find(filters)
        .select(
          '_id conversationId sender senderName text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment replyTo replyToMessageId errorMessage'
        )
        .populate(
          'replyTo',
          '_id text sender whatsappMessageId mediaType mediaCaption timestamp attachment'
        )
        .sort({ timestamp: -1, _id: -1 })
        .limit(limit + 1)
        .lean();

    const loadScopedMessages = async () => {
      if (process.env.NODE_ENV !== 'production') {
        console.log('[conversations] thread filters', {
          conversationId,
          reqCompanyId: req.companyId || null,
          reqUserCompanyId: req.user?.companyId || null,
          normalizedCompanyId,
          queryFilters
        });
      }
      const scopedMessages = await loadMessages({
        ...queryFilters,
        ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {})
      });

      if (scopedMessages.length > 0 || !normalizedCompanyId) {
        return scopedMessages;
      }

      const companyWideMessages = await loadMessages({
        conversationId,
        ...queryCursor,
        companyId: normalizedCompanyId
      });

      if (companyWideMessages.length > 0) {
        return companyWideMessages;
      }

      return loadMessages({
        conversationId,
        ...queryCursor,
        $or: [
          { companyId: { $exists: false } },
          { companyId: null }
        ]
      });
    };

    const cachedResponse = threadScope
      ? await getOrSetCachedJson({
          namespace: 'messages',
          scope: threadScope,
          versionGroup: 'thread',
          keyParts: [String(limit), String(req.query?.cursor || '').trim()],
          ttlSeconds: CACHE_TTL_SECONDS.messages,
          loader: async () => {
            const messages = await loadScopedMessages();

            const page = buildChronologicalPage({
              documents: messages,
              limit,
              encodeCursor: encodeMessageCursor
            });

            return {
              data: page.items,
              meta: {
                limit,
                hasMore: page.hasMore,
                nextCursor: page.nextCursor
              }
            };
          }
        })
      : null;

    if (cachedResponse) {
      const cachedItems = Array.isArray(cachedResponse?.data) ? cachedResponse.data : [];
      if (cachedItems.length > 0) {
        return res.json(cachedResponse);
      }

      const totalMessagesForConversation = await Message.countDocuments({ conversationId });
      if (totalMessagesForConversation > 0) {
        await invalidateInboxConversation({
          companyId: req.companyId || '',
          userId: req.user?.id || '',
          conversationId
        });
      } else {
        return res.json(cachedResponse);
      }
    }

    const messages = await loadScopedMessages();

    const page = buildChronologicalPage({
      documents: messages,
      limit,
      encodeCursor: encodeMessageCursor
    });

    return res.json({
      data: page.items,
      meta: {
        limit,
        hasMore: page.hasMore,
        nextCursor: page.nextCursor
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
