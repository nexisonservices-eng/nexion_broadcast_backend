const {
  validateFreeformOutboundSend
} = require('../services/whatsappOutreach/policy');
const {
  getConversationReadState,
  recordConversationRead,
  shouldSkipConversationReadUpdate
} = require('../utils/conversationReadStateCache');
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
  invalidateInboxConversation,
  invalidateInboxScope
} = require('../utils/teamInboxCache');
const {
  normalizeRole,
  isTenantWideRole
} = require('../utils/accessControl');
const { buildContactSearchPlan } = require('../utils/contactSearchPlan');
const {
  buildConversationPhoneLookupFilter,
  dedupeConversationsByIdentity
} = require('../utils/conversationIdentity');
const {
  syncConversationSummaryFromConversation,
  upsertConversationSummaries,
  upsertConversationSummary
} = require('../services/conversationSummaryService');
const ConversationSummary = require('../models/ConversationSummary');
const { buildInboxSearchPlan } = require('../utils/inboxSearchPlan');

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

const buildCompanyWideMessageFilters = (req, extra = {}) => {
  const normalizedCompanyId = String(req?.companyId || req?.user?.companyId || '').trim();
  return {
    ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
    ...extra
  };
};

const registerLegacyCoreRoutes = (app, deps) => {
  const {
    auth,
    requirePlanFeature,
    requireWhatsAppCredentials,
    whatsappService,
    getLeadScoringSettings,
    updateLeadScoringSettings,
    Contact,
    Conversation,
    Message,
    Broadcast,
    broadcastService,
    getWhatsAppCredentialsForUser,
    mongoose,
    ENABLE_DEBUG_LOGS,
    emitRealtimeEvent
  } = deps;

  const normalizePhoneDigits = (value = '') => String(value || '').replace(/\D/g, '');
  const isValidObjectId = (value = '') => /^[a-f\d]{24}$/i.test(String(value || '').trim());
  const buildCompanyScopeFilter = (companyId) => (companyId ? { companyId } : {});

  const resolveReplyReferenceForOutboundSend = async ({
    userId,
    companyId,
    replyToMessageId,
    whatsappContextMessageId
  }) => {
    const normalizedReplyToMessageId = String(replyToMessageId || '').trim();
    if (normalizedReplyToMessageId && isValidObjectId(normalizedReplyToMessageId)) {
      const baseIdFilter = { _id: normalizedReplyToMessageId, userId };
      const byScopeReply = await Message.findOne({
        ...baseIdFilter,
        ...buildCompanyScopeFilter(companyId)
      })
        .select('_id whatsappMessageId')
        .lean();
      if (byScopeReply) return byScopeReply;
    }

    const normalizedContextId = String(whatsappContextMessageId || '').trim();
    if (!normalizedContextId) return null;

    const baseContextFilter = { userId, whatsappMessageId: normalizedContextId };
    return Message.findOne({
      ...baseContextFilter,
      ...buildCompanyScopeFilter(companyId)
    })
      .select('_id whatsappMessageId')
      .lean();
  };

  const resolveConversationForOutboundSend = async ({ req, conversationId, to }) => {
    const userId = req?.user?.id;
    if (!userId || !conversationId) return null;

    const baseIdQuery = { _id: conversationId, userId };
    const byScopeConversation = await Conversation.findOne({
      ...baseIdQuery,
      ...buildCompanyScopeFilter(req.companyId)
    });
    if (byScopeConversation) return byScopeConversation;

    const phoneLookupFilter = buildConversationPhoneLookupFilter(to);
    if (!phoneLookupFilter) return null;

    return Conversation.findOne({
      userId,
      ...buildCompanyScopeFilter(req.companyId),
      ...phoneLookupFilter
    }).sort({ lastMessageTime: -1, updatedAt: -1, createdAt: -1 });
  };

  const resolveContactForConversation = async ({ userId, companyId, conversation }) => {
    if (!conversation?._id || !userId) return null;

    if (conversation.contactId) {
      const contactById = await Contact.findOne({
        _id: conversation.contactId,
        userId,
        ...buildCompanyScopeFilter(companyId)
      });

      if (contactById) return contactById;
    }

    const normalizedPhone = normalizePhoneDigits(conversation.contactPhone);
    if (!normalizedPhone) return null;

    const phoneCandidates = Array.from(
      new Set(
        [
          String(conversation.contactPhone || '').trim(),
          normalizedPhone,
          `+${normalizedPhone}`,
          normalizedPhone.length > 10 ? normalizedPhone.slice(-10) : ''
        ].filter(Boolean)
      )
    );

    return Contact.findOne({
      userId,
      ...buildCompanyScopeFilter(companyId),
      phone: { $in: phoneCandidates }
    });
  };

  const hydrateContactWithLatestInboundActivity = async ({
    userId,
    companyId,
    conversation,
    contact
  }) => {
    if (!contact || !conversation?._id || !userId) return contact;

    const latestInboundMessage = await Message.findOne({
      userId,
      ...buildCompanyScopeFilter(companyId || conversation.companyId || null),
      conversationId: conversation._id,
      sender: 'contact'
    })
      .sort({ timestamp: -1, createdAt: -1, _id: -1 })
      .select('timestamp whatsappTimestamp')
      .lean();

    const inboundActivityAt = latestInboundMessage?.whatsappTimestamp || latestInboundMessage?.timestamp;
    if (!inboundActivityAt) return contact;

    const inboundDate = new Date(inboundActivityAt);
    if (Number.isNaN(inboundDate.getTime())) return contact;

    const currentInboundAt = contact.lastInboundMessageAt
      ? new Date(contact.lastInboundMessageAt)
      : null;
    const hasCurrentInbound = currentInboundAt && !Number.isNaN(currentInboundAt.getTime());
    if (hasCurrentInbound && currentInboundAt.getTime() >= inboundDate.getTime()) {
      return contact;
    }

    const hydratedContact = contact;
    hydratedContact.lastInboundMessageAt = inboundDate;
    hydratedContact.serviceWindowClosesAt = new Date(inboundDate.getTime() + 24 * 60 * 60 * 1000);
    return hydratedContact;
  };

  app.post(
    '/api/messages/send',
    auth,
    requirePlanFeature('broadcastMessaging'),
    requireWhatsAppCredentials,
    async (req, res) => {
      try {
        const {
          to,
          text,
          conversationId,
          mediaUrl,
          mediaType,
          replyToMessageId = '',
          whatsappContextMessageId = ''
        } = req.body || {};

        if (ENABLE_DEBUG_LOGS) {
          console.log('DEBUG /api/messages/send called with:', {
            hasTo: Boolean(to),
            textLength: String(text || '').length,
            conversationId: String(conversationId || ''),
            hasMediaUrl: Boolean(mediaUrl),
            mediaType: String(mediaType || '')
          });
        }

        const conversation = await resolveConversationForOutboundSend({
          req,
          conversationId,
          to
        });
        if (!conversation) {
          return res.status(400).json({
            success: false,
            error: 'Conversation not found for provided conversationId'
          });
        }

        const outboundContact = await resolveContactForConversation({
          userId: req.user.id,
          companyId: conversation.companyId || req.companyId || null,
          conversation
        });
        const policyContact = outboundContact
          ? await hydrateContactWithLatestInboundActivity({
              userId: req.user.id,
              companyId: conversation.companyId || req.companyId || null,
              conversation,
              contact: outboundContact
            })
          : null;
        const freeformValidation = outboundContact
          ? validateFreeformOutboundSend(policyContact || outboundContact)
          : { ok: true, policy: null };
        if (!freeformValidation.ok) {
          return res.status(freeformValidation.statusCode || 403).json({
            success: false,
            error: freeformValidation.error,
            policy: freeformValidation.policy
          });
        }

        const messageCompanyId = conversation.companyId || req.companyId || null;
        const replyReference = await resolveReplyReferenceForOutboundSend({
          userId: req.user.id,
          companyId: messageCompanyId,
          replyToMessageId,
          whatsappContextMessageId
        });
        const resolvedReplyContextMessageId =
          String(whatsappContextMessageId || replyReference?.whatsappMessageId || '').trim();

        let result;
        if (mediaUrl && mediaType) {
          result = await whatsappService.sendMediaMessage(
            to,
            mediaType,
            mediaUrl,
            text,
            req.whatsappCredentials,
            {
              whatsappContextMessageId: resolvedReplyContextMessageId,
              allowLinkFallback: true
            }
          );
        } else {
          result = await whatsappService.sendTextMessage(to, text, req.whatsappCredentials, {
            whatsappContextMessageId: resolvedReplyContextMessageId
          });
        }

        if (!result.success) {
          return res.status(400).json({ success: false, error: result.error });
        }

        const whatsappMessageId =
          result?.data?.messages?.[0]?.id || result?.data?.messageId || null;

        const message = await Message.create({
          userId: req.user.id,
          companyId: messageCompanyId,
          conversationId: conversation._id,
          sender: 'agent',
          text,
          mediaUrl,
          mediaType,
          replyTo: replyReference?._id || undefined,
          whatsappContextMessageId: resolvedReplyContextMessageId || undefined,
          whatsappMessageId,
          status: 'sent',
          timestamp: new Date()
        });
        await message.populate('replyTo', '_id text sender whatsappMessageId mediaType mediaCaption timestamp');

        await Conversation.updateOne(
          { _id: conversation._id },
          {
            lastMessageTime: new Date(),
            lastMessage: text,
            lastMessageMediaType: String(mediaType || '').trim(),
            lastMessageAttachmentName: '',
            lastMessageAttachmentPages: null,
            lastMessageFrom: 'agent',
            lastMessageWhatsappMessageId: whatsappMessageId || '',
            lastMessageStatus: 'sent'
          }
        );
        await upsertConversationSummary({
          conversationId: conversation._id,
          userId: req.user.id,
          companyId: messageCompanyId,
          contactId: conversation.contactId,
          contactPhone: conversation.contactPhone,
          contactName: conversation.contactName,
          status: conversation.status,
          assignedTo: conversation.assignedTo,
          assignedToId: conversation.assignedToId,
          tags: conversation.tags,
          priority: conversation.priority,
          lastMessageTime: new Date(),
          lastMessage: text,
          lastMessageMediaType: String(mediaType || '').trim(),
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent',
          unreadCount: conversation.unreadCount,
          notes: conversation.notes,
          resolvedAt: conversation.resolvedAt
        });
        await invalidateInboxConversation({
          companyId: messageCompanyId || '',
          userId: req.user.id || '',
          conversationId: conversation._id
        });

        emitRealtimeEvent(req.user.id, {
          type: 'message_sent',
          message: message.toObject()
        });

        return res.json({ success: true, message });
      } catch (error) {
        console.error('Send message error:', error);
        return res.status(500).json({ success: false, error: error.message });
      }
    }
  );

  app.get('/api/lead-scoring/settings', auth, async (req, res) => {
    try {
      const settings = await getLeadScoringSettings({
        userId: req.user.id,
        companyId: req.companyId
      });

      res.json({
        success: true,
        data: settings
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  app.put('/api/lead-scoring/settings', auth, async (req, res) => {
    try {
      const updated = await updateLeadScoringSettings({
        userId: req.user.id,
        companyId: req.companyId,
        updatedBy: req.user.id,
        payload: req.body || {}
      });

      res.json({
        success: true,
        data: updated
      });
    } catch (error) {
      const statusCode =
        String(error.message || '').includes('No valid lead scoring fields provided') ? 400 : 500;
      res.status(statusCode).json({
        success: false,
        error: error.message
      });
    }
  });

  app.get('/api/conversations', auth, async (req, res) => {
    try {
      const { status, assignedTo, search } = req.query;
      const baseFilters = { userId: req.user.id, companyId: req.companyId };
      const searchPlan = buildInboxSearchPlan(search);

      if (status) baseFilters.status = status;
      if (assignedTo) baseFilters.assignedTo = assignedTo;

      if (searchPlan.summaryClause) {
        const summaryFilters = { ...baseFilters, ...searchPlan.summaryClause };
        let summaryQuery = ConversationSummary.find(summaryFilters)
          .select('conversationId lastMessageTime')
          .sort({ lastMessageTime: -1, _id: -1 })
          .limit(100)
          .lean();

        if (searchPlan.hint) {
          summaryQuery = summaryQuery.hint(searchPlan.hint);
        }

        const summaryRows = await summaryQuery;
        const summaryConversationIds = summaryRows
          .map((row) => String(row?.conversationId || '').trim())
          .filter(Boolean);

        if (summaryConversationIds.length) {
          const conversationFilters = {
            ...baseFilters,
            _id: { $in: summaryConversationIds }
          };
          const conversations = await Conversation.find(conversationFilters)
            .populate('contactId', 'name phone email tags')
            .lean();

          const conversationById = new Map(
            conversations.map((conversation) => [String(conversation?._id || '').trim(), conversation])
          );

          const orderedFromSummary = summaryConversationIds
            .map((conversationId) => conversationById.get(conversationId))
            .filter(Boolean);

          const enrichedFromSummary = await enrichConversationsWithLatestAgentStatus(
            orderedFromSummary,
            req
          );

          if (enrichedFromSummary.length >= 100 || !searchPlan.fallbackClause) {
            return res.json(enrichedFromSummary);
          }

          const remainingLimit = 100 - enrichedFromSummary.length;
          if (remainingLimit > 0) {
            const fallbackFilters = {
              ...baseFilters,
              ...(searchPlan.fallbackClause || {}),
              _id: { $nin: summaryConversationIds }
            };
            const fallbackConversations = await Conversation.find(fallbackFilters)
              .populate('contactId', 'name phone email tags')
              .sort({ lastMessageTime: -1 })
              .limit(remainingLimit)
              .lean();
            const enrichedFallback = await enrichConversationsWithLatestAgentStatus(
              fallbackConversations,
              req
            );
            const combined = [...enrichedFromSummary, ...enrichedFallback].sort((a, b) => {
              const aTime = new Date(a?.lastMessageTime || 0).getTime();
              const bTime = new Date(b?.lastMessageTime || 0).getTime();
              if (Number.isNaN(aTime) && Number.isNaN(bTime)) return 0;
              if (Number.isNaN(aTime)) return 1;
              if (Number.isNaN(bTime)) return -1;
              return bTime - aTime;
            });
            return res.json(combined.slice(0, 100));
          }
        }

        if (searchPlan.fallbackClause) {
          const fallbackFilters = {
            ...baseFilters,
            ...searchPlan.fallbackClause
          };
          const fallbackConversations = await Conversation.find(fallbackFilters)
            .populate('contactId', 'name phone email tags')
            .sort({ lastMessageTime: -1 })
            .limit(100)
            .lean();
          const enrichedFallback = await enrichConversationsWithLatestAgentStatus(
            fallbackConversations,
            req
          );
          return res.json(enrichedFallback);
        }
      }

      const conversations = await Conversation.find(baseFilters)
        .populate('contactId', 'name phone email tags')
        .sort({ lastMessageTime: -1 })
        .limit(100)
        .lean();
      const enrichedConversations = await enrichConversationsWithLatestAgentStatus(
        conversations,
        req
      );
      res.json(dedupeConversationsByIdentity(enrichedConversations));
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/conversations', auth, async (req, res) => {
    try {
      const { contactId, contactPhone, contactName, status } = req.body;

      const phoneLookupFilter = buildConversationPhoneLookupFilter(contactPhone);
      const existingConversation = await Conversation.findOne({
        userId: req.user.id,
        companyId: req.companyId,
        status: { $in: ['active', 'pending'] },
        ...(phoneLookupFilter || { contactPhone })
      }).sort({ lastMessageTime: -1, updatedAt: -1, createdAt: -1 });

      if (existingConversation) {
        return res.status(400).json({ error: 'Conversation already exists for this contact' });
      }

      const conversation = await Conversation.create({
        userId: req.user.id,
        companyId: req.companyId,
        contactId,
        contactPhone,
        contactName,
        status: status || 'active',
        lastMessageTime: new Date(),
        lastMessageWhatsappMessageId: '',
        unreadCount: 0
      });
      await syncConversationSummaryFromConversation(conversation);
      await invalidateInboxScope({
        companyId: req.companyId || '',
        userId: req.user.id || ''
      });

      return res.status(201).json(conversation);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/conversations/:id', auth, async (req, res) => {
    try {
      const conversation = await Conversation.findOne({
        _id: req.params.id,
        userId: req.user.id,
        companyId: req.companyId
      })
        .populate('contactId', 'name phone email tags')
        .lean();
      if (!conversation) {
        return res.status(404).json({ error: 'Conversation not found' });
      }
      return res.json(conversation);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.put('/api/conversations/:id', auth, async (req, res) => {
    try {
      const conversation = await Conversation.findOneAndUpdate(
        { _id: req.params.id, userId: req.user.id, companyId: req.companyId },
        req.body,
        { new: true }
      );
      if (!conversation) {
        return res.status(404).json({ error: 'Conversation not found' });
      }
      await syncConversationSummaryFromConversation(conversation);
      await invalidateInboxConversation({
        companyId: req.companyId || '',
        userId: req.user.id || '',
        conversationId: conversation._id
      });
      return res.json(conversation);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.put('/api/conversations/:id/read', auth, async (req, res) => {
    try {
      const conversationId = String(req.params.id || '').trim();
      if (!conversationId) {
        return res.status(400).json({ error: 'conversationId is required' });
      }

      const conversation = await Conversation.findOne({
        _id: conversationId,
        userId: req.user.id,
        companyId: req.companyId
      }).lean();
      if (!conversation) {
        return res.status(404).json({ error: 'Conversation not found' });
      }

      const scope = {
        companyId: req.companyId || '',
        userId: req.user.id || '',
        conversationId
      };
      const state = await getConversationReadState(scope);
      const latestInboundMessageId = String(state?.lastInboundMessageId || '').trim();
      const latestInboundAt = state?.lastInboundAt ? new Date(state.lastInboundAt) : null;
      const shouldSkipWrite =
        Number(conversation.unreadCount || 0) <= 0 &&
        shouldSkipConversationReadUpdate(state || {}, latestInboundMessageId);

      if (!shouldSkipWrite) {
        await Message.updateMany(
          {
            conversationId,
            userId: req.user.id,
            companyId: req.companyId,
            sender: 'contact',
            status: 'received'
          },
          { status: 'read' }
        );

        await Conversation.updateOne(
          { _id: conversationId, userId: req.user.id, companyId: req.companyId },
          { unreadCount: 0 }
        );
        await upsertConversationSummary({
          ...conversation,
          unreadCount: 0
        });
        await invalidateInboxConversation({
          companyId: req.companyId || '',
          userId: req.user.id || '',
          conversationId
        });

        await recordConversationRead({
          ...scope,
          latestInboundMessageId,
          latestInboundAt: latestInboundAt || new Date()
        });
      }

      emitRealtimeEvent(req.user.id, {
        type: 'conversation_read',
        conversationId,
        unreadCount: 0
      });

      return res.json({
        ...conversation,
        unreadCount: 0
      });
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/conversations/:id/messages', auth, async (req, res) => {
    try {
      const conversationId = String(req.params.id || '').trim();
      if (!conversationId) {
        return res.status(400).json({ error: 'conversationId is required' });
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
      const baseFilters = buildScopedMessageFilters(req, {
        conversationId,
        ...(cursor ? buildMessageCursorFilter(cursor) : {})
      });

      const loadMessages = async (filters) =>
        Message.find(filters)
          .select(
            '_id conversationId sender senderName text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment replyTo replyToMessageId errorMessage'
          )
          .sort({ timestamp: -1, _id: -1 })
          .limit(limit + 1)
          .lean();

      const loadScopedMessages = async () => {
        if (process.env.NODE_ENV !== 'production') {
          console.log('[legacyCoreRoutes] thread filters', {
            conversationId,
            reqCompanyId: req.companyId || null,
            reqUserCompanyId: req.user?.companyId || null,
            normalizedCompanyId,
            baseFilters
          });
        }
        const scopedMessages = await loadMessages({
          ...baseFilters,
          ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {})
        });

        if (scopedMessages.length > 0 || !normalizedCompanyId) {
          return scopedMessages;
        }

        const companyWideMessages = await loadMessages({
          conversationId,
          ...(cursor ? buildMessageCursorFilter(cursor) : {}),
          ...buildCompanyWideMessageFilters(req)
        });

        if (companyWideMessages.length > 0) {
          return companyWideMessages;
        }

        return loadMessages({
          ...baseFilters,
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
              if (messages.some((message) => String(message?.replyTo || '').trim())) {
                await Message.populate(messages, {
                  path: 'replyTo',
                  select: '_id text sender whatsappMessageId mediaType mediaCaption timestamp attachment'
                });
              }
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
        return res.json(cachedResponse);
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
      return res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/contacts', auth, async (req, res) => {
    try {
      const { search, tags } = req.query;
      const searchPlan = buildContactSearchPlan(search);
      const baseFilters = { userId: req.user.id, companyId: req.companyId };
      const filters = { ...baseFilters };

      if (searchPlan.summaryClause) {
        filters.$and = filters.$and ? [...filters.$and, searchPlan.summaryClause] : [searchPlan.summaryClause];
      }
      if (tags) {
        filters.tags = { $in: tags.split(',') };
      }

      let contactQuery = Contact.find(filters).sort({ lastContact: -1, createdAt: -1, _id: -1 }).lean();
      if (searchPlan.hint) {
        contactQuery = contactQuery.hint(searchPlan.hint);
      }

      let contacts = await contactQuery;

      if (!contacts.length && searchPlan.fallbackClause) {
        const fallbackFilters = {
          ...baseFilters,
          ...(tags ? { tags: { $in: tags.split(',') } } : {}),
          ...searchPlan.fallbackClause
        };
        let fallbackQuery = Contact.find(fallbackFilters)
          .sort({ lastContact: -1, createdAt: -1, _id: -1 })
          .lean();

        if (searchPlan.hint) {
          fallbackQuery = fallbackQuery.hint(searchPlan.hint);
        }

        contacts = await fallbackQuery;
      }

      res.json(contacts);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/contacts', auth, async (req, res) => {
    try {
      const contact = await Contact.create({
        ...req.body,
        userId: req.user.id,
        companyId: req.companyId,
        sourceType: 'manual'
      });
      res.json(contact);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.put('/api/contacts/:id', auth, async (req, res) => {
    try {
      const { id } = req.params;
      const { name, phone, email, tags, notes, isBlocked } = req.body;

      const currentContact = await Contact.findOne({ _id: id, userId: req.user.id, companyId: req.companyId });
      if (!currentContact) {
        return res.status(404).json({ error: 'Contact not found' });
      }

      if (phone && phone !== currentContact.phone) {
        const existingContact = await Contact.findOne({
          phone,
          _id: { $ne: id },
          userId: req.user.id,
          companyId: req.companyId
        });
        if (existingContact) {
          return res.status(400).json({ error: 'Another contact with this phone number already exists' });
        }
      }

      const updateData = {};
      if (name !== undefined) updateData.name = name;
      if (phone !== undefined) updateData.phone = phone;
      if (email !== undefined) updateData.email = email;
      if (tags !== undefined) updateData.tags = tags;
      if (notes !== undefined) updateData.notes = notes;
      if (isBlocked !== undefined) updateData.isBlocked = isBlocked;

      const contact = await Contact.findOneAndUpdate(
        { _id: id, userId: req.user.id, companyId: req.companyId },
        updateData,
        { new: true, runValidators: true }
      );

      const conversationUpdate = {};
      if (name !== undefined && name !== currentContact.name) {
        conversationUpdate.contactName = name;
      }
      if (phone !== undefined && phone !== currentContact.phone) {
        conversationUpdate.contactPhone = phone;
      }

      if (Object.keys(conversationUpdate).length > 0) {
        await Conversation.updateMany(
          { contactId: id, userId: req.user.id, companyId: req.companyId },
          conversationUpdate
        );
        const updatedConversations = await Conversation.find({
          contactId: id,
          userId: req.user.id,
          companyId: req.companyId
        })
          .select(
            '_id userId companyId contactId contactPhone contactName status assignedTo assignedToId tags priority lastMessageTime lastMessage lastMessageMediaType lastMessageAttachmentName lastMessageAttachmentPages lastMessageFrom lastMessageWhatsappMessageId lastMessageStatus unreadCount notes resolvedAt'
          )
          .lean();
        await upsertConversationSummaries(updatedConversations);
        await invalidateInboxScope({
          companyId: req.companyId || '',
          userId: req.user.id || ''
        });
      }

      return res.json(contact);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/debug-broadcasts', auth, async (req, res) => {
    try {
      const debugInServer = require('../debugInServer');
      await debugInServer();
      res.json({ success: true, message: 'Debug completed - check server logs' });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  });

  app.get('/api/analytics', auth, async (req, res) => {
    try {
      const analyticsScopeFilter = req.companyId
        ? {
            $or: [
              { companyId: req.companyId },
              { userId: req.user.id }
            ]
          }
        : { userId: req.user.id };
      const analyticsScopeKey = req.companyId
        ? `company:${String(req.companyId)}:user:${String(req.user.id)}`
        : `user:${String(req.user.id)}`;
      const last7Days = new Date();
      last7Days.setDate(last7Days.getDate() - 7);
      const last12Hours = new Date();
      last12Hours.setHours(last12Hours.getHours() - 12);
      const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      const hourlyLabelFormatter = new Intl.DateTimeFormat('en-US', { hour: 'numeric', hour12: true });

      const analytics = await getOrSetCachedJson({
        namespace: 'dashboard',
        scope: analyticsScopeKey,
        versionGroup: 'summary-v1',
        keyParts: ['broadcast-analytics'],
        ttlSeconds: 20,
        loader: async () => {
          const dailyBuckets = new Map();
          const hourlyBuckets = new Map();

          for (let i = 6; i >= 0; i -= 1) {
            const day = new Date();
            day.setHours(0, 0, 0, 0);
            day.setDate(day.getDate() - i);
            const key = day.toISOString().slice(0, 10);
            dailyBuckets.set(key, {
              date: dayNames[day.getDay()],
              sent: 0,
              delivered: 0,
              read: 0,
              conversations: 0
            });
          }

          for (let i = 11; i >= 0; i -= 1) {
            const hour = new Date();
            hour.setMinutes(0, 0, 0);
            hour.setHours(hour.getHours() - i);
            const key = hour.toISOString().slice(0, 13);
            hourlyBuckets.set(key, {
              hour: hourlyLabelFormatter.format(hour),
              messages: 0,
              conversations: 0
            });
          }

          const [
            totalConversations,
            activeConversations,
            messageTotals,
            campaignTotals,
            receivedCount,
            dailyTrendsAgg,
            hourlyActivityAgg,
            messageTypesAgg
          ] = await Promise.all([
            Conversation.countDocuments(analyticsScopeFilter),
            Conversation.countDocuments({
              ...analyticsScopeFilter,
              status: 'active'
            }),
            Message.aggregate([
              {
                $match: {
                  ...analyticsScopeFilter,
                  sender: 'agent'
                }
              },
              {
                $group: {
                  _id: null,
                  sent: { $sum: 1 },
                  delivered: {
                    $sum: {
                      $cond: [{ $in: ['$status', ['delivered', 'read']] }, 1, 0]
                    }
                  },
                  read: {
                    $sum: {
                      $cond: [{ $eq: ['$status', 'read'] }, 1, 0]
                    }
                  },
                  failed: {
                    $sum: {
                      $cond: [{ $eq: ['$status', 'failed'] }, 1, 0]
                    }
                  }
                }
              }
            ]),
            Broadcast.aggregate([
              {
                $match: analyticsScopeFilter
              },
              {
                $group: {
                  _id: null,
                  sent: { $sum: '$stats.sent' },
                  delivered: { $sum: '$stats.delivered' },
                  read: { $sum: '$stats.read' },
                  failed: { $sum: '$stats.failed' }
                }
              }
            ]),
            Message.countDocuments({
              ...analyticsScopeFilter,
              sender: 'contact',
              timestamp: { $gte: last7Days }
            }),
            Message.aggregate([
              {
                $match: {
                  ...analyticsScopeFilter,
                  sender: 'agent',
                  timestamp: { $gte: last7Days }
                }
              },
              {
                $group: {
                  _id: {
                    day: {
                      $dateToString: {
                        format: '%Y-%m-%d',
                        date: '$timestamp'
                      }
                    },
                    conversationId: '$conversationId'
                  },
                  sent: { $sum: 1 },
                  delivered: {
                    $sum: {
                      $cond: [{ $in: ['$status', ['delivered', 'read']] }, 1, 0]
                    }
                  },
                  read: {
                    $sum: {
                      $cond: [{ $eq: ['$status', 'read'] }, 1, 0]
                    }
                  }
                }
              },
              {
                $group: {
                  _id: '$_id.day',
                  sent: { $sum: '$sent' },
                  delivered: { $sum: '$delivered' },
                  read: { $sum: '$read' },
                  conversations: { $sum: 1 }
                }
              }
            ]),
            Message.aggregate([
              {
                $match: {
                  ...analyticsScopeFilter,
                  sender: 'agent',
                  timestamp: { $gte: last12Hours }
                }
              },
              {
                $group: {
                  _id: {
                    hour: {
                      $dateToString: {
                        format: '%Y-%m-%d-%H',
                        date: '$timestamp'
                      }
                    },
                    conversationId: '$conversationId'
                  },
                  messages: { $sum: 1 }
                }
              },
              {
                $group: {
                  _id: '$_id.hour',
                  messages: { $sum: '$messages' },
                  conversations: { $sum: 1 }
                }
              }
            ]),
            Message.aggregate([
              {
                $match: {
                  ...analyticsScopeFilter,
                  sender: 'agent',
                  timestamp: { $gte: last7Days }
                }
              },
              {
                $group: {
                  _id: {
                    mediaType: {
                      $cond: [
                        { $in: ['$mediaType', ['image', 'video', 'audio', 'document', 'sticker']] },
                        '$mediaType',
                        'text'
                      ]
                    }
                  },
                  value: { $sum: 1 }
                }
              }
            ])
          ]);

          (Array.isArray(dailyTrendsAgg) ? dailyTrendsAgg : []).forEach((item) => {
            const bucket = dailyBuckets.get(String(item?._id || '')) || null;
            if (!bucket) return;
            bucket.sent = Number(item?.sent || 0);
            bucket.delivered = Number(item?.delivered || 0);
            bucket.read = Number(item?.read || 0);
            bucket.conversations = Number(item?.conversations || 0);
          });

          (Array.isArray(hourlyActivityAgg) ? hourlyActivityAgg : []).forEach((item) => {
            const key = String(item?._id || '').trim();
            const bucket = hourlyBuckets.get(key) || null;
            if (!bucket) return;
            bucket.messages = Number(item?.messages || 0);
            bucket.conversations = Number(item?.conversations || 0);
          });

          const messageTypeCounts = {
            text: 0,
            image: 0,
            video: 0,
            audio: 0,
            document: 0,
            sticker: 0
          };
          (Array.isArray(messageTypesAgg) ? messageTypesAgg : []).forEach((item) => {
            const mediaType = String(item?._id?.mediaType || 'text').trim().toLowerCase();
            const count = Number(item?.value || 0);
            if (Object.prototype.hasOwnProperty.call(messageTypeCounts, mediaType)) {
              messageTypeCounts[mediaType] += count;
            } else {
              messageTypeCounts.text += count;
            }
          });

          const headlineTotals = messageTotals[0] || { sent: 0, delivered: 0, read: 0, failed: 0 };
          const broadcastTotals = campaignTotals[0] || { sent: 0, delivered: 0, read: 0, failed: 0 };
          const messagesFailed = Math.max(Number(headlineTotals.failed || 0), Number(broadcastTotals.failed || 0));
          const messagesRead = Math.max(Number(headlineTotals.read || 0), Number(broadcastTotals.read || 0));
          const totalDeliveredOrRead = Math.max(
            Number(headlineTotals.delivered || 0),
            Number(broadcastTotals.delivered || 0)
          );
          const messagesSent = Math.max(
            Number(headlineTotals.sent || 0),
            Number(broadcastTotals.sent || 0),
            totalDeliveredOrRead + messagesFailed
          );
          const deliveryRate = messagesSent > 0 ? ((totalDeliveredOrRead / messagesSent) * 100).toFixed(1) : '0.0';
          const readRate = messagesSent > 0 ? ((messagesRead / messagesSent) * 100).toFixed(1) : '0.0';

          const dailyTrends = Array.from(dailyBuckets.values());
          const hourlyActivity = Array.from(hourlyBuckets.values());

          return {
            totalConversations,
            activeConversations,
            messagesSent,
            messagesDelivered: totalDeliveredOrRead,
            messagesRead,
            messagesFailed,
            messagesReceived: Number(receivedCount || 0),
            avgResponseTime: '2m 34s',
            responseRate: messagesSent > 0 ? Math.round((messagesRead / messagesSent) * 100) : 0,
            customerSatisfaction: 4.7,
            sentGrowth: '12',
            deliveredGrowth: '8',
            readRateGrowth: '5',
            failedGrowth: '-2',
            dailyTrends,
            hourlyActivity,
            messageTypes: [
              { name: 'Text Messages', value: messageTypeCounts.text, color: '#3b82f6' },
              { name: 'Image Messages', value: messageTypeCounts.image, color: '#2563eb' },
              { name: 'Video Messages', value: messageTypeCounts.video, color: '#f59e0b' },
              { name: 'Sticker Messages', value: messageTypeCounts.sticker, color: '#a855f7' },
              {
                name: 'Document/Audio',
                value: messageTypeCounts.document + messageTypeCounts.audio,
                color: '#8b5cf6'
              }
            ],
            performanceMetrics: {
              avgDeliveryTime: '1.2 minutes',
              avgReadTime: '8.5 minutes',
              peakHour: hourlyActivity.length
                ? hourlyActivity.reduce((a, b) => (a.messages > b.messages ? a : b)).hour
                : 'N/A',
              bestDay: dailyTrends.length
                ? dailyTrends.reduce((a, b) => (a.sent > b.sent ? a : b)).date
                : 'N/A',
              deliveryRate: `${deliveryRate}%`,
              readRate: `${readRate}%`
            }
          };
        }
      });

      res.json(analytics);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/broadcasts/:id/sync', auth, async (req, res) => {
    try {
      const { id } = req.params;
      console.log('Manual sync requested for broadcast:', id);

      const ownsBroadcast = await Broadcast.findOne({ _id: id, createdById: req.user.id }).select('_id').lean();
      if (!ownsBroadcast) {
        return res.status(404).json({ success: false, error: 'Broadcast not found' });
      }

      const result = await broadcastService.syncBroadcastStats(id);
      if (result.success) {
        console.log('Manual sync completed:', result.data);
        return res.json(result);
      }

      console.log('Manual sync failed:', result.error);
      return res.status(400).json(result);
    } catch (error) {
      console.error('Error in manual sync:', error);
      return res.status(500).json({ success: false, error: error.message });
    }
  });

  app.get('/api/campaigns/test', (req, res) => {
    res.json({
      success: true,
      message: 'Campaign routes are working',
      timestamp: new Date().toISOString()
    });
  });

  app.get('/api/campaigns/debug/status', (req, res) => {
    try {
      const campaignRoutesExist = require.resolve('../routes/campaignroutes');
      const campaignControllerExists = require.resolve('../controllers/campaigncontroller');
      const campaignModelExists = require.resolve('../models/campaign');

      res.json({
        success: true,
        modules: {
          routes: !!campaignRoutesExist,
          controller: !!campaignControllerExists,
          model: !!campaignModelExists
        },
        mongodb: {
          connected: mongoose.connection.readyState === 1,
          database: mongoose.connection.name
        },
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  app.get('/health', (req, res) => {
    res.json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      services: {
        campaigns: 'active',
        websocket: 'active',
        database: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected'
      }
    });
  });

  app.get('/api/version', (req, res) => {
    res.json({
      service: 'whatsapp-backend',
      version: 'bulk_direct_meta_v2',
      timestamp: new Date().toISOString(),
      features: ['campaigns', 'broadcasts', 'meta-ads', 'websocket', 'lead-scoring', 'crm', 'google-calendar']
    });
  });

  app.get('/api/debug/auth-credentials', auth, async (req, res) => {
    try {
      const authHeader = req.headers.authorization || '';
      const credentials = await getWhatsAppCredentialsForUser({
        authHeader,
        userId: req.user?.id || null
      });

      res.json({
        success: true,
        user: req.user,
        hasAuthHeader: Boolean(authHeader),
        credentialsFound: Boolean(credentials),
        credentials: credentials
          ? {
              twilioId: credentials.twilioId,
              whatsappId: credentials.whatsappId,
              whatsappBusiness: credentials.whatsappBusiness,
              accessTokenPreview: `${String(credentials.accessToken).slice(0, 10)}...`
            }
          : null
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message,
        user: req.user
      });
    }
  });
};

module.exports = {
  registerLegacyCoreRoutes
};
