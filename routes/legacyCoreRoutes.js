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

  const resolveReplyReferenceForOutboundSend = async ({
    userId,
    companyId,
    replyToMessageId,
    whatsappContextMessageId
  }) => {
    const normalizedReplyToMessageId = String(replyToMessageId || '').trim();
    if (normalizedReplyToMessageId && isValidObjectId(normalizedReplyToMessageId)) {
      const baseIdFilter = { _id: normalizedReplyToMessageId, userId };
      if (companyId) {
        const strictReply = await Message.findOne({ ...baseIdFilter, companyId })
          .select('_id whatsappMessageId')
          .lean();
        if (strictReply) return strictReply;
      }

      const byUserOnlyReply = await Message.findOne(baseIdFilter)
        .select('_id whatsappMessageId')
        .lean();
      if (byUserOnlyReply) return byUserOnlyReply;
    }

    const normalizedContextId = String(whatsappContextMessageId || '').trim();
    if (!normalizedContextId) return null;

    const baseContextFilter = { userId, whatsappMessageId: normalizedContextId };
    if (companyId) {
      const strictContextReply = await Message.findOne({ ...baseContextFilter, companyId })
        .select('_id whatsappMessageId')
        .lean();
      if (strictContextReply) return strictContextReply;
    }

    return Message.findOne(baseContextFilter)
      .select('_id whatsappMessageId')
      .lean();
  };

  const resolveConversationForOutboundSend = async ({ req, conversationId, to }) => {
    const userId = req?.user?.id;
    if (!userId || !conversationId) return null;

    const baseIdQuery = { _id: conversationId, userId };

    if (req.companyId) {
      const strict = await Conversation.findOne({ ...baseIdQuery, companyId: req.companyId });
      if (strict) return strict;
    }

    const byUserOnly = await Conversation.findOne(baseIdQuery);
    if (byUserOnly) return byUserOnly;

    const normalizedPhone = normalizePhoneDigits(to);
    if (!normalizedPhone) return null;

    const phoneCandidates = Array.from(
      new Set(
        [
          String(to || '').trim(),
          normalizedPhone,
          `+${normalizedPhone}`,
          normalizedPhone.length > 10 ? normalizedPhone.slice(-10) : ''
        ].filter(Boolean)
      )
    );

    const companyFallbackFilter = req.companyId
      ? { $or: [{ companyId: req.companyId }, { companyId: null }, { companyId: { $exists: false } }] }
      : {};

    return Conversation.findOne({
      userId,
      ...companyFallbackFilter,
      contactPhone: { $in: phoneCandidates }
    }).sort({ lastMessageTime: -1, updatedAt: -1, createdAt: -1 });
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
              whatsappContextMessageId: resolvedReplyContextMessageId
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
            lastMessageFrom: 'agent'
          }
        );

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
      const filters = { userId: req.user.id, companyId: req.companyId };

      if (status) filters.status = status;
      if (assignedTo) filters.assignedTo = assignedTo;
      if (search) {
        filters.$or = [
          { contactName: { $regex: search, $options: 'i' } },
          { contactPhone: { $regex: search, $options: 'i' } }
        ];
      }

      const conversations = await Conversation.find(filters)
        .populate('contactId', 'name phone email tags')
        .sort({ lastMessageTime: -1 })
        .limit(100)
        .lean();
      res.json(conversations);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/conversations', auth, async (req, res) => {
    try {
      const { contactId, contactPhone, contactName, status } = req.body;

      const existingConversation = await Conversation.findOne({
        userId: req.user.id,
        companyId: req.companyId,
        contactPhone,
        status: { $in: ['active', 'pending'] }
      });

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
        unreadCount: 0
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
      return res.json(conversation);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.put('/api/conversations/:id/read', auth, async (req, res) => {
    try {
      await Message.updateMany(
        {
          conversationId: req.params.id,
          userId: req.user.id,
          companyId: req.companyId,
          sender: 'contact',
          status: 'received'
        },
        { status: 'read' }
      );

      const conversation = await Conversation.findOneAndUpdate(
        { _id: req.params.id, userId: req.user.id, companyId: req.companyId },
        { unreadCount: 0 },
        { new: true }
      );
      if (!conversation) {
        return res.status(404).json({ error: 'Conversation not found' });
      }

      emitRealtimeEvent(req.user.id, {
        type: 'conversation_read',
        conversationId: req.params.id,
        unreadCount: 0
      });

      return res.json(conversation);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/conversations/:id/messages', auth, async (req, res) => {
    try {
      const parsedLimit = Number(req.query?.limit);
      const hasPagination = Number.isFinite(parsedLimit) && parsedLimit > 0;
      const limit = hasPagination ? Math.max(1, Math.min(parsedLimit, 80)) : 0;
      const cursor = String(req.query?.cursor || '').trim();
      const filters = {
        conversationId: req.params.id,
        userId: req.user.id
      };

      if (req.companyId) {
        filters.companyId = req.companyId;
      }

      if (hasPagination && cursor) {
        const cursorDate = new Date(cursor);
        if (!Number.isNaN(cursorDate.valueOf())) {
          filters.timestamp = { $lt: cursorDate };
        }
      }

      const baseQuery = Message.find(filters)
        .select(
          '_id conversationId sender senderName text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment replyTo replyToMessageId errorMessage'
        )
        .populate(
          'replyTo',
          '_id text sender whatsappMessageId mediaType mediaCaption timestamp attachment'
        )
        .sort(hasPagination ? { timestamp: -1, _id: -1 } : { timestamp: 1, _id: 1 })
        .lean();

      const messages = hasPagination
        ? await baseQuery.limit(limit + 1)
        : await baseQuery;

      if (!hasPagination) {
        return res.json(messages);
      }

      const hasMore = messages.length > limit;
      const trimmedMessages = hasMore ? messages.slice(0, limit) : messages;
      const chronologicalMessages = [...trimmedMessages].reverse();
      const nextCursor = hasMore
        ? new Date(trimmedMessages[trimmedMessages.length - 1]?.timestamp || Date.now()).toISOString()
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
      return res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/contacts', auth, async (req, res) => {
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
        filters.tags = { $in: tags.split(',') };
      }

      const contacts = await Contact.find(filters).sort({ lastContact: -1 }).lean();
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

      if (name !== undefined && name !== currentContact.name) {
        await Conversation.updateMany(
          { contactId: id, userId: req.user.id, companyId: req.companyId },
          { contactName: name }
        );
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
      const totalConversations = await Conversation.countDocuments({ userId: req.user.id, companyId: req.companyId });
      const activeConversations = await Conversation.countDocuments({
        userId: req.user.id,
        companyId: req.companyId,
        status: 'active'
      });

      const last7Days = new Date();
      last7Days.setDate(last7Days.getDate() - 7);

      const allAgentMessages = await Message.find({
        userId: req.user.id,
        companyId: req.companyId,
        sender: 'agent',
        timestamp: { $gte: last7Days }
      });

      const userBroadcasts = await Broadcast.find({
        companyId: req.companyId,
        $or: [
          { createdById: req.user.id },
          { createdBy: req.user.username || req.user.email || req.user.id }
        ]
      }).select('stats').lean();

      const campaignTotals = userBroadcasts.reduce(
        (acc, b) => {
          const sent = Number(b?.stats?.sent || 0);
          const delivered = Number(b?.stats?.delivered || 0);
          const read = Number(b?.stats?.read || 0);
          const failed = Number(b?.stats?.failed || 0);
          return {
            sent: acc.sent + sent,
            delivered: acc.delivered + Math.max(delivered, read),
            read: acc.read + read,
            failed: acc.failed + failed
          };
        },
        { sent: 0, delivered: 0, read: 0, failed: 0 }
      );

      const messageTotals = await Message.aggregate([
        {
          $match: {
            userId: req.user.id,
            companyId: req.companyId,
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
      ]);

      const headlineTotals = messageTotals[0] || { sent: 0, delivered: 0, read: 0, failed: 0 };
      const messagesFailed = Math.max(Number(headlineTotals.failed || 0), Number(campaignTotals.failed || 0));
      const messagesRead = Math.max(Number(headlineTotals.read || 0), Number(campaignTotals.read || 0));
      const totalDeliveredOrRead = Math.max(
        Number(headlineTotals.delivered || 0),
        Number(campaignTotals.delivered || 0)
      );
      const messagesSent = Math.max(
        Number(headlineTotals.sent || 0),
        Number(campaignTotals.sent || 0),
        totalDeliveredOrRead + messagesFailed
      );

      const messagesReceived = await Message.countDocuments({
        userId: req.user.id,
        companyId: req.companyId,
        sender: 'contact',
        timestamp: { $gte: last7Days }
      });

      const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      const dailyMap = new Map();
      const dailyConversationSets = new Map();

      for (let i = 6; i >= 0; i -= 1) {
        const day = new Date();
        day.setHours(0, 0, 0, 0);
        day.setDate(day.getDate() - i);
        const key = day.toISOString().slice(0, 10);

        dailyMap.set(key, { date: dayNames[day.getDay()], sent: 0, delivered: 0, read: 0, conversations: 0 });
        dailyConversationSets.set(key, new Set());
      }

      allAgentMessages.forEach((msg) => {
        const ts = new Date(msg.timestamp);
        ts.setHours(0, 0, 0, 0);
        const key = ts.toISOString().slice(0, 10);
        if (!dailyMap.has(key)) return;

        const bucket = dailyMap.get(key);
        bucket.sent += 1;

        if (msg.status === 'delivered' || msg.status === 'read') {
          bucket.delivered += 1;
        }
        if (msg.status === 'read') {
          bucket.read += 1;
        }

        if (msg.conversationId) {
          dailyConversationSets.get(key).add(String(msg.conversationId));
        }
      });

      const dailyTrends = Array.from(dailyMap.entries()).map(([key, value]) => ({
        ...value,
        conversations: dailyConversationSets.get(key).size
      }));

      const hourlyMap = new Map();
      const hourlyConversationSets = new Map();
      for (let i = 11; i >= 0; i -= 1) {
        const hour = new Date();
        hour.setMinutes(0, 0, 0);
        hour.setHours(hour.getHours() - i);
        const key = hour.toISOString().slice(0, 13);
        const label = hour.toLocaleTimeString('en-US', { hour: 'numeric', hour12: true });
        hourlyMap.set(key, { hour: label, messages: 0, conversations: 0 });
        hourlyConversationSets.set(key, new Set());
      }

      allAgentMessages.forEach((msg) => {
        const hour = new Date(msg.timestamp);
        hour.setMinutes(0, 0, 0);
        const key = hour.toISOString().slice(0, 13);
        if (!hourlyMap.has(key)) return;
        hourlyMap.get(key).messages += 1;
        if (msg.conversationId) {
          hourlyConversationSets.get(key).add(String(msg.conversationId));
        }
      });

      const hourlyActivity = Array.from(hourlyMap.entries()).map(([key, value]) => ({
        ...value,
        conversations: hourlyConversationSets.get(key).size
      }));

      const messageTypeCounts = {
        text: 0,
        image: 0,
        video: 0,
        audio: 0,
        document: 0
      };

      allAgentMessages.forEach((msg) => {
        const mediaType = msg.mediaType || 'text';
        if (Object.prototype.hasOwnProperty.call(messageTypeCounts, mediaType)) {
          messageTypeCounts[mediaType] += 1;
        } else {
          messageTypeCounts.text += 1;
        }
      });

      const messageTypes = [
        { name: 'Text Messages', value: messageTypeCounts.text, color: '#3b82f6' },
        { name: 'Image Messages', value: messageTypeCounts.image, color: '#2563eb' },
        { name: 'Video Messages', value: messageTypeCounts.video, color: '#f59e0b' },
        {
          name: 'Document/Audio',
          value: messageTypeCounts.document + messageTypeCounts.audio,
          color: '#8b5cf6'
        }
      ];

      const deliveryRate = messagesSent > 0 ? ((totalDeliveredOrRead / messagesSent) * 100).toFixed(1) : '0.0';
      const readRate = messagesSent > 0 ? ((messagesRead / messagesSent) * 100).toFixed(1) : '0.0';

      const analytics = {
        totalConversations,
        activeConversations,
        messagesSent,
        messagesDelivered: totalDeliveredOrRead,
        messagesRead,
        messagesFailed,
        messagesReceived,
        avgResponseTime: '2m 34s',
        responseRate: 94.5,
        customerSatisfaction: 4.7,
        sentGrowth: '12',
        deliveredGrowth: '8',
        readRateGrowth: '5',
        failedGrowth: '-2',
        dailyTrends,
        hourlyActivity,
        messageTypes,
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
