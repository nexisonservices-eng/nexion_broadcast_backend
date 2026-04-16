const crypto = require('crypto');
const {
  resolveInboxStorageUsername,
  downloadAndStoreIncomingWhatsAppMedia
} = require('../services/inboxMediaService');
const {
  applyContactOptOut,
  detectWhatsAppOptOutKeyword
} = require('../services/whatsappOutreach/policy');
const { logConsentEvent } = require('../services/whatsappConsentLogService');

const registerWhatsAppWebhookRoutes = (app, deps) => {
  const {
    whatsappConfig,
    ENABLE_DEBUG_LOGS,
    resolveUserIdByPhoneNumberId,
    getWhatsAppCredentialsByUserId,
    applyIncomingMessageScore,
    applyReadScoreForMessage,
    Contact,
    Conversation,
    Message,
    Broadcast,
    emitRealtimeEvent
  } = deps;

  const handleWebhookVerification = (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    console.log('Webhook verification request:', {
      mode,
      hasToken: Boolean(token),
      challengeLength: String(challenge || '').length
    });

    if (mode === 'subscribe' && token === whatsappConfig.WEBHOOK_VERIFY_TOKEN) {
      console.log('Webhook verified successfully');
      res.status(200).send(challenge);
    } else {
      console.log('Webhook verification failed');
      res.sendStatus(403);
    }
  };

  app.get('/webhook', handleWebhookVerification);
  app.get('/webhooks/whatsapp', handleWebhookVerification);

  const verifyMetaSignature = (req) => {
    const signature = req.headers['x-hub-signature-256'];
    const secret = process.env.WHATSAPP_APP_SECRET || process.env.META_APP_SECRET || '';
    if (!signature || !secret || !req.rawBody) return false;
    const expected = `sha256=${crypto
      .createHmac('sha256', secret)
      .update(req.rawBody)
      .digest('hex')}`;
    return signature === expected;
  };

  const toTitleCase = (value = '') =>
    String(value || '')
      .trim()
      .replace(/[_-]+/g, ' ')
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const firstNonEmpty = (...values) => {
    for (const value of values) {
      const normalized = String(value || '').trim();
      if (normalized) return normalized;
    }
    return '';
  };

  const summarizeNfmReply = (responseJson) => {
    if (!responseJson) return '';
    try {
      const parsed =
        typeof responseJson === 'string' ? JSON.parse(responseJson) : responseJson;
      if (!parsed || typeof parsed !== 'object') return '';
      const values = Object.values(parsed)
        .map((value) => String(value ?? '').trim())
        .filter(Boolean);
      return values.slice(0, 3).join(' | ');
    } catch (_error) {
      return String(responseJson || '').trim();
    }
  };

  const extractIncomingMessagePayload = (messageData = {}) => {
    const type = String(messageData?.type || '').trim().toLowerCase();
    const mediaTypes = new Set(['image', 'video', 'audio', 'document']);
    const reactionTargetMessageId = firstNonEmpty(messageData?.reaction?.message_id);

    let text = '';
    let scoringText = '';
    let mediaType = null;
    let mediaCaption = '';
    let reactionEmoji = '';
    const whatsappContextMessageId = firstNonEmpty(
      reactionTargetMessageId,
      messageData?.context?.id
    );
    const interactionMeta = {
      interactionType: '',
      interactionId: '',
      interactionTitle: ''
    };
    const fallbackReplyText = firstNonEmpty(
      messageData?.button?.text,
      messageData?.button?.payload,
      messageData?.interactive?.button_reply?.title,
      messageData?.interactive?.button_reply?.id,
      messageData?.interactive?.list_reply?.title,
      messageData?.interactive?.list_reply?.description,
      messageData?.interactive?.list_reply?.id,
      summarizeNfmReply(messageData?.interactive?.nfm_reply?.response_json),
      messageData?.referral?.headline,
      messageData?.referral?.body,
      reactionTargetMessageId,
      messageData?.context?.id
    );

    if (type === 'text') {
      text = String(messageData?.text?.body || '').trim();
      scoringText = text;
    } else if (type === 'button') {
      interactionMeta.interactionType = 'button';
      interactionMeta.interactionId = firstNonEmpty(messageData?.button?.payload);
      interactionMeta.interactionTitle = firstNonEmpty(
        messageData?.button?.text,
        messageData?.button?.payload
      );
      text = String(messageData?.button?.text || messageData?.button?.payload || '').trim();
      scoringText = text;
    } else if (type === 'interactive') {
      const buttonReply = messageData?.interactive?.button_reply || null;
      const listReply = messageData?.interactive?.list_reply || null;
      const nfmReply = messageData?.interactive?.nfm_reply || null;
      const nfmReplyText = summarizeNfmReply(
        messageData?.interactive?.nfm_reply?.response_json
      );

      if (buttonReply) {
        interactionMeta.interactionType = 'interactive_button_reply';
        interactionMeta.interactionId = firstNonEmpty(buttonReply?.id);
        interactionMeta.interactionTitle = firstNonEmpty(buttonReply?.title, buttonReply?.id);
      } else if (listReply) {
        interactionMeta.interactionType = 'interactive_list_reply';
        interactionMeta.interactionId = firstNonEmpty(listReply?.id);
        interactionMeta.interactionTitle = firstNonEmpty(
          listReply?.title,
          listReply?.description,
          listReply?.id
        );
      } else if (nfmReply) {
        interactionMeta.interactionType = 'interactive_nfm_reply';
        interactionMeta.interactionId = firstNonEmpty(nfmReply?.name);
        interactionMeta.interactionTitle = firstNonEmpty(
          summarizeNfmReply(nfmReply?.response_json),
          nfmReply?.name
        );
      }

      text = String(
        buttonReply?.title ||
          listReply?.title ||
          listReply?.description ||
          buttonReply?.id ||
          listReply?.id ||
          nfmReplyText ||
          ''
      ).trim();
      scoringText = text;
    } else if (mediaTypes.has(type)) {
      mediaType = type;
      mediaCaption = String(messageData?.[type]?.caption || '').trim();
      text = mediaCaption || `[${toTitleCase(type)}]`;
      scoringText = mediaCaption;
    } else if (type === 'reaction') {
      reactionEmoji = String(messageData?.reaction?.emoji || '').trim();
      text = reactionEmoji ? `Reacted with ${reactionEmoji}` : '[Reaction removed]';
    } else if (type === 'location') {
      const location = messageData?.location || {};
      const name = String(location?.name || '').trim();
      const address = String(location?.address || '').trim();
      text = [name, address].filter(Boolean).join(' - ') || '[Location]';
    } else if (type === 'contacts') {
      text = '[Contact card]';
    } else if (type === 'order') {
      text = '[Order message]';
    }

    if (!text && fallbackReplyText) {
      text = fallbackReplyText;
      scoringText = scoringText || fallbackReplyText;
    }

    if (!text) {
      text = type ? `[${toTitleCase(type)}]` : '[Unsupported message]';
    }

    return {
      text,
      scoringText,
      mediaType,
      mediaCaption,
      reactionEmoji,
      rawMessageType: type || '',
      whatsappContextMessageId: whatsappContextMessageId || '',
      interactionType: interactionMeta.interactionType,
      interactionId: interactionMeta.interactionId,
      interactionTitle: interactionMeta.interactionTitle
    };
  };

  const handleIncomingMessage = async (messageData, value, userId, companyId) => {
    try {
      console.log('Processing incoming message...');

      const from = messageData.from;
      const {
        text,
        scoringText,
        mediaType,
        mediaCaption,
        reactionEmoji,
        rawMessageType,
        whatsappContextMessageId,
        interactionType,
        interactionId,
        interactionTitle
      } = extractIncomingMessagePayload(messageData);
      const messageId = messageData.id;
      const incomingMediaPayload = mediaType ? messageData?.[mediaType] || {} : {};
      const incomingMediaId = String(incomingMediaPayload?.id || '').trim();
      const incomingMimeType = String(incomingMediaPayload?.mime_type || '').trim();
      const incomingMediaFileName = String(
        incomingMediaPayload?.filename || `${mediaType || 'attachment'}-${messageId || Date.now()}`
      ).trim();

      let storedIncomingAttachment = null;
      if (mediaType && incomingMediaId) {
        try {
          const userCredentials = await getWhatsAppCredentialsByUserId(userId);
          const storageUsername = resolveInboxStorageUsername({
            username: userCredentials?.username,
            email: userCredentials?.email,
            userId
          });

          storedIncomingAttachment = await downloadAndStoreIncomingWhatsAppMedia({
            mediaId: incomingMediaId,
            credentials: userCredentials,
            username: storageUsername,
            userId,
            sender: from,
            recipient: userId,
            fallbackMimeType: incomingMimeType,
            fallbackFileName: incomingMediaFileName
          });
        } catch (incomingMediaError) {
          console.error(
            `Failed to persist incoming ${mediaType} media for user ${userId}:`,
            incomingMediaError?.message || incomingMediaError
          );
        }
      }

      let replyToMessageId = null;
      if (whatsappContextMessageId) {
        const referencedMessage =
          (await Message.findOne({
            userId,
            companyId,
            whatsappMessageId: whatsappContextMessageId
          })
            .select('_id')
            .lean()) ||
          (await Message.findOne({
            userId,
            whatsappMessageId: whatsappContextMessageId
          })
            .select('_id')
            .lean());

        if (referencedMessage?._id) {
          replyToMessageId = referencedMessage._id;
        }
      }

      const inboundActivityAt = new Date();
      const serviceWindowClosesAt = new Date(inboundActivityAt.getTime() + 24 * 60 * 60 * 1000);

      let contact = await Contact.findOne({ userId, companyId, phone: from });
      if (!contact) {
        contact = await Contact.create({
          userId,
          companyId,
          phone: from,
          name: value.contacts?.[0]?.profile?.name || from,
          sourceType: 'incoming_message',
          lastContact: inboundActivityAt,
          lastContactAt: inboundActivityAt,
          lastInboundMessageAt: inboundActivityAt,
          serviceWindowClosesAt
        });
        if (detectWhatsAppOptOutKeyword(text)) {
          applyContactOptOut(contact, { source: 'keyword' });
          await contact.save();
          await logConsentEvent({
            contact,
            action: 'opt_out',
            payload: {
              source: 'keyword'
            }
          });
        }
      } else {
        contact.lastContact = inboundActivityAt;
        contact.lastContactAt = inboundActivityAt;
        contact.lastInboundMessageAt = inboundActivityAt;
        contact.serviceWindowClosesAt = serviceWindowClosesAt;
        if (detectWhatsAppOptOutKeyword(text)) {
          applyContactOptOut(contact, { source: 'keyword' });
        }
        await contact.save();
        if (detectWhatsAppOptOutKeyword(text)) {
          await logConsentEvent({
            contact,
            action: 'opt_out',
            payload: {
              source: 'keyword'
            }
          });
        }
      }

      const isReactionMessage = String(rawMessageType || '').trim().toLowerCase() === 'reaction';

      let conversation = await Conversation.findOne({
        userId,
        companyId,
        contactPhone: from,
        status: { $in: ['active', 'pending'] }
      });
      if (!conversation) {
        conversation = await Conversation.create({
          userId,
          companyId,
          contactId: contact._id,
          contactPhone: from,
          contactName: contact.name,
          lastMessageTime: inboundActivityAt,
          lastMessage: text,
          lastMessageMediaType: String(mediaType || '').trim(),
          lastMessageAttachmentName:
            String(mediaType || '').trim().toLowerCase() === 'document'
              ? String(storedIncomingAttachment?.originalFileName || '').trim()
              : '',
          lastMessageAttachmentPages:
            String(mediaType || '').trim().toLowerCase() === 'document'
              ? Number(storedIncomingAttachment?.pages || 0) || null
              : null,
          lastMessageFrom: 'contact',
          unreadCount: 1
        });
      } else {
        if (!isReactionMessage) {
          conversation.lastMessageTime = inboundActivityAt;
          conversation.lastMessage = text;
          conversation.lastMessageMediaType = String(mediaType || '').trim();
          conversation.lastMessageAttachmentName =
            String(mediaType || '').trim().toLowerCase() === 'document'
              ? String(storedIncomingAttachment?.originalFileName || '').trim()
              : '';
          conversation.lastMessageAttachmentPages =
            String(mediaType || '').trim().toLowerCase() === 'document'
              ? Number(storedIncomingAttachment?.pages || 0) || null
              : null;
          conversation.lastMessageFrom = 'contact';
          const currentUnread = Number(conversation.unreadCount);
          conversation.unreadCount = Number.isFinite(currentUnread)
            ? Math.max(0, currentUnread) + 1
            : 1;
        }
        await conversation.save();
      }

      const message = await Message.create({
        userId,
        companyId,
        conversationId: conversation._id,
        sender: 'contact',
        senderName: contact.name,
        text,
        mediaUrl: storedIncomingAttachment?.secureUrl || undefined,
        mediaType,
        mediaCaption: mediaCaption || undefined,
        attachment: storedIncomingAttachment || undefined,
        rawMessageType: rawMessageType || undefined,
        reactionEmoji: reactionEmoji || undefined,
        whatsappContextMessageId: whatsappContextMessageId || undefined,
        interactionType: interactionType || undefined,
        interactionId: interactionId || undefined,
        interactionTitle: interactionTitle || undefined,
        replyTo: replyToMessageId || undefined,
        whatsappMessageId: messageId,
        status: 'received',
        whatsappTimestamp: new Date(messageData.timestamp * 1000),
        timestamp: new Date()
      });
      await message.populate('replyTo', '_id text sender whatsappMessageId mediaType mediaCaption timestamp');

      const incomingScoreResult = await applyIncomingMessageScore({
        messageId: message._id,
        userId,
        companyId,
        text: scoringText
      });

      if (incomingScoreResult?.contact) {
        emitRealtimeEvent(userId, {
          type: 'lead_score_updated',
          contactId: String(incomingScoreResult.contact._id),
          conversationId: String(conversation._id),
          leadScore: Number(incomingScoreResult.contact.leadScore || 0),
          breakdown: incomingScoreResult.contact.leadScoreBreakdown || {},
          scoringDetail: incomingScoreResult.detail || null
        });
      }

      const broadcast = await Broadcast.findOne({
        companyId,
        createdById: userId,
        'recipients.phone': from,
        status: { $in: ['sending', 'completed'] },
        startedAt: { $exists: true }
      }).sort({ startedAt: -1 });

      if (broadcast) {
        const previousReplies = await Message.countDocuments({
          conversationId: conversation._id,
          sender: 'contact',
          timestamp: {
            $gte: broadcast.startedAt,
            $lte: new Date()
          }
        });

        if (previousReplies === 1) {
          await Broadcast.updateOne(
            { _id: broadcast._id },
            { $inc: { 'stats.replied': 1 } }
          );

          const updatedBroadcast = await Broadcast.findById(broadcast._id);

          emitRealtimeEvent(userId, {
            type: 'broadcast_stats_updated',
            broadcastId: broadcast._id.toString(),
            stats: {
              ...updatedBroadcast.stats,
              repliedPercentage: updatedBroadcast.repliedPercentage,
              repliedPercentageOfTotal: updatedBroadcast.repliedPercentageOfTotal
            }
          });

          console.log(
            `Updated replied count for broadcast "${broadcast.name}": ${updatedBroadcast.stats.replied} (${updatedBroadcast.repliedPercentage}% of sent)`
          );
        }
      }

      emitRealtimeEvent(userId, {
        type: 'new_message',
        conversation: conversation.toObject(),
        message: message.toObject()
      });

      console.log('Message processing complete');
    } catch (error) {
      console.error('Error in handleIncomingMessage:', error);
      throw error;
    }
  };

  const handleMessageStatus = async (statusData, userId, companyId) => {
    try {
      const messageId = statusData.id;
      const status = statusData.status;
      const recipient = statusData.recipient_id;
      const statusError = [
        statusData?.errors?.[0]?.message,
        statusData?.errors?.[0]?.error_data?.details,
        statusData?.errors?.[0]?.title
      ]
        .map((value) => String(value || '').trim())
        .find(Boolean) || '';

      console.log('Received status update:', {
        messageId,
        status,
        recipient,
        timestamp: statusData.timestamp,
        conversationStatus: statusData.conversation?.id,
        error: statusError || undefined
      });

      let message = null;
      if (companyId) {
        message = await Message.findOne({ whatsappMessageId: messageId, companyId });
      }
      if (!message) {
        message = await Message.findOne({ whatsappMessageId: messageId });
      }
      if (!message) {
        console.log('No message found for whatsappMessageId:', messageId);
        return;
      }

      const effectiveUserId = message.userId || userId;
      const effectiveCompanyId = message.companyId || companyId;

      const oldStatus = message.status;
      const updatedMessage = await Message.findOneAndUpdate(
        { _id: message._id },
        {
          status,
          errorMessage: status === 'failed' ? statusError : '',
          updatedAt: new Date()
        },
        { new: true }
      );

      if (!updatedMessage) {
        return;
      }

      emitRealtimeEvent(effectiveUserId, {
        type: 'message_status',
        messageId,
        status,
        errorMessage: updatedMessage.errorMessage || '',
        conversationId: message.conversationId,
        broadcastId: updatedMessage.broadcastId ? String(updatedMessage.broadcastId) : null,
        previousStatus: oldStatus
      });

      if (status === 'read' && oldStatus !== 'read') {
        const readScoreResult = await applyReadScoreForMessage({
          messageId: updatedMessage._id,
          userId: effectiveUserId,
          companyId: effectiveCompanyId
        });

        if (readScoreResult?.contact) {
          emitRealtimeEvent(effectiveUserId, {
            type: 'lead_score_updated',
            contactId: String(readScoreResult.contact._id),
            conversationId: String(updatedMessage.conversationId),
            leadScore: Number(readScoreResult.contact.leadScore || 0),
            breakdown: readScoreResult.contact.leadScoreBreakdown || {},
            scoringDetail: {
              event: 'message_read',
              points: readScoreResult.points
            }
          });
        }
      }

      if (updatedMessage.broadcastId && oldStatus !== updatedMessage.status) {
        try {
          const broadcast = await Broadcast.findOne({
            _id: updatedMessage.broadcastId,
            createdById: effectiveUserId,
            companyId: effectiveCompanyId,
            status: { $in: ['sending', 'completed'] }
          });

          if (!broadcast) {
            return;
          }

          const newStatus = updatedMessage.status;
          const update = {};

          if (newStatus === 'delivered' && oldStatus === 'sent') {
            update['stats.delivered'] = 1;
          } else if (newStatus === 'read' && oldStatus !== 'read') {
            update['stats.read'] = 1;
            if (oldStatus !== 'delivered') {
              update['stats.delivered'] = 1;
            }
          } else if (newStatus === 'failed' && oldStatus !== 'failed') {
            update['stats.failed'] = 1;
            if (oldStatus === 'sent') {
              update['stats.sent'] = -1;
            }
          }

          if (Object.keys(update).length > 0) {
            await Broadcast.updateOne({ _id: broadcast._id }, { $inc: update });
            const updatedBroadcast = await Broadcast.findById(broadcast._id);

            if (updatedBroadcast) {
              let clamped = false;
              ['sent', 'delivered', 'read', 'failed', 'replied'].forEach((k) => {
                if ((updatedBroadcast.stats?.[k] || 0) < 0) {
                  updatedBroadcast.stats[k] = 0;
                  clamped = true;
                }
              });
              if (clamped) {
                await updatedBroadcast.save();
              }

              emitRealtimeEvent(effectiveUserId, {
                type: 'broadcast_stats_updated',
                broadcastId: broadcast._id.toString(),
                stats: updatedBroadcast.stats,
                statusChange: `${oldStatus} -> ${newStatus}`
              });
            }
          }
        } catch (broadcastError) {
          console.error('Error updating broadcast stats:', broadcastError);
        }
      }
    } catch (error) {
      console.error('Error handling message status:', error);
    }
  };

  const processWebhookPayload = async (data) => {
    if (data.object !== 'whatsapp_business_account') return;
    for (const entry of data.entry || []) {
      for (const change of entry.changes || []) {
        if (change.field === 'messages') {
          const messageData = change.value.messages?.[0];
          const statusData = change.value.statuses?.[0];
          const phoneNumberId = change.value?.metadata?.phone_number_id;
          const userId = await resolveUserIdByPhoneNumberId(phoneNumberId);
          const credentials = userId
            ? await getWhatsAppCredentialsByUserId(userId)
            : null;
          const companyId = credentials?.companyId || null;

          if (messageData && userId && companyId) {
            await handleIncomingMessage(messageData, change.value, userId, companyId);
          }
          if (statusData) {
            await handleMessageStatus(statusData, userId || null, companyId || null);
          }
        }
      }
    }
  };

  app.post('/webhooks/whatsapp', (req, res) => {
    if (!verifyMetaSignature(req)) {
      return res.sendStatus(401);
    }

    // Acknowledge immediately so Meta retries are avoided on slow downstream processing.
    res.sendStatus(200);

    processWebhookPayload(req.body).catch((error) => {
      console.error('Webhook error (async):', error);
    });
  });

  app.post('/webhook', (req, res) => {
    const data = req.body;

    console.log('Webhook received:', {
      object: data?.object || 'unknown',
      entries: Array.isArray(data?.entry) ? data.entry.length : 0
    });
    if (ENABLE_DEBUG_LOGS) {
      console.log('Webhook payload (debug):', JSON.stringify(data, null, 2));
    }

    // Acknowledge immediately to keep webhook sender latency low.
    res.sendStatus(200);

    processWebhookPayload(data).catch((error) => {
      console.error('Webhook error (async):', error);
    });
  });
};

module.exports = {
  registerWhatsAppWebhookRoutes
};
