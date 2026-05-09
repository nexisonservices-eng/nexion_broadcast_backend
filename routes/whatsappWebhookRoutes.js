const crypto = require('crypto');
const {
  resolveInboxStorageUsername,
  downloadAndStoreIncomingWhatsAppMedia
} = require('../services/inboxMediaService');
const {
  applyContactOptIn,
  applyContactOptOut,
  detectWhatsAppOptOutKeyword
} = require('../services/whatsappOutreach/policy');
const {
  buildPhoneCandidates,
  normalizePhoneDigits
} = require('../services/whatsappOutreach/conversationResolver');
const { logConsentEvent } = require('../services/whatsappConsentLogService');
const broadcastService = require('../services/broadcastService');
const { invalidateInboxConversation } = require('../utils/teamInboxCache');
const {
  syncConversationSummaryFromConversation
} = require('../services/conversationSummaryService');
const {
  recordConversationInboundUnread
} = require('../utils/conversationReadStateCache');

const truncateMediaDebugValue = (value, max = 120) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw.length > max ? `${raw.slice(0, max - 3)}...` : raw;
};

const emitMediaDebugLog = ({
  stage,
  level = 'info',
  requestId,
  userId,
  companyId,
  from,
  messageId,
  mediaType,
  mediaId,
  mimeType,
  fileName,
  storedAttachment,
  error,
  message,
  extra = {}
} = {}) => {
  const payload = {
    ts: new Date().toISOString(),
    event: 'media_pipeline',
    stage: truncateMediaDebugValue(stage || 'unknown', 80),
    requestId: truncateMediaDebugValue(requestId || '', 80) || null,
    message: truncateMediaDebugValue(message || '', 240) || null,
    userId: truncateMediaDebugValue(userId || '', 40) || null,
    companyId: truncateMediaDebugValue(companyId || '', 40) || null,
    from: truncateMediaDebugValue(from || '', 40) || null,
    messageId: truncateMediaDebugValue(messageId || '', 80) || null,
    mediaType: truncateMediaDebugValue(mediaType || '', 32) || null,
    mediaId: truncateMediaDebugValue(mediaId || '', 80) || null,
    mimeType: truncateMediaDebugValue(mimeType || '', 80) || null,
    fileName: truncateMediaDebugValue(fileName || '', 120) || null,
    storedAttachment: storedAttachment
      ? {
          publicId: truncateMediaDebugValue(storedAttachment?.publicId || '', 180),
          resourceType: truncateMediaDebugValue(storedAttachment?.resourceType || '', 24),
          fileCategory: truncateMediaDebugValue(storedAttachment?.fileCategory || '', 24),
          bytes: Number(storedAttachment?.bytes || 0) || 0
        }
      : null,
    error: error
      ? {
          message: truncateMediaDebugValue(
            error?.message || error?.response?.data?.error?.message || '',
            240
          ),
          status: Number(error?.status || error?.response?.status || 0) || null,
          code: truncateMediaDebugValue(error?.code || error?.response?.data?.error?.code || '', 48),
          upstreamMessage: truncateMediaDebugValue(
            error?.response?.data?.error?.message ||
              error?.response?.data?.error?.error_user_msg ||
              error?.response?.data?.message ||
              '',
            240
          )
        }
      : null,
    ...extra
  };

  const serialized = JSON.stringify(payload);
  if (level === 'error') {
    console.error(`[MEDIA_PIPELINE] ${serialized}`);
    return payload;
  }
  if (level === 'warn') {
    console.warn(`[MEDIA_PIPELINE] ${serialized}`);
    return payload;
  }
  console.info(`[MEDIA_PIPELINE] ${serialized}`);
  return payload;
};

const registerWhatsAppWebhookRoutes = (app, deps) => {
  const {
    whatsappConfig,
    ENABLE_DEBUG_LOGS,
    resolveUserIdByPhoneNumberId,
    getWhatsAppCredentialsByUserId,
    getLeadScoringSettings,
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

  const normalizeKeywordText = (value = '') =>
    String(value || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ');

  const getExactKeywordMatches = ({ text = '', keywordRules = [] }) => {
    const normalizedText = normalizeKeywordText(text);
    if (!normalizedText) return [];

    return (Array.isArray(keywordRules) ? keywordRules : []).filter((rule) => {
      const keyword = normalizeKeywordText(rule?.keyword || '');
      return Boolean(keyword) && keyword === normalizedText;
    });
  };

  const applyKeywordOptInToContact = async ({
    contact,
    userId,
    companyId,
    messageText,
    whatsappMessageId,
    optInScope,
    matchedKeyword
  }) => {
    if (!contact) return null;

    applyContactOptIn(contact, { source: 'keyword_reply' });
    contact.whatsappOptInScope = String(optInScope || 'marketing').trim().toLowerCase() || 'marketing';
    contact.whatsappOptInTextSnapshot = String(messageText || '').trim();
    contact.whatsappOptInProofType = 'keyword_reply';
    contact.whatsappOptInProofId = String(whatsappMessageId || '').trim();
    contact.whatsappOptInProofUrl = '';
    contact.whatsappOptInCapturedBy = 'whatsapp_webhook';
    contact.whatsappOptInPageUrl = '';
    contact.whatsappOptInIp = '';
    contact.whatsappOptInUserAgent = '';
    contact.whatsappOptInMetadata = {
      source: 'keyword_reply',
      matchedKeyword: String(matchedKeyword || '').trim()
    };
    await contact.save();

    await logConsentEvent({
      contact,
      action: 'opt_in',
      payload: {
        source: 'keyword_reply',
        scope: contact.whatsappOptInScope,
        consentText: contact.whatsappOptInTextSnapshot,
        proofType: contact.whatsappOptInProofType,
        proofId: contact.whatsappOptInProofId,
        capturedBy: contact.whatsappOptInCapturedBy,
        metadata: contact.whatsappOptInMetadata
      }
    });

    return contact;
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
      const normalizedFrom = normalizePhoneDigits(from);
      const phoneCandidates = buildPhoneCandidates(from);
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

      if (mediaType && incomingMediaId) {
        emitMediaDebugLog({
          stage: 'inbound_media_detected',
          requestId: messageId,
          userId,
          companyId,
          from,
          messageId,
          mediaType,
          mediaId: incomingMediaId,
          mimeType: incomingMimeType,
          fileName: incomingMediaFileName,
          message: 'Inbound WhatsApp media detected'
        });
      }

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
            companyContext: {
              companyId,
              companyName: userCredentials?.companyName || '',
              companySlug: userCredentials?.companySlug || '',
              cloudinaryFolderRoot: userCredentials?.cloudinaryFolderRoot || ''
            },
            userId,
            sender: from,
            recipient: userId,
            fallbackMimeType: incomingMimeType,
            fallbackFileName: incomingMediaFileName
          });

          emitMediaDebugLog({
            stage: 'inbound_media_store_success',
            requestId: messageId,
            userId,
            companyId,
            from,
            messageId,
            mediaType,
            mediaId: incomingMediaId,
            mimeType: incomingMimeType,
            fileName: incomingMediaFileName,
            storedAttachment: storedIncomingAttachment,
            message: 'Inbound WhatsApp media stored in Cloudinary'
          });
        } catch (incomingMediaError) {
          emitMediaDebugLog({
            stage: 'inbound_media_store_failed',
            level: 'error',
            requestId: messageId,
            userId,
            companyId,
            from,
            messageId,
            mediaType,
            mediaId: incomingMediaId,
            mimeType: incomingMimeType,
            fileName: incomingMediaFileName,
            error: incomingMediaError,
            message: 'Failed to persist inbound WhatsApp media'
          });
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

      let contact = await Contact.findOne({
        userId,
        companyId,
        phone: { $in: phoneCandidates }
      });
      const leadScoringSettings =
        typeof getLeadScoringSettings === 'function'
          ? await getLeadScoringSettings({ userId, companyId })
          : { optInKeywordRules: [], whatsappOptInScope: 'marketing' };
      const optInKeywordRules = Array.isArray(leadScoringSettings?.whatsappOptInKeywordRules)
        ? leadScoringSettings.whatsappOptInKeywordRules
        : [];
      const optInScope =
        String(leadScoringSettings?.whatsappOptInScope || 'marketing').trim().toLowerCase() ||
        'marketing';
      const matchedOptInKeywords = getExactKeywordMatches({
        text,
        keywordRules: optInKeywordRules
      });
      const matchedOptInKeyword = matchedOptInKeywords[0]?.keyword || '';
      const shouldApplyKeywordOptIn =
        !detectWhatsAppOptOutKeyword(text) && Boolean(matchedOptInKeyword);
      if (!contact) {
        contact = await Contact.create({
          userId,
          companyId,
          phone: normalizedFrom || from,
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
        if (shouldApplyKeywordOptIn) {
          await applyKeywordOptInToContact({
            contact,
            userId,
            companyId,
            messageText: text,
            whatsappMessageId: messageId,
            optInScope,
            matchedKeyword: matchedOptInKeyword
          });
        }
      } else {
        if (normalizedFrom && contact.phone !== normalizedFrom) {
          contact.phone = normalizedFrom;
        }
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
        if (shouldApplyKeywordOptIn) {
          await applyKeywordOptInToContact({
            contact,
            userId,
            companyId,
            messageText: text,
            whatsappMessageId: messageId,
            optInScope,
            matchedKeyword: matchedOptInKeyword
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
      await syncConversationSummaryFromConversation(conversation);

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
      await recordConversationInboundUnread({
        userId,
        companyId,
        conversationId: conversation._id,
        messageId: message._id,
        messageAt: inboundActivityAt,
        unreadCount: Number(conversation.unreadCount || 1)
      });
      await invalidateInboxConversation({
        userId,
        companyId,
        conversationId: conversation._id
      });

      emitRealtimeEvent(userId, {
        type: 'crm_changed',
        contactId: String(contact._id),
        conversationId: String(conversation._id),
        reason: 'whatsapp_inbound_message',
        lastInboundMessageAt: inboundActivityAt.toISOString(),
        serviceWindowClosesAt: serviceWindowClosesAt.toISOString()
      });

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

      if (shouldApplyKeywordOptIn) {
        emitRealtimeEvent(userId, {
          type: 'crm_changed',
          contactId: String(contact._id),
          conversationId: String(conversation._id),
          reason: 'whatsapp_keyword_opt_in'
        });
      }

      const broadcast = await Broadcast.findOne({
        companyId,
        createdById: userId,
        'recipients.phone': { $in: phoneCandidates },
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
        console.log('No message found for whatsappMessageId:', messageId, 'attempting repair from dispatch');
        const repairResult = await broadcastService.repairBroadcastDispatchInbox({
          whatsappMessageId: messageId
        });
        if (repairResult?.success) {
          message = await Message.findOne({ whatsappMessageId: messageId, companyId });
          if (!message) {
            message = await Message.findOne({ whatsappMessageId: messageId });
          }
        }
        if (!message) {
          console.log('No message found after repair attempt for whatsappMessageId:', messageId);
          return;
        }
      }

      const effectiveUserId = message.userId || userId;
      const effectiveCompanyId = message.companyId || companyId;
      const mediaPipelineRequestId = String(message.mediaPipelineRequestId || '').trim();
      if (mediaPipelineRequestId) {
        emitMediaDebugLog({
          stage: 'outbound_media_status_resolved',
          requestId: mediaPipelineRequestId,
          userId: effectiveUserId,
          companyId: effectiveCompanyId,
          from: recipient,
          messageId,
          message: 'Resolved outbound media status back to message pipeline id'
        });
      }

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

      if (updatedMessage.sender === 'agent') {
        try {
          await Conversation.updateOne(
            { _id: updatedMessage.conversationId },
            {
              lastMessageStatus: status,
              lastMessageFrom: 'agent',
              lastMessageWhatsappMessageId: updatedMessage.whatsappMessageId || messageId || ''
            }
          );
          await syncConversationSummaryFromConversation({
            _id: updatedMessage.conversationId,
            userId: updatedMessage.userId,
            companyId: updatedMessage.companyId,
            lastMessageStatus: status,
            lastMessageFrom: 'agent',
            lastMessageWhatsappMessageId:
              updatedMessage.whatsappMessageId || messageId || ''
          });
        } catch (conversationStatusError) {
          console.error('Error updating conversation lastMessageStatus:', conversationStatusError);
        }
      }
      await invalidateInboxConversation({
        userId: effectiveUserId,
        companyId: effectiveCompanyId,
        conversationId: updatedMessage.conversationId
      });

      emitRealtimeEvent(effectiveUserId, {
        type: 'message_status',
        messageId,
        status,
        errorMessage: updatedMessage.errorMessage || '',
        conversationId: message.conversationId,
        broadcastId: updatedMessage.broadcastId ? String(updatedMessage.broadcastId) : null,
        previousStatus: oldStatus,
        mediaPipelineRequestId: mediaPipelineRequestId || null
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
