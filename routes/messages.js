const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const multer = require('multer');
const axios = require('axios');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const Contact = require('../models/Contact');
const Template = require('../models/Template');
const whatsappService = require('../services/whatsappService');
const {
  buildChronologicalPage,
  buildThreadPageResponse,
  buildMessageCursorFilter,
  buildMessageIdCursorFilter,
  decodeMessageCursor,
  decodeMessageIdCursor,
  encodeAttachmentCursor,
  encodeMessageCursor,
  encodeMessageIdCursor,
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
const { createRedisRateLimiter } = require('../middleware/redisRateLimit');

const setInboxNoCacheHeaders = (res) => {
  res.set({
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0, private',
    Pragma: 'no-cache',
    Expires: '0'
  });
};

const threadReadRateLimit = createRedisRateLimiter({
  namespace: 'thread-message-read',
  windowMs: 60_000,
  max: 240,
  message: 'Message history is being loaded too quickly.'
});

const sendMessageRateLimit = createRedisRateLimiter({
  namespace: 'message-send',
  windowMs: 60_000,
  max: 60,
  message: 'Message sending is being rate limited. Please wait and retry.'
});

const attachmentRateLimit = createRedisRateLimiter({
  namespace: 'message-attachment',
  windowMs: 60_000,
  max: 30,
  message: 'Attachment actions are being rate limited. Please wait and retry.'
});
const {
  upsertConversationSummary
} = require('../services/conversationSummaryService');
const {
  enqueueRealtimeOutboxEvent
} = require('../services/realtimeOutboxService');
const {
  resolveInboxStorageUsername,
  uploadInboxAttachment,
  generateSignedAttachmentUrl,
  generateAttachmentDownloadUrl,
  isAttachmentPathOwned,
  deleteInboxAttachment
} = require('../services/inboxMediaService');
const { resolveOutboundSenderMeta } = require('../utils/messageSenderMeta');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const requirePlanFeature = require('../middleware/planGuard');
const {
  buildPhoneCandidates,
  resolveConversationForOutboundSend,
  resolveOrCreateConversationForTemplateSend,
  markOutboundTemplateContactActivity,
  cleanupCreatedTemplateOutreachTarget
} = require('../services/whatsappOutreach/conversationResolver');
const {
  resolveRelatedConversationIds
} = require('../utils/conversationThreadLookup');
const {
  validateFreeformOutboundSend,
  validateTemplateOutboundSend,
  applyMarketingTemplateSent,
  toCleanString,
  getWhatsAppMessagingPolicy
} = require('../services/whatsappOutreach/policy');
const { uploadCampaignCreative } = require('../utils/cloudinaryUpload');
const { resolveCompanyFolders } = require('../services/cloudinaryCompanyFolders');

router.use(auth);
router.use(
  requireTenantPolicy({
    requiredFeatures: ['teamInbox', 'contacts'],
    auditEvent: 'messages_policy'
  })
);

// Compatibility endpoint for Team Inbox message history when conversation routes are unavailable.
router.get('/conversation/:id', threadReadRateLimit, async (req, res) => {
  try {
    setInboxNoCacheHeaders(res);
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
    const conversationRecord = await Conversation.findOne({
      _id: conversationId,
      ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {})
    })
      .select('_id contactPhone contactId assignedTo assignedToId assignedAgent')
      .lean();
    const relatedConversationIds = await resolveRelatedConversationIds({
      Conversation,
      ConversationSummary,
      req,
      conversation: conversationRecord || { _id: conversationId },
      includeAllIdentityMatches: true
    });
    const familyScopeKey = relatedConversationIds
      .map((id) => String(id || '').trim())
      .filter(Boolean)
      .sort()
      .join('|');
    const threadScope = scope ? `${scope}:${conversationId}:${familyScopeKey}` : '';

    const loadMessages = async (filters) =>
      Message.find(filters)
        .select(
          '_id conversationId sender senderRole senderName senderId text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment replyTo replyToMessageId errorMessage'
        )
        .sort({ timestamp: -1, _id: -1 })
        .limit(limit + 1)
        .lean();

    const loadScopedMessages = async () => {
      const scopedMessages = await loadMessages({
        ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
        ...(relatedConversationIds.length > 0
          ? { conversationId: { $in: relatedConversationIds } }
          : { conversationId })
      });

      if (scopedMessages.length > 0 || !normalizedCompanyId) {
        return scopedMessages;
      }

      const companyWideMessages = await loadMessages({
        ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
        ...(relatedConversationIds.length > 0
          ? { conversationId: { $in: relatedConversationIds } }
          : { conversationId }),
        ...(cursor ? buildMessageCursorFilter(cursor) : {})
      });

      if (companyWideMessages.length > 0) {
        return companyWideMessages;
      }

      return loadMessages({
        ...(relatedConversationIds.length > 0
          ? { conversationId: { $in: relatedConversationIds } }
          : { conversationId }),
        ...(cursor ? buildMessageCursorFilter(cursor) : {}),
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
                select: '_id text sender senderRole senderName senderId whatsappMessageId mediaType mediaCaption timestamp attachment'
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
      const cachedItems = Array.isArray(cachedResponse?.data) ? cachedResponse.data : [];
      if (cachedItems.length > 0) {
        return res.json(cachedResponse);
      }

      await invalidateInboxConversation({
        companyId: normalizedCompanyId,
        userId: req.user?.id || '',
        conversationId
      });
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
      error: error?.message || 'Failed to fetch conversation messages'
    });
  }
});

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.INBOX_ATTACHMENT_MAX_FILE_SIZE_BYTES || 30 * 1024 * 1024)
  }
});

const isValidObjectId = (value = '') => /^[a-f\d]{24}$/i.test(String(value || '').trim());

const resolveReplyReferenceForOutboundSend = async ({
  userId,
  companyId,
  replyToMessageId,
  whatsappContextMessageId,
  isTenantWide = false
}) => {
  const normalizedReplyToMessageId = String(replyToMessageId || '').trim();
  if (normalizedReplyToMessageId && isValidObjectId(normalizedReplyToMessageId)) {
    const baseIdFilter = { _id: normalizedReplyToMessageId };
    const byScopeReply = await Message.findOne(
      companyId
        ? { ...baseIdFilter, companyId, ...(isTenantWide ? {} : { userId }) }
        : { ...baseIdFilter, ...(isTenantWide ? {} : { userId }) }
    )
      .select('_id whatsappMessageId')
      .lean();
    if (byScopeReply) return byScopeReply;
  }

  const normalizedContextId = String(whatsappContextMessageId || '').trim();
  if (!normalizedContextId) return null;

  const baseContextFilter = { whatsappMessageId: normalizedContextId };
  return Message.findOne(
    companyId
      ? { ...baseContextFilter, companyId, ...(isTenantWide ? {} : { userId }) }
      : { ...baseContextFilter, ...(isTenantWide ? {} : { userId }) }
  )
    .select('_id whatsappMessageId')
    .lean();
};

const resolveReactionTargetForOutboundSend = async ({
  userId,
  companyId,
  conversationId,
  targetMessageId,
  targetWhatsAppMessageId,
  isTenantWide = false
}) => {
  const baseFilters = {
    conversationId,
    ...(isTenantWide ? {} : { userId })
  };

  const normalizedTargetMessageId = String(targetMessageId || '').trim();
  if (normalizedTargetMessageId && isValidObjectId(normalizedTargetMessageId)) {
    const baseIdFilter = { ...baseFilters, _id: normalizedTargetMessageId };
    const byScopeTarget = await Message.findOne(
      companyId ? { ...baseIdFilter, companyId } : baseIdFilter
    )
      .select('_id whatsappMessageId rawMessageType timestamp whatsappTimestamp createdAt')
      .lean();
    if (byScopeTarget) return byScopeTarget;
  }

  const normalizedTargetWhatsAppMessageId = String(targetWhatsAppMessageId || '').trim();
  if (!normalizedTargetWhatsAppMessageId) return null;

  const baseWhatsAppFilter = {
    ...baseFilters,
    whatsappMessageId: normalizedTargetWhatsAppMessageId
  };
  return Message.findOne(
    companyId ? { ...baseWhatsAppFilter, companyId } : baseWhatsAppFilter
  )
    .select('_id whatsappMessageId rawMessageType timestamp whatsappTimestamp createdAt')
    .lean();
};

const resolveAttachmentUsername = (req) =>
  resolveInboxStorageUsername({
    username: req?.user?.username,
    email: req?.user?.email,
    userId: req?.user?.id
  });

const resolveCompanyStorageContext = (req) => ({
  companyId: req.companyId || req.user?.companyId || null,
  companyName: req.user?.companyName || '',
  companySlug: req.user?.companySlug || '',
  cloudinaryFolderRoot: req.user?.cloudinaryFolderRoot || ''
});

const resolveBroadcastTemplateHeaderFolder = (req) => {
  const companyContext = resolveCompanyStorageContext(req);
  return resolveCompanyFolders(companyContext).metaTemplateImagesFolder;
};

const buildAttachmentLabel = (mediaType = '') => {
  const normalized = String(mediaType || '').trim().toLowerCase();
  if (normalized === 'image') return '[Image]';
  if (normalized === 'audio') return '[Audio]';
  if (normalized === 'document') return '[Document]';
  if (normalized) return `[${normalized.charAt(0).toUpperCase()}${normalized.slice(1)}]`;
  return '[Attachment]';
};

const syncConversationSummary = async (conversation = {}, updates = {}) => {
  const base = typeof conversation.toObject === 'function' ? conversation.toObject() : conversation;
  if (!base?._id) return null;

  return upsertConversationSummary({
    conversationId: base._id,
    userId: base.userId,
    companyId: base.companyId,
    contactId: base.contactId,
    contactPhone: base.contactPhone,
    contactName: base.contactName,
    status: base.status,
    assignedTo: base.assignedTo,
    assignedToId: base.assignedToId,
    tags: base.tags,
    priority: base.priority,
    lastMessageTime: base.lastMessageTime,
    lastMessage: base.lastMessage,
    lastMessageMediaType: base.lastMessageMediaType,
    lastMessageAttachmentName: base.lastMessageAttachmentName,
    lastMessageAttachmentPages: base.lastMessageAttachmentPages,
    lastMessageFrom: base.lastMessageFrom,
    lastMessageWhatsappMessageId: base.lastMessageWhatsappMessageId,
    lastMessageStatus: base.lastMessageStatus,
    unreadCount: base.unreadCount,
    notes: base.notes,
    resolvedAt: base.resolvedAt,
    ...updates
  });
};

const writeConversationThreadState = async ({
  conversation,
  conversationUpdate = {},
  summaryUpdate = {},
  companyId = '',
  userId = '',
  relatedConversationIds = []
}) => {
  if (!conversation?._id) return null;

  await Conversation.updateOne({ _id: conversation._id }, conversationUpdate);
  await syncConversationSummary(conversation, summaryUpdate);
  await invalidateInboxConversation({
    companyId: companyId || '',
    userId: userId || '',
    conversationId: conversation._id,
    conversationIds: relatedConversationIds
  });

  return true;
};

const enqueueUserRealtimeEvent = async ({
  userId,
  companyId,
  conversationId,
  eventType,
  data,
  dedupeKey
}) => {
  const normalizedUserId = String(userId || '').trim();
  const payload = {
    scope: 'user',
    userId: normalizedUserId || null,
    companyId: String(companyId || '').trim() || null,
    conversationId: String(conversationId || '').trim() || null,
    data
  };

  try {
    const queued = await enqueueRealtimeOutboxEvent({
      eventType: eventType || String(data?.type || 'realtime_event').trim(),
      scope: 'user',
      userId: normalizedUserId,
      companyId,
      conversationId,
      payload,
      dedupeKey:
        String(dedupeKey || '').trim() ||
        `${eventType || String(data?.type || 'realtime_event').trim()}:${String(
          conversationId || normalizedUserId || crypto.randomUUID()
        ).trim()}`
    });
    return Boolean(queued);
  } catch (error) {
    console.error(`Failed to enqueue ${eventType || 'realtime'} user event:`, error?.message || error);
    return false;
  }
};

const buildMessageSentRealtimeData = ({ companyId, conversationId, message, conversation = null, relatedConversationIds = [] }) => ({
  type: 'message_sent',
  companyId: String(companyId || '').trim() || null,
  conversationId: String(conversationId || message?.conversationId || '').trim() || null,
  ...(conversation ? { conversation } : {}),
  relatedConversationIds: Array.isArray(relatedConversationIds)
    ? relatedConversationIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [],
  message: message?.toObject ? message.toObject() : message
});

const collectRealtimeRecipientUserIds = async ({ userId = '', companyId = '', relatedConversationIds = [] } = {}) => {
  const recipientIds = new Set([String(userId || '').trim()].filter(Boolean));
  const conversationIds = Array.from(
    new Set(
      (Array.isArray(relatedConversationIds) ? relatedConversationIds : [])
        .map((id) => String(id || '').trim())
        .filter(Boolean)
    )
  );

  if (conversationIds.length === 0) {
    return Array.from(recipientIds);
  }

  const relatedConversations = await Conversation.find({
    _id: { $in: conversationIds },
    ...(companyId ? { companyId } : {})
  })
    .select('userId assignedTo assignedToId assignedAgent')
    .lean();

  relatedConversations.forEach((conversation) => {
    [
      conversation?.userId,
      conversation?.assignedTo,
      conversation?.assignedToId,
      conversation?.assignedAgent
    ].forEach((candidateId) => {
      const normalizedId = String(candidateId || '').trim();
      if (normalizedId) recipientIds.add(normalizedId);
    });
  });

  return Array.from(recipientIds);
};

const fanoutMessageSentRealtime = async ({
  req,
  companyId,
  conversationId,
  message,
  conversation = null,
  relatedConversationIds = []
}) => {
  const sendToUser = req?.app?.locals?.sendToUser;
  if (typeof sendToUser !== 'function') {
    return false;
  }

  const payload = buildMessageSentRealtimeData({
    companyId,
    conversationId,
    message,
    conversation,
    relatedConversationIds
  });
  const recipientUserIds = await collectRealtimeRecipientUserIds({
    userId: req?.user?.id,
    companyId,
    relatedConversationIds: [conversationId, ...relatedConversationIds]
  });

  await Promise.all(
    recipientUserIds.map((recipientUserId) => sendToUser(String(recipientUserId), payload))
  );
  return true;
};

const enqueueMessageSentEvent = async ({ userId, companyId, conversationId, message, conversation = null, relatedConversationIds = [] }) =>
  enqueueUserRealtimeEvent({
    userId,
    companyId,
    conversationId,
    eventType: 'message_sent',
    data: buildMessageSentRealtimeData({
      companyId,
      conversationId,
      message,
      conversation,
      relatedConversationIds
    }),
    dedupeKey: `message_sent:${String(message?._id || message?.whatsappMessageId || crypto.randomUUID()).trim()}`
  });

const resolveContactForConversation = async ({
  userId,
  companyId,
  conversation,
  isTenantWide = false
}) => {
  if (!conversation?._id || (!userId && !isTenantWide)) return null;

  if (conversation.contactId) {
    const contactById = await Contact.findOne(
      isTenantWide
        ? {
            _id: conversation.contactId,
            ...(companyId ? { companyId } : {})
          }
        : {
            _id: conversation.contactId,
            userId,
            ...(companyId ? { companyId } : {})
          }
    );

    if (contactById) return contactById;
  }

  const phoneCandidates = buildPhoneCandidates(conversation.contactPhone || '');
  if (!phoneCandidates.length) return null;

  return Contact.findOne(
    isTenantWide
      ? {
          ...(companyId ? { companyId } : {}),
          phone: { $in: phoneCandidates }
        }
      : {
          userId,
          ...(companyId ? { companyId } : {}),
          phone: { $in: phoneCandidates }
        }
  );
};

const resolveAttachmentMessageFilters = ({ req, messageId }) => {
  const filters = { _id: messageId, userId: req.user.id };
  if (req.companyId) {
    filters.companyId = req.companyId;
  }
  return filters;
};

const resolveLatestInboundConversationActivity = async ({ req, conversationId, conversation }) => {
  const relatedConversationIds = new Set();
  const normalizedConversationId = String(conversationId || conversation?._id || '').trim();
  if (normalizedConversationId) {
    relatedConversationIds.add(normalizedConversationId);
  }

  if (conversation) {
    try {
      const resolvedIds = await resolveRelatedConversationIds({
        Conversation,
        ConversationSummary,
        req,
        conversation,
        includeAllIdentityMatches: true
      });
      resolvedIds.forEach((id) => {
        const normalizedId = String(id || '').trim();
        if (normalizedId) relatedConversationIds.add(normalizedId);
      });
    } catch (lookupError) {
      console.warn(
        'Failed to resolve related inbound conversations for send policy:',
        lookupError?.message || lookupError
      );
    }
  }

  const filters = buildScopedMessageFilters(req, {
    conversationId:
      relatedConversationIds.size > 1
        ? { $in: Array.from(relatedConversationIds) }
        : normalizedConversationId,
    sender: 'contact'
  });
  return Message.findOne(filters)
    .select('_id timestamp whatsappTimestamp createdAt')
    .sort({ timestamp: -1, _id: -1 })
    .lean();
};

const loadAuthorizedAttachmentMessage = async ({ req, messageId }) => {
  const filters = resolveAttachmentMessageFilters({ req, messageId });
  const message = await Message.findOne(filters)
    .select('_id mediaUrl mediaType mediaCaption attachment')
    .lean();

  if (!message) {
    const error = new Error('Attachment message not found');
    error.status = 404;
    throw error;
  }

  const attachment = message?.attachment || {};
  if (!attachment?.publicId && !message?.mediaUrl) {
    const error = new Error('No attachment found for this message');
    error.status = 404;
    throw error;
  }

  const storageUsername = resolveAttachmentUsername(req);
  const attachmentOwnerSegment = String(attachment?.username || storageUsername || '').trim();
  if (
    attachment?.publicId &&
    !isAttachmentPathOwned({
      publicId: attachment.publicId,
      username: attachmentOwnerSegment,
      companyContext: resolveCompanyStorageContext(req)
    })
  ) {
    const error = new Error('Attachment does not belong to current user storage path');
    error.status = 403;
    throw error;
  }

  return message;
};

const encodeContentDispositionFileName = (fileName = '') =>
  encodeURIComponent(String(fileName || 'attachment').trim() || 'attachment').replace(
    /['()]/g,
    escape
  );

const resolveAttachmentDownloadFileName = (message = {}) => {
  const attachment = message?.attachment || {};
  const originalFileName = String(attachment?.originalFileName || '').trim();
  if (originalFileName) return originalFileName;

  const extension = String(attachment?.extension || '').trim().toLowerCase();
  const mediaType = String(message?.mediaType || attachment?.fileCategory || 'attachment')
    .trim()
    .toLowerCase();
  return `${mediaType || 'attachment'}${extension ? `.${extension}` : ''}`;
};

const resolveAttachmentAccessUrls = ({
  message = {},
  expiresInSeconds = 300,
  mode = 'view'
}) => {
  const attachment = message?.attachment || {};
  const directUrl = String(message?.mediaUrl || attachment?.secureUrl || '').trim();
  const normalizedMode = String(mode || 'view').trim().toLowerCase();
  const signed =
    normalizedMode === 'download'
      ? generateAttachmentDownloadUrl({
          attachment: {
            ...attachment,
            secureUrl: directUrl
          },
          expiresInSeconds
        })
      : generateSignedAttachmentUrl({
          attachment: {
            ...attachment,
            secureUrl: directUrl
          },
          mode: normalizedMode,
          expiresInSeconds
        });
  const signedUrl = String(signed?.url || '').trim();
  const orderedUrls =
    normalizedMode === 'download'
      ? Array.from(new Set([signedUrl, directUrl].filter(Boolean)))
      : Array.from(new Set([directUrl, signedUrl].filter(Boolean)));

  return {
    directUrl,
    signedUrl,
    expiresAt: signed?.expiresAt || null,
    urls: orderedUrls
  };
};

const fetchAttachmentUpstream = async (urls = []) => {
  let lastStatus = 0;
  let lastErrorMessage = 'Attachment download failed';

  for (const candidateUrl of urls) {
    try {
      const upstreamResponse = await fetch(candidateUrl, {
        method: 'GET',
        redirect: 'follow'
      });

      if (upstreamResponse.ok) {
        const arrayBuffer = await upstreamResponse.arrayBuffer();
        const headers = {};
        upstreamResponse.headers.forEach((value, key) => {
          headers[String(key).toLowerCase()] = value;
        });

        return {
          status: upstreamResponse.status,
          headers,
          data: Buffer.from(arrayBuffer)
        };
      }

      const statusCode = Number(upstreamResponse.status || 0);
      if (statusCode > 0) {
        lastStatus = statusCode;
      }
      lastErrorMessage = `Attachment download failed with status ${statusCode || 502}`;
    } catch (error) {
      lastErrorMessage = String(error?.message || '').trim() || 'Attachment download failed';
    }
  }

  const error = new Error(lastErrorMessage);
  error.status = lastStatus || 502;
  throw error;
};

const mapAttachmentSummary = (message = {}) => {
  const attachment = message?.attachment || {};
  return {
    messageId: String(message?._id || ''),
    conversationId: String(message?.conversationId || ''),
    sender: String(message?.sender || ''),
    senderName: String(message?.senderName || ''),
    recipient:
      String(attachment?.recipient || '').trim() ||
      (String(message?.sender || '').trim() === 'agent' ? 'contact' : 'agent'),
    type:
      String(attachment?.fileCategory || '').trim() ||
      String(message?.mediaType || '').trim() ||
      'document',
    mediaType: String(message?.mediaType || '').trim(),
    fileName: String(attachment?.originalFileName || '').trim(),
    mimeType: String(attachment?.mimeType || '').trim(),
    extension: String(attachment?.extension || '').trim(),
    sizeBytes: Number(attachment?.bytes || 0),
    status: String(message?.status || '').trim(),
    timestamp: message?.timestamp || message?.createdAt || null,
    uploadedAt: attachment?.uploadedAt || null,
    hasAttachment: Boolean(attachment?.publicId || message?.mediaUrl)
  };
};

const runAttachmentUpload = (req, res) =>
  new Promise((resolve, reject) => {
    upload.single('file')(req, res, (error) => {
      if (error) return reject(error);
      return resolve();
    });
  });

const runTemplateHeaderUpload = (req, res) =>
  new Promise((resolve, reject) => {
    upload.single('file')(req, res, (error) => {
      if (error) return reject(error);
      return resolve();
    });
  });

const buildScopedMessageFilters = (req, extra = {}, options = {}) => {
  const normalizedRole = normalizeRole(
    req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role
  );
  const normalizedCompanyId = String(req?.companyId || req?.user?.companyId || '').trim();
  const normalizedScope = String(options?.scope || '').trim().toLowerCase();
  const shouldSkipUserScope = normalizedScope === 'team';
  const filters = {
    ...(normalizedCompanyId ? { companyId: normalizedCompanyId } : {}),
    ...extra
  };

  if (!shouldSkipUserScope && !isTenantWideRole(normalizedRole)) {
    filters.userId = req.user.id;
  }

  return filters;
};

router.post(
  '/template-header-media',
  attachmentRateLimit,
  requirePlanFeature('broadcastMessaging'),
  async (req, res) => {
    try {
      await runTemplateHeaderUpload(req, res);

      if (!req.file) {
        return res.status(400).json({
          success: false,
          error: 'Image file is required'
        });
      }

      const folder = resolveBroadcastTemplateHeaderFolder(req);
      const mediaUrl = await uploadCampaignCreative(req.file, {
        folder,
        resourceType: 'auto'
      });

      return res.json({
        success: true,
        data: {
          mediaUrl,
          folder
        }
      });
    } catch (error) {
      return res.status(error?.status || 500).json({
        success: false,
        error: error?.message || 'Failed to upload template header image'
      });
    }
  }
);

router.post(
  '/react',
  sendMessageRateLimit,
  requirePlanFeature('teamInbox'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      const mediaPipelineRequestId =
        String(req.headers?.['x-request-id'] || '').trim() ||
        `reaction-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
      const {
        to,
        conversationId,
        targetMessageId = '',
        targetWhatsAppMessageId = '',
        emoji = ''
      } = req.body || {};

      if (!to || !conversationId) {
        return res.status(400).json({
          success: false,
          error: 'to and conversationId are required',
          mediaPipelineRequestId
        });
      }

      const conversation = await resolveConversationForOutboundSend({
        userId: req.user.id,
        companyId: req.companyId || null,
        conversationId,
        to,
        isTenantWide: isTenantWideRole(
          normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
        )
      });
      if (!conversation) {
        return res.status(400).json({
          success: false,
          error: 'Conversation not found for provided conversationId'
        });
      }

      const messageCompanyId = conversation.companyId || req.companyId || null;
      const reactionTarget = await resolveReactionTargetForOutboundSend({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        targetMessageId,
        targetWhatsAppMessageId,
        isTenantWide: isTenantWideRole(
          normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
        )
      });

      if (!reactionTarget?._id || !String(reactionTarget?.whatsappMessageId || '').trim()) {
        return res.status(400).json({
          success: false,
          error: 'Target message was not found or cannot be reacted to'
        });
      }

      if (String(reactionTarget?.rawMessageType || '').trim().toLowerCase() === 'reaction') {
        return res.status(400).json({
          success: false,
          error: 'WhatsApp reactions cannot be added to reaction messages'
        });
      }

      const reactionTargetTimestamp = new Date(
        reactionTarget?.whatsappTimestamp ||
          reactionTarget?.timestamp ||
          reactionTarget?.createdAt ||
          Date.now()
      );
      if (
        !Number.isNaN(reactionTargetTimestamp.valueOf()) &&
        Date.now() - reactionTargetTimestamp.getTime() > 30 * 24 * 60 * 60 * 1000
      ) {
        return res.status(400).json({
          success: false,
          error: 'WhatsApp reactions can only be sent to messages from the last 30 days'
        });
      }

      const normalizedEmoji = String(emoji || '').trim();
      const sendResult = await whatsappService.sendReactionMessage(
        to,
        reactionTarget.whatsappMessageId,
        normalizedEmoji,
        req.whatsappCredentials
      );

      if (!sendResult.success) {
        return res.status(400).json({
          success: false,
          error: sendResult.error || 'Failed to send reaction'
        });
      }

      const whatsappMessageId =
        sendResult?.data?.messages?.[0]?.id || sendResult?.data?.messageId || null;
      const reactionText = normalizedEmoji ? `Reacted with ${normalizedEmoji}` : '[Reaction removed]';
      const threadTimestamp = new Date();

      const message = await Message.create({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        sender: 'agent',
        ...resolveOutboundSenderMeta(req.user),
        text: reactionText,
        rawMessageType: 'reaction',
        reactionEmoji: normalizedEmoji || undefined,
        whatsappContextMessageId: reactionTarget.whatsappMessageId,
        whatsappMessageId,
        status: 'sent',
        timestamp: threadTimestamp
      });
      const relatedConversationIds = await resolveRelatedConversationIds({
        Conversation,
        ConversationSummary,
        req,
        conversation,
        includeAllIdentityMatches: true
      });

      await writeConversationThreadState({
        conversation,
        companyId: messageCompanyId || '',
        userId: req.user.id || '',
        relatedConversationIds,
        conversationUpdate: {
          lastMessageTime: threadTimestamp,
          lastMessage: reactionText,
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent'
        },
        summaryUpdate: {
          lastMessageTime: threadTimestamp,
          lastMessage: reactionText,
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent'
        }
      });

      const queuedRealtimeEvent = await enqueueMessageSentEvent({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        message,
        conversation: conversation?.toObject ? conversation.toObject() : conversation,
        relatedConversationIds
      });
      const realtimeSent = await fanoutMessageSentRealtime({
        req,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        message,
        conversation: conversation?.toObject ? conversation.toObject() : conversation,
        relatedConversationIds
      });
      if (!realtimeSent && !queuedRealtimeEvent) {
        console.warn('message_sent realtime event was not queued and no websocket sender is available.');
      } else if (!queuedRealtimeEvent) {
        console.warn('message_sent realtime event was not queued; direct websocket fanout was used.');
      }

      return res.json({ success: true, message });
    } catch (error) {
      console.error('React to message error:', error);
      return res.status(500).json({
        success: false,
        error: error?.message || 'Failed to send reaction'
      });
    }
  }
);

router.post(
  '/template-header-media',
  attachmentRateLimit,
  requirePlanFeature('templates'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      await runAttachmentUpload(req, res);
      if (!req.file) {
        return res.status(400).json({ success: false, error: 'Template header media file is required' });
      }

      const companyContext = resolveCompanyStorageContext(req);
      const folder = resolveCompanyFolders(companyContext).metaTemplateImagesFolder;
      const mediaUrl = await uploadCampaignCreative(req.file, {
        folder,
        resourceType: 'auto'
      });

      return res.json({
        success: true,
        data: {
          mediaUrl,
          url: mediaUrl,
          folder
        }
      });
    } catch (error) {
      const statusCode = Number(error?.status || 500);
      return res.status(statusCode).json({
        success: false,
        error: error?.message || 'Failed to upload template header media'
      });
    }
  }
);

router.post(
  '/send-template',
  sendMessageRateLimit,
  requirePlanFeature('teamInbox'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      const {
        to,
        templateName,
        language = 'en_US',
        variables = [],
        conversationId,
        contactId,
        contactName = '',
        components = [],
        templateCategory = ''
      } = req.body || {};

      if (!to || !templateName) {
        return res.status(400).json({
          success: false,
          error: 'to and templateName are required'
        });
      }

      const normalizedVariables = Array.isArray(variables)
        ? variables.map((value) => String(value ?? '').trim())
        : [];
      const normalizedComponents = Array.isArray(components) ? components : [];

      const outreachTarget = await resolveOrCreateConversationForTemplateSend({
        userId: req.user.id,
        companyId: req.companyId || null,
        conversationId,
        contactId,
        contactName,
        to,
        isTenantWide: isTenantWideRole(
          normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
        )
      });
      const {
        conversation,
        contact,
        createdContact,
        createdConversation
      } = outreachTarget;
      if (!conversation) {
        return res.status(400).json({
          success: false,
          error: 'Unable to resolve or create a conversation for this contact'
        });
      }

      let normalizedTemplateCategory = toCleanString(templateCategory).toLowerCase();
      if (!normalizedTemplateCategory && isValidObjectId(req.user.id)) {
        const templateRecord = await Template.findOne({
          userId: req.user.id,
          name: String(templateName || '').trim()
        })
          .sort({ updatedAt: -1, createdAt: -1 })
          .select('category')
          .lean();

        normalizedTemplateCategory = toCleanString(templateRecord?.category).toLowerCase() || 'utility';
      }
      if (!normalizedTemplateCategory) {
        normalizedTemplateCategory = 'utility';
      }
      const templateValidation = validateTemplateOutboundSend(contact || {}, {
        templateCategory: normalizedTemplateCategory
      });
      if (!templateValidation.ok) {
        await cleanupCreatedTemplateOutreachTarget(outreachTarget);
        return res.status(templateValidation.statusCode || 403).json({
          success: false,
          error: templateValidation.error,
          policy: templateValidation.policy
        });
      }

      const result = await whatsappService.sendTemplateMessage(
        to,
        templateName,
        language || 'en_US',
        normalizedVariables,
        req.whatsappCredentials,
        true,
        normalizedComponents
      );

      if (!result.success) {
        await cleanupCreatedTemplateOutreachTarget(outreachTarget);
        return res.status(400).json({ success: false, error: result.error });
      }

      const whatsappMessageId =
        result?.data?.messages?.[0]?.id || result?.data?.messageId || null;

      const messageCompanyId = conversation.companyId || req.companyId || null;
      const previewSuffix =
        normalizedVariables.filter(Boolean).length > 0
          ? ` (${normalizedVariables.filter(Boolean).join(', ')})`
          : '';
      const previewText = `Template: ${templateName}${previewSuffix}`;
      const messageTimestamp = new Date();

      const message = await Message.create({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        sender: 'agent',
        ...resolveOutboundSenderMeta(req.user),
        text: previewText,
        whatsappMessageId,
        status: 'sent',
        timestamp: messageTimestamp
      });

      await markOutboundTemplateContactActivity({
        contact,
        contactName
      });
      const relatedConversationIds = await resolveRelatedConversationIds({
        Conversation,
        ConversationSummary,
        req,
        conversation,
        includeAllIdentityMatches: true
      });

      if (normalizedTemplateCategory === 'marketing' && contact) {
        applyMarketingTemplateSent(contact, { now: messageTimestamp });
        await contact.save();
      }

      await writeConversationThreadState({
        conversation,
        companyId: messageCompanyId || '',
        userId: req.user.id || '',
        relatedConversationIds,
        conversationUpdate: {
          lastMessageTime: messageTimestamp,
          lastMessage: previewText,
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent',
          contactName:
            String(contact?.name || conversation.contactName || contactName || '').trim() ||
            conversation.contactName
        },
        summaryUpdate: {
          contactName:
            String(contact?.name || conversation.contactName || contactName || '').trim() ||
            conversation.contactName,
          lastMessageTime: messageTimestamp,
          lastMessage: previewText,
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent'
        }
      });

      const queuedRealtimeEvent = await enqueueMessageSentEvent({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        message,
        conversation: conversation?.toObject ? conversation.toObject() : conversation,
        relatedConversationIds
      });
      const realtimeSent = await fanoutMessageSentRealtime({
        req,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        message,
        conversation: conversation?.toObject ? conversation.toObject() : conversation,
        relatedConversationIds
      });
      if (!realtimeSent && !queuedRealtimeEvent) {
        console.warn('message_sent realtime event was not queued and no websocket sender is available.');
      } else if (!queuedRealtimeEvent) {
        console.warn('message_sent realtime event was not queued; direct websocket fanout was used.');
      }

      return res.json({
        success: true,
        message,
        conversationId: conversation._id,
        contactId: contact?._id || conversation.contactId,
        createdConversation,
        createdContact
      });
    } catch (error) {
      console.error('Send template message error:', error);
      return res.status(500).json({ success: false, error: error.message });
    }
  }
);

router.post(
  '/send-attachment',
  attachmentRateLimit,
  requirePlanFeature('teamInbox'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      const mediaPipelineRequestId =
        String(req.headers?.['x-request-id'] || '').trim() ||
        `media-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
      await runAttachmentUpload(req, res);

      const {
        to,
        conversationId,
        caption = '',
        replyToMessageId = '',
        whatsappContextMessageId = '',
        conversationLastInboundMessageAt = ''
      } = req.body || {};
      if (!to || !conversationId) {
        return res.status(400).json({
          success: false,
          error: 'to and conversationId are required'
        });
      }

      if (!req.file) {
        return res.status(400).json({
          success: false,
          error: 'Attachment file is required',
          mediaPipelineRequestId
        });
      }

      const conversation = await resolveConversationForOutboundSend({
        userId: req.user.id,
        companyId: req.companyId || null,
        conversationId,
        to,
        isTenantWide: isTenantWideRole(
          normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
        )
      });
      if (!conversation) {
        return res.status(400).json({
          success: false,
          error: 'Conversation not found for provided conversationId',
          mediaPipelineRequestId
        });
      }

      const outboundContact = await resolveContactForConversation({
        userId: req.user.id,
        companyId: conversation.companyId || req.companyId || null,
        conversation,
        isTenantWide: isTenantWideRole(
          normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
        )
      });
      const parsedConversationLastInboundMessageAt = new Date(
        String(conversationLastInboundMessageAt || '').trim()
      );
      const hasConversationLastInboundMessageAt =
        !Number.isNaN(parsedConversationLastInboundMessageAt.getTime());
      let freeformValidation = outboundContact
        ? validateFreeformOutboundSend(outboundContact)
        : { ok: true, policy: null };
      if (!freeformValidation.ok && outboundContact && hasConversationLastInboundMessageAt) {
        const fallbackPolicy = getWhatsAppMessagingPolicy(outboundContact, {
          conversationLastInboundMessageAt: parsedConversationLastInboundMessageAt
        });
        if (fallbackPolicy.freeformAllowed) {
          freeformValidation = { ok: true, policy: fallbackPolicy };
        }
      }
      if (!freeformValidation.ok && outboundContact) {
        const latestInboundActivity = await resolveLatestInboundConversationActivity({
          req,
          conversationId: conversation._id,
          conversation
        });
        if (latestInboundActivity) {
          const fallbackPolicy = getWhatsAppMessagingPolicy(outboundContact, {
            conversationLastInboundMessageAt:
              latestInboundActivity.timestamp ||
              latestInboundActivity.whatsappTimestamp ||
              latestInboundActivity.createdAt ||
              null
          });
          if (fallbackPolicy.freeformAllowed) {
            freeformValidation = { ok: true, policy: fallbackPolicy };
          }
        }
      }
      if (!freeformValidation.ok) {
        return res.status(freeformValidation.statusCode || 403).json({
          success: false,
          error: freeformValidation.error,
          policy: freeformValidation.policy,
          mediaPipelineRequestId
        });
      }

      const storageUsername = resolveAttachmentUsername(req);
      const normalizedCaption = String(caption || '').trim();
      const messageCompanyId = conversation.companyId || req.companyId || null;
      const replyReference = await resolveReplyReferenceForOutboundSend({
        userId: req.user.id,
        companyId: messageCompanyId,
        replyToMessageId,
        whatsappContextMessageId,
        isTenantWide: isTenantWideRole(
          normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role)
        )
      });
      const resolvedReplyContextMessageId =
        String(whatsappContextMessageId || replyReference?.whatsappMessageId || '').trim();

      const attachment = await uploadInboxAttachment({
        file: req.file,
        username: storageUsername,
        direction: 'sent',
        companyContext: resolveCompanyStorageContext(req),
        userId: req.user.id,
        sender: req.user.id,
        recipient: to
      });

      const normalizedFileCategory = String(attachment?.fileCategory || '').trim().toLowerCase();
      const normalizedMimeType = String(attachment?.mimeType || req.file?.mimetype || '')
        .trim()
        .toLowerCase();
      const normalizedExtension = String(attachment?.extension || '').trim().toLowerCase();
      if (
        normalizedFileCategory === 'video' ||
        normalizedMimeType === 'image/webp' ||
        normalizedExtension === 'webp'
      ) {
        try {
          await deleteInboxAttachment({ attachment });
        } catch (_cleanupError) {
          // best-effort cleanup
        }
        return res.status(415).json({
          success: false,
          error: 'This website only supports sending images, audio, and documents.',
          errorCode: 'UNSUPPORTED_OUTBOUND_MEDIA_TYPE',
          errorDetails: `Outbound media type "${normalizedFileCategory || normalizedMimeType || normalizedExtension || 'unknown'}" is disabled.`,
          mediaPipelineRequestId
        });
      }
      const mediaType =
        normalizedFileCategory === 'image'
          ? 'image'
          : normalizedFileCategory === 'audio'
            ? 'audio'
            : 'document';
      const mediaUploadResult = await whatsappService.uploadMediaAsset(
        req.file,
        req.whatsappCredentials,
        {
          debugContext: {
            requestId: mediaPipelineRequestId,
            conversationId: conversation._id,
            to,
            mediaType
          }
        }
      );
      if (!mediaUploadResult.success) {
        try {
          await deleteInboxAttachment({ attachment });
        } catch (_cleanupError) {
          // no-op: best-effort cleanup on downstream send failure
        }

        return res.status(400).json({
          success: false,
          error: mediaUploadResult.error || 'Failed to upload media to WhatsApp',
          errorCode: mediaUploadResult.errorCode || null,
          errorDetails: mediaUploadResult.errorDetails || null,
          mediaPipelineRequestId
        });
      }
      const uploadedMetaMediaId = String(
        mediaUploadResult?.data?.id ||
          mediaUploadResult?.data?.media_id ||
          mediaUploadResult?.data?.mediaId ||
          mediaUploadResult?.media_id ||
          mediaUploadResult?.mediaId ||
          ''
      ).trim();
      if (!uploadedMetaMediaId) {
        try {
          console.warn('WhatsApp media upload returned no media id', {
            conversationId: String(conversation?._id || ''),
            mediaType,
            mediaUploadResult: mediaUploadResult?.data || mediaUploadResult
          });
        } catch (_logError) {
          // ignore logging failures
        }
      }

      const sendResult = await whatsappService.sendMediaMessage(
        to,
        mediaType,
        attachment.secureUrl,
        normalizedCaption,
        req.whatsappCredentials,
        {
          whatsappContextMessageId: resolvedReplyContextMessageId,
          fileName: mediaType === 'document' ? attachment?.originalFileName : '',
          mediaId: uploadedMetaMediaId,
          debugContext: {
            requestId: mediaPipelineRequestId,
            conversationId: conversation._id,
            to,
            mediaType,
            cloudinary: attachment,
            meta: mediaUploadResult?.data || null
          }
        }
      );

      if (!sendResult.success) {
        try {
          await deleteInboxAttachment({ attachment });
        } catch (_cleanupError) {
          // no-op: best-effort cleanup on downstream send failure
        }

        return res.status(400).json({
          success: false,
          error: sendResult.error || 'Failed to send media message',
          errorCode: sendResult.errorCode || null,
          errorDetails: sendResult.errorDetails || null,
          mediaPipelineRequestId
        });
      }

      const whatsappMessageId =
        sendResult?.data?.messages?.[0]?.id || sendResult?.data?.messageId || null;
      const messageText = normalizedCaption || buildAttachmentLabel(mediaType);
      const messageTimestamp = new Date();

      const message = await Message.create({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        sender: 'agent',
        ...resolveOutboundSenderMeta(req.user),
        text: messageText,
        mediaUrl: attachment.secureUrl,
        mediaType,
        mediaCaption: normalizedCaption || undefined,
        mediaPipelineRequestId,
        attachment,
        replyTo: replyReference?._id || undefined,
        whatsappContextMessageId: resolvedReplyContextMessageId || undefined,
        whatsappMessageId,
        status: 'sent',
        timestamp: messageTimestamp
      });
      await message.populate(
        'replyTo',
        '_id text sender senderRole senderName senderId whatsappMessageId mediaType mediaCaption timestamp'
      );
      const relatedConversationIds = await resolveRelatedConversationIds({
        Conversation,
        ConversationSummary,
        req,
        conversation,
        includeAllIdentityMatches: true
      });

      await writeConversationThreadState({
        conversation,
        companyId: messageCompanyId || '',
        userId: req.user.id || '',
        relatedConversationIds,
        conversationUpdate: {
          lastMessageTime: messageTimestamp,
          lastMessage: messageText,
          lastMessageMediaType: mediaType,
          lastMessageAttachmentName:
            mediaType === 'document' ? String(attachment?.originalFileName || '').trim() : '',
          lastMessageAttachmentPages:
            mediaType === 'document' ? Number(attachment?.pages || 0) || null : null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent'
        },
        summaryUpdate: {
          lastMessageTime: messageTimestamp,
          lastMessage: messageText,
          lastMessageMediaType: mediaType,
          lastMessageAttachmentName:
            mediaType === 'document' ? String(attachment?.originalFileName || '').trim() : '',
          lastMessageAttachmentPages:
            mediaType === 'document' ? Number(attachment?.pages || 0) || null : null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || '',
          lastMessageStatus: 'sent'
        }
      });

      const queuedRealtimeEvent = await enqueueMessageSentEvent({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        message,
        conversation: conversation?.toObject ? conversation.toObject() : conversation,
        relatedConversationIds
      });
      const realtimeSent = await fanoutMessageSentRealtime({
        req,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        message,
        conversation: conversation?.toObject ? conversation.toObject() : conversation,
        relatedConversationIds
      });
      if (!realtimeSent && !queuedRealtimeEvent) {
        console.warn('message_sent realtime event was not queued and no websocket sender is available.');
      } else if (!queuedRealtimeEvent) {
        console.warn('message_sent realtime event was not queued; direct websocket fanout was used.');
      }

      return res.json({
        success: true,
        message,
        attachment: mapAttachmentSummary(message)
      });
    } catch (error) {
      if (error?.name === 'MulterError') {
        const errorMessage =
          error?.code === 'LIMIT_FILE_SIZE'
            ? 'File size exceeds allowed upload limit.'
            : error?.message || 'Attachment upload failed';
        return res.status(400).json({
          success: false,
          error: errorMessage
        });
      }

      const statusCode = Number(error?.status || 500);
      console.error('Send attachment message error:', error);
      return res.status(statusCode).json({
        success: false,
        error: error?.message || 'Failed to send attachment message',
        errorCode: error?.code || null,
        errorDetails: error?.response?.data?.error?.message || error?.response?.data?.message || null,
        mediaPipelineRequestId
      });
    }
  }
);

router.get('/attachments', async (req, res) => {
  try {
    const {
      conversationId = '',
      limit: rawLimit = '30',
      cursor = '',
      type = ''
    } = req.query || {};

    const limit = Math.max(1, Math.min(normalizePageLimit(rawLimit), 50));

      const filters = buildScopedMessageFilters(req, {
        $or: [{ 'attachment.publicId': { $exists: true, $ne: '' } }, { mediaUrl: { $exists: true, $ne: '' } }]
      });

    const normalizedConversationId = String(conversationId || '').trim();
    if (normalizedConversationId) {
      filters.conversationId = normalizedConversationId;
    }

    const normalizedType = String(type || '').trim().toLowerCase();
    if (normalizedType === 'image' || normalizedType === 'document' || normalizedType === 'audio') {
      filters.mediaType = normalizedType;
    }

    const cursorFilter = decodeMessageCursor(cursor);
    if (cursorFilter) {
      Object.assign(filters, buildMessageCursorFilter(cursorFilter));
    }

    const messages = await Message.find(filters)
      .sort({ timestamp: -1, _id: -1 })
      .limit(limit + 1)
      .select(
        '_id conversationId sender senderRole senderName senderId text mediaType mediaCaption mediaUrl status timestamp attachment'
      )
      .lean();

    const page = buildChronologicalPage({
      documents: messages,
      limit,
      encodeCursor: encodeAttachmentCursor
    });

    return res.json({
      success: true,
      data: page.items.map(mapAttachmentSummary),
      meta: {
        limit,
        hasMore: page.hasMore,
        nextCursor: page.nextCursor
      }
    });
  } catch (error) {
    console.error('List attachments error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to list attachments'
    });
  }
});

router.get('/attachments/:messageId/url', async (req, res) => {
  try {
    const { messageId } = req.params;
    const mode = String(req.query?.mode || 'view').trim().toLowerCase();
    const expiresInSeconds = Number(req.query?.ttl || 300);
    const message = await loadAuthorizedAttachmentMessage({ req, messageId });
    const { directUrl, signedUrl, expiresAt } = resolveAttachmentAccessUrls({
      message,
      mode,
      expiresInSeconds
    });
    const preferredUrl = directUrl || signedUrl || '';

    return res.json({
      success: true,
      data: {
        messageId: String(message._id),
        mode: mode === 'download' ? 'download' : 'view',
        url: preferredUrl,
        expiresAt: preferredUrl === directUrl ? null : expiresAt
      }
    });
  } catch (error) {
    console.error('Get attachment URL error:', error);
    const statusCode = Number(error?.status || 0) || 500;
    return res.status(statusCode).json({
      success: false,
      error: error?.message || 'Failed to generate attachment URL'
    });
  }
});

router.get('/attachments/:messageId/download', async (req, res) => {
  try {
    const { messageId } = req.params;
    const expiresInSeconds = Number(req.query?.ttl || 300);
    const message = await loadAuthorizedAttachmentMessage({ req, messageId });
    const attachment = message?.attachment || {};
    const { urls } = resolveAttachmentAccessUrls({
      message,
      mode: 'download',
      expiresInSeconds
    });

    if (!urls.length) {
      return res.status(404).json({
        success: false,
        error: 'Attachment URL unavailable'
      });
    }

    const upstreamResponse = await fetchAttachmentUpstream(urls);

    const buffer = Buffer.from(upstreamResponse.data);
    const contentType =
      String(upstreamResponse.headers['content-type'] || '').trim() ||
      String(attachment?.mimeType || '').trim() ||
      'application/octet-stream';
    const contentLength = Number(upstreamResponse.headers['content-length'] || buffer.length || 0);
    const fileName = resolveAttachmentDownloadFileName(message);

    res.setHeader('Content-Type', contentType);
    if (contentLength > 0) {
      res.setHeader('Content-Length', String(contentLength));
    }
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${fileName.replace(/"/g, '')}"; filename*=UTF-8''${encodeContentDispositionFileName(
        fileName
      )}`
    );
    res.setHeader('Cache-Control', 'private, no-store, max-age=0');
    return res.status(200).send(buffer);
  } catch (error) {
    console.error('Download attachment error:', error);
    const statusCode = Number(error?.status || 0) || 500;
    return res.status(statusCode).json({
      success: false,
      error: error?.message || 'Failed to download attachment'
    });
  }
});

router.delete('/attachments/:messageId', async (req, res) => {
  try {
    const mediaPipelineRequestId = String(req.headers?.['x-request-id'] || '').trim();
    const { messageId } = req.params;
    const filters = { _id: messageId, userId: req.user.id };
    if (req.companyId) {
      filters.companyId = req.companyId;
    }

    const message = await Message.findOne(filters);
    if (!message) {
      return res.status(404).json({
        success: false,
        error: 'Attachment message not found',
        mediaPipelineRequestId: mediaPipelineRequestId || null
      });
    }

    const attachment = message.attachment || {};
    if (!attachment?.publicId && !message.mediaUrl) {
      return res.status(404).json({
        success: false,
        error: 'No attachment found for this message',
        mediaPipelineRequestId: mediaPipelineRequestId || null
      });
    }

    const storageUsername = resolveAttachmentUsername(req);
    const attachmentOwnerSegment = String(attachment?.username || storageUsername || '').trim();
    if (
      attachment?.publicId &&
      !isAttachmentPathOwned({
        publicId: attachment.publicId,
        username: attachmentOwnerSegment,
        companyContext: resolveCompanyStorageContext(req)
      })
    ) {
      return res.status(403).json({
        success: false,
        error: 'Attachment does not belong to current user storage path',
        mediaPipelineRequestId: mediaPipelineRequestId || null
      });
    }

    if (attachment?.publicId) {
      try {
        await deleteInboxAttachment({ attachment });
      } catch (deleteError) {
        console.error('Cloudinary attachment delete failed:', deleteError.message);
      }
    }

    message.attachment = {
      ...attachment,
      deletedAt: new Date(),
      deletedBy: req.user.id
    };
    message.mediaUrl = '';
    message.mediaCaption = '';
    message.text = message.text || '[Attachment deleted]';
    await message.save();

    await invalidateInboxConversation({
      companyId: req.companyId || message.companyId || '',
      userId: req.user.id || '',
      conversationId: message.conversationId
    });

    const sendToUser = req.app?.locals?.sendToUser;
    const queuedRealtimeEvent = await enqueueUserRealtimeEvent({
      userId: req.user.id,
      companyId: req.companyId || message.companyId || '',
      conversationId: message.conversationId,
      eventType: 'message_attachment_deleted',
      data: {
        type: 'message_attachment_deleted',
        messageId: String(message._id),
        conversationId: String(message.conversationId || ''),
        mediaPipelineRequestId: mediaPipelineRequestId || null
      },
      dedupeKey: `message_attachment_deleted:${String(message._id || '')}:${mediaPipelineRequestId || 'no-request'}`
    });
    if (!queuedRealtimeEvent && typeof sendToUser === 'function') {
      sendToUser(String(req.user.id), {
        type: 'message_attachment_deleted',
        messageId: String(message._id),
        conversationId: String(message.conversationId || ''),
        mediaPipelineRequestId: mediaPipelineRequestId || null
      });
    }

    return res.json({
      success: true,
      message: 'Attachment deleted successfully',
      mediaPipelineRequestId: mediaPipelineRequestId || null
    });
  } catch (error) {
    console.error('Delete attachment error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to delete attachment',
      mediaPipelineRequestId: String(req.headers?.['x-request-id'] || '').trim() || null
    });
  }
});

// Delete selected messages
router.delete('/delete-selected', async (req, res) => {
  try {
    const { messageIds } = req.body;
    
    if (!messageIds || !Array.isArray(messageIds)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Message IDs array is required' 
      });
    }
    
    const deleteFilter = { _id: { $in: messageIds }, userId: req.user.id };
    if (req.companyId) {
      deleteFilter.companyId = req.companyId;
    }

    const affectedConversationIds = await Message.distinct('conversationId', deleteFilter);
    const deleteResult = await Message.deleteMany(deleteFilter);

    if (Array.isArray(affectedConversationIds) && affectedConversationIds.length > 0) {
      await Promise.all(
        affectedConversationIds.map((conversationId) =>
          invalidateInboxConversation({
            companyId: req.companyId || '',
            userId: req.user.id || '',
            conversationId
          })
        )
      );
    } else {
      await invalidateInboxScope({
        companyId: req.companyId || '',
        userId: req.user.id || ''
      });
    }
    
    res.json({ 
      success: true, 
      message: `${deleteResult.deletedCount} messages deleted successfully`,
      deletedCount: deleteResult.deletedCount
    });
  } catch (error) {
    console.error('Error deleting messages:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/:conversationId', threadReadRateLimit, async (req, res) => {
  try {
    setInboxNoCacheHeaders(res);

    const conversationId = String(req.params.conversationId || '').trim();
    if (!conversationId) {
      return res.status(400).json({
        success: false,
        error: 'conversationId is required'
      });
    }

    const rawCursor = String(req.query?.cursor || '').trim();
    const cursor = rawCursor ? decodeMessageIdCursor(rawCursor) : null;
    if (rawCursor && !cursor) {
      return res.status(400).json({
        success: false,
        error: 'Invalid cursor value'
      });
    }

    const limit = normalizePageLimit(req.query?.limit, {
      fallback: 20,
      max: 20
    });
    const normalizedScope = String(req.query?.scope || 'team').trim().toLowerCase() || 'team';
    const normalizedCompanyId = String(req.companyId || req.user?.companyId || '').trim();
    const normalizedUserId = String(req.user?.id || '').trim();
    const scopeVariants = getInboxScopeVariants({
      companyId: normalizedCompanyId,
      userId: normalizedUserId
    });
    const cacheScope = scopeVariants[0] || scopeVariants[1] || '';
    const cacheKeyScope = cacheScope ? `${cacheScope}:${conversationId}` : '';
    const cursorFilter = cursor ? buildMessageIdCursorFilter(cursor) : {};

    const loadMessagePage = async () => {
      const filters = buildScopedMessageFilters(
        req,
        {
          conversationId
        },
        {
          scope: normalizedScope
        }
      );

      Object.assign(filters, cursorFilter);

      const messages = await Message.find(filters)
        .sort({ _id: -1 })
        .limit(limit + 1)
        .select(
          '_id conversationId sender senderRole senderName senderId text mediaUrl mediaType mediaCaption status timestamp createdAt whatsappTimestamp whatsappMessageId whatsappContextMessageId rawMessageType reactionEmoji attachment attachments replyTo replyToMessageId errorMessage deliveredTo readBy'
        )
        .lean();

      return buildThreadPageResponse({
        documents: Array.isArray(messages) ? messages : [],
        limit,
        encodeCursor: encodeMessageIdCursor
      });
    };

    const response = cacheKeyScope
      ? await getOrSetCachedJson({
          namespace: 'messages',
          scope: cacheKeyScope,
          versionGroup: 'thread',
          keyParts: [String(limit), String(rawCursor || ''), normalizedScope],
          ttlSeconds: CACHE_TTL_SECONDS.messages,
          loader: loadMessagePage
        })
      : await loadMessagePage();

    return res.json(response);
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to fetch conversation messages'
    });
  }
});

module.exports = router;
