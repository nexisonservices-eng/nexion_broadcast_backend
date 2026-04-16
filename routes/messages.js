const express = require('express');
const router = express.Router();
const multer = require('multer');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const whatsappService = require('../services/whatsappService');
const {
  resolveInboxStorageUsername,
  uploadInboxAttachment,
  generateSignedAttachmentUrl,
  generateAttachmentDownloadUrl,
  isAttachmentPathOwned,
  deleteInboxAttachment
} = require('../services/inboxMediaService');
const auth = require('../middleware/auth');
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
  validateFreeformOutboundSend,
  validateTemplateOutboundSend,
  applyMarketingTemplateSent,
  toCleanString
} = require('../services/whatsappOutreach/policy');

router.use(auth);

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

const resolveReactionTargetForOutboundSend = async ({
  userId,
  companyId,
  conversationId,
  targetMessageId,
  targetWhatsAppMessageId
}) => {
  const baseFilters = {
    userId,
    conversationId
  };

  const normalizedTargetMessageId = String(targetMessageId || '').trim();
  if (normalizedTargetMessageId && isValidObjectId(normalizedTargetMessageId)) {
    const baseIdFilter = { ...baseFilters, _id: normalizedTargetMessageId };
    if (companyId) {
      const strictTarget = await Message.findOne({ ...baseIdFilter, companyId })
        .select('_id whatsappMessageId rawMessageType timestamp whatsappTimestamp createdAt')
        .lean();
      if (strictTarget) return strictTarget;
    }

    const byUserOnlyTarget = await Message.findOne(baseIdFilter)
      .select('_id whatsappMessageId rawMessageType timestamp whatsappTimestamp createdAt')
      .lean();
    if (byUserOnlyTarget) return byUserOnlyTarget;
  }

  const normalizedTargetWhatsAppMessageId = String(targetWhatsAppMessageId || '').trim();
  if (!normalizedTargetWhatsAppMessageId) return null;

  const baseWhatsAppFilter = {
    ...baseFilters,
    whatsappMessageId: normalizedTargetWhatsAppMessageId
  };
  if (companyId) {
    const strictWhatsAppTarget = await Message.findOne({ ...baseWhatsAppFilter, companyId })
      .select('_id whatsappMessageId rawMessageType timestamp whatsappTimestamp createdAt')
      .lean();
    if (strictWhatsAppTarget) return strictWhatsAppTarget;
  }

  return Message.findOne(baseWhatsAppFilter)
    .select('_id whatsappMessageId rawMessageType timestamp whatsappTimestamp createdAt')
    .lean();
};

const resolveAttachmentUsername = (req) =>
  resolveInboxStorageUsername({
    username: req?.user?.username,
    email: req?.user?.email,
    userId: req?.user?.id
  });

const buildAttachmentLabel = (mediaType = '') => {
  const normalized = String(mediaType || '').trim().toLowerCase();
  if (normalized === 'image') return '[Image]';
  if (normalized === 'audio') return '[Audio]';
  if (normalized === 'document') return '[Document]';
  if (normalized) return `[${normalized.charAt(0).toUpperCase()}${normalized.slice(1)}]`;
  return '[Attachment]';
};

const resolveContactForConversation = async ({ userId, companyId, conversation }) => {
  if (!conversation?._id || !userId) return null;

  if (conversation.contactId) {
    const contactById =
      (companyId
        ? await Contact.findOne({ _id: conversation.contactId, userId, companyId })
        : null) ||
      (await Contact.findOne({
        _id: conversation.contactId,
        userId,
        ...(companyId
          ? {
              $or: [{ companyId }, { companyId: null }, { companyId: { $exists: false } }]
            }
          : {})
      }));

    if (contactById) return contactById;
  }

  const phoneCandidates = buildPhoneCandidates(conversation.contactPhone || '');
  if (!phoneCandidates.length) return null;

  return Contact.findOne({
    userId,
    ...(companyId
      ? {
          $or: [{ companyId }, { companyId: null }, { companyId: { $exists: false } }]
        }
      : {}),
    phone: { $in: phoneCandidates }
  });
};

const resolveAttachmentMessageFilters = ({ req, messageId }) => {
  const filters = { _id: messageId, userId: req.user.id };
  if (req.companyId) {
    filters.companyId = req.companyId;
  }
  return filters;
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
    !isAttachmentPathOwned({ publicId: attachment.publicId, username: attachmentOwnerSegment })
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
      const upstreamResponse = await fetch(candidateUrl, { redirect: 'follow' });
      if (upstreamResponse.ok) {
        return upstreamResponse;
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

router.post(
  '/react',
  requirePlanFeature('broadcastMessaging'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
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
          error: 'to and conversationId are required'
        });
      }

      const conversation = await resolveConversationForOutboundSend({
        userId: req.user.id,
        companyId: req.companyId || null,
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
      const reactionTarget = await resolveReactionTargetForOutboundSend({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        targetMessageId,
        targetWhatsAppMessageId
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

      const message = await Message.create({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        sender: 'agent',
        text: reactionText,
        rawMessageType: 'reaction',
        reactionEmoji: normalizedEmoji || undefined,
        whatsappContextMessageId: reactionTarget.whatsappMessageId,
        whatsappMessageId,
        status: 'sent',
        timestamp: new Date()
      });

      const sendToUser = req.app?.locals?.sendToUser;
      if (typeof sendToUser === 'function') {
        sendToUser(String(req.user.id), {
          type: 'message_sent',
          message: message.toObject()
        });
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
  '/send-template',
  requirePlanFeature('broadcastMessaging'),
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
        to
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

      // Default to utility when category is not provided so existing template sends
      // are not incorrectly blocked by marketing opt-in policy.
      const normalizedTemplateCategory =
        toCleanString(templateCategory).toLowerCase() || 'utility';
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
        text: previewText,
        whatsappMessageId,
        status: 'sent',
        timestamp: messageTimestamp
      });

      await markOutboundTemplateContactActivity({
        contact,
        contactName
      });

      if (normalizedTemplateCategory === 'marketing' && contact) {
        applyMarketingTemplateSent(contact, { now: messageTimestamp });
        await contact.save();
      }

      await Conversation.updateOne(
        { _id: conversation._id },
        {
          lastMessageTime: messageTimestamp,
          lastMessage: previewText,
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          contactName:
            String(contact?.name || conversation.contactName || contactName || '').trim() ||
            conversation.contactName
        }
      );

      const sendToUser = req.app?.locals?.sendToUser;
      if (typeof sendToUser === 'function') {
        sendToUser(String(req.user.id), {
          type: 'message_sent',
          message: message.toObject()
        });
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
  requirePlanFeature('broadcastMessaging'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      await runAttachmentUpload(req, res);

      const {
        to,
        conversationId,
        caption = '',
        replyToMessageId = '',
        whatsappContextMessageId = ''
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
          error: 'Attachment file is required'
        });
      }

      const conversation = await resolveConversationForOutboundSend({
        userId: req.user.id,
        companyId: req.companyId || null,
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
      const freeformValidation = outboundContact
        ? validateFreeformOutboundSend(outboundContact)
        : { ok: true, policy: null };
      if (!freeformValidation.ok) {
        return res.status(freeformValidation.statusCode || 403).json({
          success: false,
          error: freeformValidation.error,
          policy: freeformValidation.policy
        });
      }

      const storageUsername = resolveAttachmentUsername(req);
      const normalizedCaption = String(caption || '').trim();
      const messageCompanyId = conversation.companyId || req.companyId || null;
      const replyReference = await resolveReplyReferenceForOutboundSend({
        userId: req.user.id,
        companyId: messageCompanyId,
        replyToMessageId,
        whatsappContextMessageId
      });
      const resolvedReplyContextMessageId =
        String(whatsappContextMessageId || replyReference?.whatsappMessageId || '').trim();

      const attachment = await uploadInboxAttachment({
        file: req.file,
        username: storageUsername,
        direction: 'sent',
        userId: req.user.id,
        sender: req.user.id,
        recipient: to
      });

      const normalizedFileCategory = String(attachment?.fileCategory || '').trim().toLowerCase();
      const mediaType =
        normalizedFileCategory === 'image'
          ? 'image'
          : normalizedFileCategory === 'audio'
            ? 'audio'
            : 'document';
      let uploadedMetaMediaId = '';
      if (mediaType !== 'image') {
        const mediaUploadResult = await whatsappService.uploadMediaAsset(
          req.file,
          req.whatsappCredentials
        );
        if (!mediaUploadResult.success) {
          try {
            await deleteInboxAttachment({ attachment });
          } catch (_cleanupError) {
            // no-op: best-effort cleanup on downstream send failure
          }

          return res.status(400).json({
            success: false,
            error: mediaUploadResult.error || 'Failed to upload document to WhatsApp'
          });
        }
        uploadedMetaMediaId = String(mediaUploadResult?.data?.id || '').trim();
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
          mediaId: uploadedMetaMediaId
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
          error: sendResult.error || 'Failed to send media message'
        });
      }

      const whatsappMessageId =
        sendResult?.data?.messages?.[0]?.id || sendResult?.data?.messageId || null;
      const messageText = normalizedCaption || buildAttachmentLabel(mediaType);

      const message = await Message.create({
        userId: req.user.id,
        companyId: messageCompanyId,
        conversationId: conversation._id,
        sender: 'agent',
        text: messageText,
        mediaUrl: attachment.secureUrl,
        mediaType,
        mediaCaption: normalizedCaption || undefined,
        attachment,
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
          lastMessage: messageText,
          lastMessageMediaType: mediaType,
          lastMessageAttachmentName:
            mediaType === 'document' ? String(attachment?.originalFileName || '').trim() : '',
          lastMessageAttachmentPages:
            mediaType === 'document' ? Number(attachment?.pages || 0) || null : null,
          lastMessageFrom: 'agent'
        }
      );

      const sendToUser = req.app?.locals?.sendToUser;
      if (typeof sendToUser === 'function') {
        sendToUser(String(req.user.id), {
          type: 'message_sent',
          message: message.toObject()
        });
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
        error: error?.message || 'Failed to send attachment message'
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

    const parsedLimit = Number(rawLimit);
    const limit = Number.isFinite(parsedLimit) ? Math.max(1, Math.min(parsedLimit, 50)) : 30;

    const filters = {
      userId: req.user.id,
      $or: [{ 'attachment.publicId': { $exists: true, $ne: '' } }, { mediaUrl: { $exists: true, $ne: '' } }]
    };

    if (req.companyId) {
      filters.companyId = req.companyId;
    }

    const normalizedConversationId = String(conversationId || '').trim();
    if (normalizedConversationId) {
      filters.conversationId = normalizedConversationId;
    }

    const normalizedType = String(type || '').trim().toLowerCase();
    if (normalizedType === 'image' || normalizedType === 'document' || normalizedType === 'audio') {
      filters.mediaType = normalizedType;
    }

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
      .select(
        '_id conversationId sender senderName text mediaType mediaCaption mediaUrl status timestamp attachment'
      )
      .lean();

    const hasMore = messages.length > limit;
    const trimmed = hasMore ? messages.slice(0, limit) : messages;
    const nextCursor = hasMore
      ? new Date(trimmed[trimmed.length - 1]?.timestamp || Date.now()).toISOString()
      : null;

    return res.json({
      success: true,
      data: trimmed.map(mapAttachmentSummary),
      meta: {
        limit,
        hasMore,
        nextCursor
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

    const buffer = Buffer.from(await upstreamResponse.arrayBuffer());
    const contentType =
      String(upstreamResponse.headers.get('content-type') || '').trim() ||
      String(attachment?.mimeType || '').trim() ||
      'application/octet-stream';
    const contentLength = Number(upstreamResponse.headers.get('content-length') || buffer.length || 0);
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
    const { messageId } = req.params;
    const filters = { _id: messageId, userId: req.user.id };
    if (req.companyId) {
      filters.companyId = req.companyId;
    }

    const message = await Message.findOne(filters);
    if (!message) {
      return res.status(404).json({
        success: false,
        error: 'Attachment message not found'
      });
    }

    const attachment = message.attachment || {};
    if (!attachment?.publicId && !message.mediaUrl) {
      return res.status(404).json({
        success: false,
        error: 'No attachment found for this message'
      });
    }

    const storageUsername = resolveAttachmentUsername(req);
    const attachmentOwnerSegment = String(attachment?.username || storageUsername || '').trim();
    if (
      attachment?.publicId &&
      !isAttachmentPathOwned({ publicId: attachment.publicId, username: attachmentOwnerSegment })
    ) {
      return res.status(403).json({
        success: false,
        error: 'Attachment does not belong to current user storage path'
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

    const sendToUser = req.app?.locals?.sendToUser;
    if (typeof sendToUser === 'function') {
      sendToUser(String(req.user.id), {
        type: 'message_attachment_deleted',
        messageId: String(message._id),
        conversationId: String(message.conversationId || '')
      });
    }

    return res.json({
      success: true,
      message: 'Attachment deleted successfully'
    });
  } catch (error) {
    console.error('Delete attachment error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to delete attachment'
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
    
    // Delete messages from database
    const deleteResult = await Message.deleteMany({ _id: { $in: messageIds }, userId: req.user.id });
    
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

module.exports = router;
