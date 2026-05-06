const Broadcast = require('../models/Broadcast');
const Message = require('../models/Message');
const Conversation = require('../models/Conversation');
const Contact = require('../models/Contact');
const LeadActivity = require('../models/LeadActivity');
const Template = require('../models/Template');
const whatsappService = require('./whatsappService');
const { getWhatsAppCredentialsForUser } = require('./userWhatsAppCredentialsService');
const {
  buildPhoneCandidates
} = require('./whatsappOutreach/conversationResolver');
const {
  toCleanString,
  validateTemplateOutboundSend,
  validateFreeformOutboundSend,
  applyMarketingTemplateSent
} = require('./whatsappOutreach/policy');
const axios = require('axios');

const ADMIN_USAGE_ENDPOINT =
  process.env.ADMIN_USAGE_ENDPOINT || '/internal/usage/record';
const ADMIN_API_BASE_URLS = [
  process.env.ADMIN_API_BASE_URL,
  process.env.ADMIN_BACKEND_URL,
  'http://localhost:8000',
  'http://localhost:5000'
]
  .map((url) => (url || '').trim())
  .filter(Boolean)
  .filter((url, index, arr) => arr.indexOf(url) === index);
const ADMIN_INTERNAL_API_KEY = process.env.ADMIN_INTERNAL_API_KEY || '';

class BroadcastService {
  emitBroadcastRealtimeEvent(broadcaster, payload) {
    if (typeof broadcaster !== 'function') return;
    try {
      broadcaster(payload);
    } catch (error) {
      console.error('Broadcast realtime emit failed:', error?.message || error);
    }
  }

  async logBroadcastContactActivity({
    broadcast,
    contact,
    conversation,
    messageText = '',
    templateCategory = ''
  }) {
    try {
      if (!broadcast?._id || !contact?._id) return;

      const previewText = String(messageText || '').trim();
      await LeadActivity.create({
        userId: broadcast.createdById,
        companyId: broadcast.companyId || null,
        contactId: contact._id,
        conversationId: conversation?._id || null,
        type: 'broadcast_sent',
        meta: {
          broadcastId: broadcast._id,
          broadcastName: toCleanString(broadcast?.name || ''),
          messageType: toCleanString(broadcast?.messageType || 'text') || 'text',
          templateName: toCleanString(broadcast?.templateName || ''),
          templateCategory: toCleanString(templateCategory || ''),
          messagePreview: previewText ? previewText.slice(0, 280) : ''
        },
        createdBy: String(broadcast.createdById || '').trim() || null
      });
    } catch (error) {
      console.error('Broadcast CRM activity log failed:', error?.message || error);
    }
  }

  async resolveContactForRecipient({ userId, companyId, phone }) {
    const phoneCandidates = buildPhoneCandidates(phone);
    if (!userId || phoneCandidates.length === 0) return null;

    return Contact.findOne({
      userId,
      ...(companyId ? { companyId } : {}),
      phone: { $in: phoneCandidates }
    });
  }

  async resolveTemplateCategoryForBroadcast({ broadcast, credentials }) {
    const templateName = toCleanString(broadcast?.templateName || '');
    if (!templateName) return '';

    if (broadcast?.templateId) {
      const byId = await Template.findOne({
        _id: broadcast.templateId,
        userId: broadcast.createdById,
        companyId: broadcast.companyId
      }).select('category').lean();
      if (byId?.category) return toCleanString(byId.category).toLowerCase();
    }

    const byName = await Template.findOne({
      userId: broadcast.createdById,
      companyId: broadcast.companyId,
      name: templateName
    }).select('category').lean();
    if (byName?.category) return toCleanString(byName.category).toLowerCase();

    try {
      const listResult = await whatsappService.getTemplateList(credentials || null);
      if (!listResult?.success) return '';
      const templates = listResult?.data?.data || [];
      const requested = templateName.toLowerCase();
      const match = templates.find((tpl) => String(tpl?.name || '').trim().toLowerCase() === requested);
      return toCleanString(match?.category || '').toLowerCase();
    } catch (_error) {
      return '';
    }
  }

  async reportUsage(companyId, count = 1) {
    if (!companyId || !ADMIN_INTERNAL_API_KEY) return;
    for (const baseUrl of ADMIN_API_BASE_URLS) {
      try {
        await axios.post(
          `${baseUrl}${ADMIN_USAGE_ENDPOINT}`,
          {
            companyId,
            usageType: 'whatsapp_message',
            count
          },
          {
            headers: {
              'x-internal-api-key': ADMIN_INTERNAL_API_KEY
            },
            timeout: 10000
          }
        );
        return;
      } catch (error) {
        continue;
      }
    }
  }
  normalizePhoneNumber(phone) {
    return String(phone || '').replace(/\D/g, '');
  }

  normalizeRetryPolicy(policy = {}) {
    const maxAttempts = Math.max(1, Math.min(5, Number(policy?.maxAttempts || 2)));
    const backoffSeconds = Math.max(0, Math.min(300, Number(policy?.backoffSeconds || 4)));
    const retryableCodes = Array.isArray(policy?.retryableCodes)
      ? policy.retryableCodes.map((code) => String(code || '').trim()).filter(Boolean)
      : [];

    return {
      enabled: policy?.enabled !== false,
      maxAttempts,
      backoffSeconds,
      retryableCodes
    };
  }

  normalizeDeliveryPolicy(policy = {}) {
    const quietHours = policy?.quietHours && typeof policy.quietHours === 'object' ? policy.quietHours : {};
    const startHour = Math.max(0, Math.min(23, Number(quietHours?.startHour ?? 22)));
    const endHour = Math.max(0, Math.min(23, Number(quietHours?.endHour ?? 8)));
    const timezone = String(quietHours?.timezone || 'UTC').trim() || 'UTC';
    const action = String(quietHours?.action || 'defer').toLowerCase() === 'skip' ? 'skip' : 'defer';
    const batchSize = Math.max(1, Math.min(500, Number(policy?.batchSize || 50)));

    return {
      quietHours: {
        enabled: Boolean(quietHours?.enabled),
        startHour,
        endHour,
        timezone,
        action
      },
      batchSize: Number.isFinite(batchSize) ? Math.trunc(batchSize) : 50
    };
  }

  normalizeBroadcastBatchPolicy(policy = {}) {
    const batchSize = Math.max(1, Math.min(500, Number(policy?.batchSize || 50)));
    return {
      batchSize: Number.isFinite(batchSize) ? Math.trunc(batchSize) : 50
    };
  }

  normalizeCompliancePolicy(policy = {}) {
    const phones = Array.isArray(policy?.suppressionListPhones)
      ? policy.suppressionListPhones
      : [];

    return {
      respectOptOut: policy?.respectOptOut !== false,
      suppressionListPhones: Array.from(
        new Set(phones.map((phone) => this.normalizePhoneNumber(phone)).filter(Boolean))
      )
    };
  }

  normalizeBroadcastPolicies(broadcastData = {}) {
    const suppressionListPhonesFromRaw = String(broadcastData?.suppressionListRaw || '')
      .split(/[\n,;\s]+/)
      .map((item) => this.normalizePhoneNumber(item))
      .filter(Boolean);

    const fallbackDeliveryPolicy = {
      quietHours: {
        enabled: Boolean(broadcastData?.quietHoursEnabled),
        startHour: broadcastData?.quietHoursStartHour,
        endHour: broadcastData?.quietHoursEndHour,
        timezone: broadcastData?.quietHoursTimezone,
        action: broadcastData?.quietHoursAction
      }
    };

    const fallbackRetryPolicy = {
      enabled: broadcastData?.retryPolicyEnabled,
      maxAttempts: broadcastData?.retryMaxAttempts,
      backoffSeconds: broadcastData?.retryBackoffSeconds
    };

    const fallbackCompliancePolicy = {
      respectOptOut: broadcastData?.respectOptOut,
      suppressionListPhones: Array.from(new Set(suppressionListPhonesFromRaw))
    };

    return {
      ...broadcastData,
      retryPolicy: this.normalizeRetryPolicy({
        ...(fallbackRetryPolicy || {}),
        ...(broadcastData?.retryPolicy || {})
      }),
      deliveryPolicy: this.normalizeDeliveryPolicy({
        ...(fallbackDeliveryPolicy || {}),
        ...(broadcastData?.deliveryPolicy || {}),
        batchSize: broadcastData?.deliveryPolicy?.batchSize ?? broadcastData?.batchSize
      }),
      compliancePolicy: this.normalizeCompliancePolicy({
        ...(fallbackCompliancePolicy || {}),
        ...(broadcastData?.compliancePolicy || {})
      }),
      analytics: {
        suppressed: Number(broadcastData?.analytics?.suppressed || 0),
        deferred: Number(broadcastData?.analytics?.deferred || 0),
        retried: Number(broadcastData?.analytics?.retried || 0),
        failureCodeBreakdown:
          broadcastData?.analytics?.failureCodeBreakdown &&
          typeof broadcastData.analytics.failureCodeBreakdown === 'object'
            ? broadcastData.analytics.failureCodeBreakdown
            : {}
      }
    };
  }

  getHourInTimezone(date, timezone) {
    try {
      const formatter = new Intl.DateTimeFormat('en-US', {
        hour: 'numeric',
        hour12: false,
        timeZone: timezone || 'UTC'
      });
      const parsed = Number(formatter.format(date));
      if (Number.isFinite(parsed)) return parsed;
      return date.getUTCHours();
    } catch (_error) {
      return date.getUTCHours();
    }
  }

  isWithinQuietHours(quietHours = {}, now = new Date()) {
    if (!quietHours?.enabled) return false;
    const startHour = Number(quietHours?.startHour);
    const endHour = Number(quietHours?.endHour);
    if (!Number.isFinite(startHour) || !Number.isFinite(endHour)) return false;
    if (startHour === endHour) return true;

    const currentHour = this.getHourInTimezone(now, quietHours?.timezone || 'UTC');
    if (startHour < endHour) {
      return currentHour >= startHour && currentHour < endHour;
    }
    return currentHour >= startHour || currentHour < endHour;
  }

  computeNextAllowedTime(quietHours = {}, now = new Date()) {
    if (!quietHours?.enabled) return now;
    const timezone = quietHours?.timezone || 'UTC';
    const currentHour = this.getHourInTimezone(now, timezone);
    const endHour = Number(quietHours?.endHour);
    if (!Number.isFinite(endHour)) return now;
    const hourDiff = (endHour - currentHour + 24) % 24 || 24;
    const next = new Date(now.getTime() + hourDiff * 60 * 60 * 1000);
    next.setMinutes(0, 0, 0);
    return next;
  }

  async sendWithRetry(sendAction, retryPolicy = {}) {
    const normalizedPolicy = this.normalizeRetryPolicy(retryPolicy || {});
    const maxAttempts = normalizedPolicy.enabled ? normalizedPolicy.maxAttempts : 1;
    const backoffMs = normalizedPolicy.backoffSeconds * 1000;
    let attempts = 0;
    let lastMeta = { errorCode: '', errorMessage: '', retryable: false };
    let retriedCount = 0;

    while (attempts < maxAttempts) {
      attempts += 1;
      try {
        const result = await sendAction();
        if (result?.success) {
          return {
            success: true,
            result,
            attempts,
            retriedCount
          };
        }

        const meta = this.classifySendError(result?.error || result);
        const retryableByCode =
          meta.retryable ||
          (meta.errorCode && normalizedPolicy.retryableCodes.includes(String(meta.errorCode)));
        lastMeta = { ...meta, retryable: retryableByCode };
        const canRetry = normalizedPolicy.enabled && retryableByCode && attempts < maxAttempts;
        if (!canRetry) break;

        retriedCount += 1;
        if (backoffMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, backoffMs * attempts));
        }
      } catch (error) {
        const meta = this.classifySendError(error);
        const retryableByCode =
          meta.retryable ||
          (meta.errorCode && normalizedPolicy.retryableCodes.includes(String(meta.errorCode)));
        lastMeta = { ...meta, retryable: retryableByCode };
        const canRetry = normalizedPolicy.enabled && retryableByCode && attempts < maxAttempts;
        if (!canRetry) break;

        retriedCount += 1;
        if (backoffMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, backoffMs * attempts));
        }
      }
    }

    return {
      success: false,
      attempts,
      retriedCount,
      errorMeta: lastMeta
    };
  }

  getStatusScore(status) {
    const normalized = String(status || '').toLowerCase();
    if (normalized === 'read') return 4;
    if (normalized === 'delivered') return 3;
    if (normalized === 'sent') return 2;
    if (normalized === 'failed') return 1;
    return 0;
  }

  async buildRecipientStatusDetails(broadcast) {
    const recipients = Array.isArray(broadcast?.recipients) ? broadcast.recipients : [];
    const detailsByPhone = new Map();
    recipients.forEach((recipient) => {
      const rawPhone = recipient?.phone || recipient;
      const normalizedPhone = this.normalizePhoneNumber(rawPhone);
      if (!normalizedPhone) return;
        detailsByPhone.set(normalizedPhone, {
          phone: rawPhone,
          name: recipient?.name || '',
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: 'pending',
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: '',
          lastFailureReason: ''
        });
    });

    const startTime = new Date(broadcast.startedAt || broadcast.createdAt || Date.now());
    const completedAt = new Date(broadcast.completedAt || startTime);
    const endTime = new Date(completedAt);
    endTime.setDate(endTime.getDate() + 1);

    let outboundMessages = await Message.find({
      userId: broadcast.createdById,
      companyId: broadcast.companyId,
      sender: 'agent',
      broadcastId: broadcast._id
    })
      .select('conversationId status timestamp text')
      .sort({ timestamp: 1 })
      .lean();

    // Legacy fallback: old records may not have broadcastId tagged.
    // In that case, find agent messages in recipient conversations during campaign window.
    if (outboundMessages.length === 0 && detailsByPhone.size > 0) {
      const recipientPhoneVariants = Array.from(detailsByPhone.keys());
      const recipientPhoneRegex = recipientPhoneVariants.map((phone) => new RegExp(`${phone}$`));
      const legacyConversations = await Conversation.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        $or: [
          { contactPhone: { $in: recipientPhoneVariants } },
          ...recipientPhoneRegex.map((phoneRegex) => ({ contactPhone: phoneRegex }))
        ]
      })
        .select('_id contactPhone contactName')
        .lean();

      const legacyConversationIds = legacyConversations.map((item) => item._id);
      if (legacyConversationIds.length > 0) {
        outboundMessages = await Message.find({
          userId: broadcast.createdById,
          companyId: broadcast.companyId,
          sender: 'agent',
          conversationId: { $in: legacyConversationIds },
          timestamp: { $gte: startTime, $lte: endTime }
        })
          .select('conversationId status timestamp text')
          .sort({ timestamp: 1 })
          .lean();
      }
    }

    const conversationIds = Array.from(
      new Set(outboundMessages.map((message) => String(message.conversationId || '')).filter(Boolean))
    );

    const conversations = conversationIds.length
      ? await Conversation.find({ _id: { $in: conversationIds }, companyId: broadcast.companyId })
          .select('_id contactPhone contactName')
          .lean()
      : [];

    const conversationPhoneMap = new Map();
    conversations.forEach((conversation) => {
      conversationPhoneMap.set(String(conversation._id), {
        normalizedPhone: this.normalizePhoneNumber(conversation.contactPhone),
        rawPhone: conversation.contactPhone,
        contactName: conversation.contactName || ''
      });
    });

    outboundMessages.forEach((message) => {
      const conversationEntry = conversationPhoneMap.get(String(message.conversationId || ''));
      const normalizedPhone = conversationEntry?.normalizedPhone;
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: conversationEntry?.rawPhone || normalizedPhone,
          name: conversationEntry?.contactName || '',
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: 'pending',
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: '',
          lastFailureReason: ''
        });
      }

      const detail = detailsByPhone.get(normalizedPhone);
      const status = String(message.status || 'sent').toLowerCase();
      const messageTime = message.timestamp ? new Date(message.timestamp) : null;

      detail.sent = true;
      if (!detail.lastSentAt || (messageTime && messageTime > new Date(detail.lastSentAt))) {
        detail.lastSentAt = messageTime;
      }

      if (!detail.name && conversationEntry?.contactName) {
        detail.name = conversationEntry.contactName;
      }
      if (!detail.phone && conversationEntry?.rawPhone) {
        detail.phone = conversationEntry.rawPhone;
      }

      if (status === 'delivered' || status === 'read') detail.delivered = true;
      if (status === 'read') detail.read = true;
      if (status === 'failed') detail.failed = true;

      const currentScore = this.getStatusScore(detail.status);
      const nextScore = this.getStatusScore(status);
      if (nextScore >= currentScore) {
        detail.status = status;
      }

      if (!detail.lastStatusAt || (messageTime && messageTime > new Date(detail.lastStatusAt))) {
        detail.lastStatusAt = messageTime;
      }
    });

    const firstSentTimeByConversation = new Map();
    outboundMessages.forEach((message) => {
      const key = String(message.conversationId || '');
      if (!key || !message.timestamp) return;
      const current = firstSentTimeByConversation.get(key);
      const candidate = new Date(message.timestamp);
      if (!current || candidate < current) {
        firstSentTimeByConversation.set(key, candidate);
      }
    });

    const incomingMessages = conversationIds.length
      ? await Message.find({
          userId: broadcast.createdById,
          companyId: broadcast.companyId,
          sender: 'contact',
          conversationId: { $in: conversationIds },
          timestamp: { $gte: startTime }
        })
          .select('conversationId text timestamp')
          .sort({ timestamp: 1 })
          .lean()
      : [];

    incomingMessages.forEach((message) => {
      const conversationId = String(message.conversationId || '');
      const conversationEntry = conversationPhoneMap.get(conversationId);
      const normalizedPhone = conversationEntry?.normalizedPhone;
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: conversationEntry?.rawPhone || normalizedPhone,
          name: conversationEntry?.contactName || '',
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: 'pending',
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: ''
        });
      }

      const firstSentTime = firstSentTimeByConversation.get(conversationId);
      const replyTime = message.timestamp ? new Date(message.timestamp) : null;
      if (firstSentTime && replyTime && replyTime < firstSentTime) return;

      const detail = detailsByPhone.get(normalizedPhone);
      detail.replied = true;
      detail.replyCount += 1;
      detail.lastReplyAt = replyTime || detail.lastReplyAt;
      detail.lastReplyText = String(message.text || '').trim();
    });

    const deliveryResults = Array.isArray(broadcast?.deliveryResults) ? broadcast.deliveryResults : [];
    deliveryResults.forEach((result) => {
      const normalizedPhone = this.normalizePhoneNumber(result?.phone || '');
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: result?.phone || normalizedPhone,
          name: String(result?.name || '').trim(),
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: 'pending',
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: '',
          lastFailureReason: ''
        });
      }

      const detail = detailsByPhone.get(normalizedPhone);
      const status = String(result?.status || '').toLowerCase();
      const isFailed = result?.success === false && !result?.skipped;
      const isSkipped = Boolean(result?.skipped);

      if (isFailed) {
        detail.failed = true;
        detail.status = 'failed';
        detail.lastFailureReason = String(result?.error || result?.reason || result?.policy?.error || '').trim();
        detail.lastStatusAt = detail.lastStatusAt || new Date();
      } else if (isSkipped) {
        detail.status = 'pending';
        detail.lastFailureReason = String(result?.reason || '').trim();
      } else if (status === 'sent' || result?.success === true) {
        detail.sent = true;
        if (!detail.status || detail.status === 'pending') {
          detail.status = 'sent';
        }
      }
    });

    // Final fallback: populate missing names from contacts table.
    const missingNamePhones = Array.from(detailsByPhone.entries())
      .filter(([, detail]) => !String(detail?.name || '').trim())
      .map(([normalizedPhone]) => normalizedPhone)
      .filter(Boolean);

    if (missingNamePhones.length > 0) {
      const contactPhoneRegex = missingNamePhones.map((phone) => new RegExp(`${phone}$`));
      const contacts = await Contact.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        $or: [
          { phone: { $in: missingNamePhones } },
          ...contactPhoneRegex.map((phoneRegex) => ({ phone: phoneRegex }))
        ]
      })
        .select('phone name')
        .lean();

      const contactNameByPhone = new Map();
      contacts.forEach((contact) => {
        const normalized = this.normalizePhoneNumber(contact?.phone);
        const name = String(contact?.name || '').trim();
        if (normalized && name && !contactNameByPhone.has(normalized)) {
          contactNameByPhone.set(normalized, name);
        }
      });

      missingNamePhones.forEach((normalizedPhone) => {
        const detail = detailsByPhone.get(normalizedPhone);
        if (!detail) return;
        const fallbackName = contactNameByPhone.get(normalizedPhone);
        if (fallbackName) {
          detail.name = fallbackName;
        }
      });
    }

    return Array.from(detailsByPhone.values()).map((detail) => ({
      ...detail,
      lastSentAt: detail.lastSentAt || null,
      lastStatusAt: detail.lastStatusAt || null,
      lastReplyAt: detail.lastReplyAt || null
    }));
  }

  buildRetryCandidateList(broadcast, recipientDetails = []) {
    const recipients = Array.isArray(broadcast?.recipients) ? broadcast.recipients : [];
    const detailsByNormalizedPhone = new Map();

    recipientDetails.forEach((detail) => {
      const normalized = this.normalizePhoneNumber(detail?.phone || '');
      if (!normalized) return;
      detailsByNormalizedPhone.set(normalized, detail);
    });

    const seen = new Set();
    const retryCandidates = [];

    recipients.forEach((recipient) => {
      const normalized = this.normalizePhoneNumber(recipient?.phone || recipient);
      if (!normalized || seen.has(normalized)) return;
      seen.add(normalized);

      const detail = detailsByNormalizedPhone.get(normalized);
      const status = String(detail?.status || '').toLowerCase();
      const shouldRetry =
        status === 'failed' ||
        status === 'pending' ||
        (!detail?.sent && !detail?.delivered && !detail?.read);

      if (!shouldRetry) return;

      retryCandidates.push({
        phone: recipient?.phone || recipient,
        name: recipient?.name || '',
        variables: Array.isArray(recipient?.variables) ? recipient.variables : [],
        attributes:
          recipient?.attributes && typeof recipient.attributes === 'object'
            ? recipient.attributes
            : {},
        lastStatus: status || 'pending'
      });
    });

    return retryCandidates;
  }

  async resolveCredentialsForBroadcast(broadcast, credentials = null) {
    if (credentials) return credentials;

    const authHeader = String(broadcast?.authHeaderSnapshot || '').trim();
    if (authHeader.startsWith('Bearer ')) {
      try {
        const fetched = await getWhatsAppCredentialsForUser({
          authHeader,
          userId: String(broadcast?.createdById || '')
        });
        if (fetched) {
          return fetched;
        }
      } catch (error) {
        console.error(`Failed to fetch admin credentials for scheduled broadcast ${broadcast?._id}:`, error.message);
      }
    }

    const snapshot = broadcast?.credentialsSnapshot || null;
    const accessToken = String(snapshot?.accessToken || snapshot?.whatsappToken || '').trim();
    const businessAccountId = String(snapshot?.businessAccountId || snapshot?.whatsappBusiness || '').trim();
    const phoneNumberId = String(snapshot?.phoneNumberId || snapshot?.whatsappId || '').trim();

    if (accessToken && businessAccountId && phoneNumberId) {
      return {
        accessToken,
        businessAccountId,
        phoneNumberId,
        whatsappToken: accessToken,
        whatsappBusiness: businessAccountId,
        whatsappId: phoneNumberId,
        twilioId: snapshot?.twilioId || null
      };
    }

    const envAccessToken = String(process.env.WHATSAPP_ACCESS_TOKEN || '').trim();
    const envBusinessAccountId = String(process.env.WHATSAPP_BUSINESS_ACCOUNT_ID || '').trim();
    const envPhoneNumberId = String(process.env.WHATSAPP_PHONE_NUMBER_ID || '').trim();

    if (envAccessToken && envBusinessAccountId && envPhoneNumberId) {
      return {
        accessToken: envAccessToken,
        businessAccountId: envBusinessAccountId,
        phoneNumberId: envPhoneNumberId,
        whatsappToken: envAccessToken,
        whatsappBusiness: envBusinessAccountId,
        whatsappId: envPhoneNumberId,
        twilioId: null
      };
    }

    return null;
  }

  async resolveTemplatePreviewTextFromMeta(templateName, language, credentials) {
    try {
      const listResult = await whatsappService.getTemplateList(credentials || null);
      if (!listResult?.success) return null;

      const templates = listResult?.data?.data || [];
      const requested = String(templateName || '').trim().toLowerCase();
      const requestedLanguage = String(language || '').trim().toLowerCase();

      const matchedTemplate =
        templates.find((t) => String(t.name || '').trim().toLowerCase() === requested && String(t.language || '').trim().toLowerCase() === requestedLanguage) ||
        templates.find((t) => String(t.name || '').trim().toLowerCase() === requested);

      if (!matchedTemplate || !Array.isArray(matchedTemplate.components)) {
        return null;
      }

      const bodyComponent = matchedTemplate.components.find((component) => String(component.type || '').toUpperCase() === 'BODY');
      const bodyText =
        bodyComponent?.text ||
        bodyComponent?.body_text ||
        (Array.isArray(bodyComponent?.example?.body_text) ? bodyComponent.example.body_text[0] : '');
      if (!bodyText) return null;

      return String(bodyText);
    } catch (error) {
      console.error('Failed to resolve template preview text from Meta:', error.message);
      return null;
    }
  }

  // Process template variables - matching Python reference format
  processTemplateVariables(templateContent, variables, rowData = {}) {
    let processedContent = String(templateContent || '');
    if (!processedContent) return '';

    if (rowData && typeof rowData === 'object') {
      Object.keys(rowData).forEach((columnName) => {
        if (columnName === 'phone') return;
        const placeholder = new RegExp(`\\{${columnName}\\}`, 'g');
        processedContent = processedContent.replace(placeholder, rowData[columnName] || '');
      });
    }
    
    // Support both {{1}} and {var1} formats like Python reference
    variables.forEach((varValue, index) => {
      // Replace {{1}} format
      const placeholder1 = new RegExp(`\\{\\{${index + 1}\\}\\}`, 'g');
      processedContent = processedContent.replace(placeholder1, varValue);
      
      // Replace {var1} format  
      const placeholder2 = new RegExp(`\\{var${index + 1}\\}`, 'g');
      processedContent = processedContent.replace(placeholder2, varValue);
    });
    
    return processedContent;
  }

  classifySendError(errorPayload) {
    const raw = String(
      errorPayload?.error?.message ||
        errorPayload?.message ||
        errorPayload ||
        ''
    );
    const codeMatch = raw.match(/\b(13[0-9]{4}|63[0-9]{3})\b/);
    const errorCode = codeMatch ? codeMatch[1] : '';
    const retryableCodes = new Set(['131049', '131056', '131016', '131048', '63018']);
    const retryable =
      retryableCodes.has(errorCode) ||
      /timeout|temporar|temporarily|rate limit|try again/i.test(raw);

    return {
      errorCode,
      retryable,
      errorMessage: raw
    };
  }
  async createBroadcast(broadcastData, broadcaster = null) {
    try {
      broadcastData = this.normalizeBroadcastPolicies(broadcastData || {});
      // If scheduledAt is provided, set status to 'scheduled'
      if (broadcastData.scheduledAt) {
        broadcastData.status = 'scheduled';
        // Handle timezone properly - datetime-local comes without timezone info
        // Parse it as local time and preserve it exactly
        const scheduledDate = new Date(broadcastData.scheduledAt);
        
        // Check if the parsed date is valid
        if (isNaN(scheduledDate.getTime())) {
          return { success: false, error: 'Invalid scheduled time format' };
        }
        if (scheduledDate.getTime() <= Date.now()) {
          return { success: false, error: 'Scheduled time must be in the future' };
        }
        
        console.log('📅 Original scheduledAt input:', broadcastData.scheduledAt);
        console.log('📅 Parsed Date object:', scheduledDate);
        console.log('📅 Date string (local):', scheduledDate.toString());
        console.log('📅 Date string (UTC):', scheduledDate.toUTCString());
        console.log('🔧 Timezone offset minutes:', scheduledDate.getTimezoneOffset());
        
        // Store the date as-is without timezone manipulation
        // MongoDB will store it in UTC and the comparison will work correctly
        broadcastData.scheduledAt = scheduledDate;
      }
      
      const broadcast = await Broadcast.create(broadcastData);
      console.log('✅ Created broadcast with scheduledAt:', broadcast.scheduledAt);
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_created',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });
      return { success: true, data: broadcast };
    } catch (error) {
      console.error('❌ Error creating broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async sendBroadcast(broadcastId, broadcaster, credentials = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      const retryPolicy = this.normalizeRetryPolicy(broadcast?.retryPolicy || {});
      const deliveryPolicy = this.normalizeDeliveryPolicy(broadcast?.deliveryPolicy || {});
      const compliancePolicy = this.normalizeCompliancePolicy(broadcast?.compliancePolicy || {});

      if (this.isWithinQuietHours(deliveryPolicy?.quietHours || {})) {
        const action = String(deliveryPolicy?.quietHours?.action || 'defer').toLowerCase();
        if (action === 'defer') {
          const nextAllowedAt = this.computeNextAllowedTime(deliveryPolicy?.quietHours || {});
          broadcast.status = 'scheduled';
          broadcast.scheduledAt = nextAllowedAt;
          broadcast.analytics = {
            ...(broadcast.analytics || {}),
            deferred: Number(broadcast?.analytics?.deferred || 0) + 1
          };
          await broadcast.save();
          this.emitBroadcastRealtimeEvent(broadcaster, {
            type: 'broadcast_updated',
            action: 'deferred',
            broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
          });
          return {
            success: true,
            data: {
              deferred: true,
              reason: 'quiet_hours',
              nextAttemptAt: nextAllowedAt,
              broadcast
              }
          };
        }
        if (action === 'skip') {
          broadcast.status = 'completed';
          broadcast.completedAt = new Date();
          const skippedRecipients = Array.isArray(broadcast?.recipients)
            ? broadcast.recipients.length
            : 0;
          broadcast.analytics = {
            ...(broadcast.analytics || {}),
            skippedQuietHours:
              Number(broadcast?.analytics?.skippedQuietHours || 0) + skippedRecipients
          };
          await broadcast.save();
          this.emitBroadcastRealtimeEvent(broadcaster, {
            type: 'broadcast_updated',
            action: 'quiet_hours_skip',
            broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
          });
          return {
            success: true,
            data: {
              skipped: true,
              reason: 'quiet_hours_skip',
              skippedRecipients,
              broadcast
            }
          };
        }
      }

      const resolvedCredentials = await this.resolveCredentialsForBroadcast(broadcast, credentials);

      if (!resolvedCredentials) {
        return { success: false, error: 'WhatsApp credentials are not configured for this user' };
      }

      console.log('🔍 Broadcast data being processed:', {
        _id: broadcast._id,
        name: broadcast.name,
        messageType: broadcast.messageType,
        templateName: broadcast.templateName,
        message: broadcast.message,
        language: broadcast.language,
        recipientsCount: broadcast.recipients?.length || 0
      });

      broadcast.status = 'sending';
      broadcast.startedAt = new Date();
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_updated',
        action: 'sending',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });

      const results = [];
      let successful = 0;
      let failed = 0;
      let suppressed = 0;
      let retried = 0;
      const failureCodeBreakdown =
        broadcast?.analytics?.failureCodeBreakdown &&
        typeof broadcast.analytics.failureCodeBreakdown === 'object'
          ? { ...broadcast.analytics.failureCodeBreakdown }
          : {};
      let usageBatchCount = 0;
      const usageBatchSize = Number(process.env.BROADCAST_USAGE_BATCH || 50);
      const sendDelayMs = Math.max(0, Number(process.env.BROADCAST_SEND_DELAY_MS || 1000));
      const batchSize = Math.max(1, Math.min(500, Number(deliveryPolicy?.batchSize || 50)));
      const suppressionSet = new Set(
        (compliancePolicy?.suppressionListPhones || [])
          .map((phone) => this.normalizePhoneNumber(phone))
          .filter(Boolean)
      );
      let templatePreviewText = broadcast.templateContent || null;
      const explicitTemplateCategory = toCleanString(broadcast?.templateCategory || '').toLowerCase();
      let templateCategoryRaw = explicitTemplateCategory;
      if (!templateCategoryRaw && broadcast.messageType === 'template') {
        templateCategoryRaw = await this.resolveTemplateCategoryForBroadcast({
          broadcast,
          credentials: resolvedCredentials
        });
      }
      const templateCategory = toCleanString(templateCategoryRaw).toLowerCase() || 'utility';
      const templateHeaderMediaUrl = String(broadcast?.mediaUrl || '').trim();
      const templateHeaderMediaType = String(broadcast?.mediaType || '').trim().toLowerCase();
      if (broadcast.templateName && templateHeaderMediaType === 'image' && !templateHeaderMediaUrl) {
        return {
          success: false,
          error: 'This broadcast template requires an image header. Add an image URL before sending.'
        };
      }
      const templateComponents =
        broadcast.templateName && templateHeaderMediaType === 'image' && templateHeaderMediaUrl
          ? [
              {
                type: 'HEADER',
                parameters: [
                  {
                    type: 'image',
                    image: { link: templateHeaderMediaUrl }
                  }
                ]
              }
            ]
          : null;

      // If templateContent is not stored, try to resolve from Meta
      if (!templatePreviewText && broadcast.templateName) {
        templatePreviewText = await this.resolveTemplatePreviewTextFromMeta(
          broadcast.templateName,
          broadcast.language || 'en_US',
          resolvedCredentials
        );
      }

      const recipients = Array.isArray(broadcast.recipients) ? broadcast.recipients : [];
      for (let batchStart = 0; batchStart < recipients.length; batchStart += batchSize) {
        const batchRecipients = recipients.slice(batchStart, batchStart + batchSize);
        for (const recipient of batchRecipients) {
        try {
          let result;
          const phoneNumber = recipient.phone || recipient;
          const normalizedPhone = this.normalizePhoneNumber(phoneNumber);
          if (normalizedPhone && suppressionSet.has(normalizedPhone)) {
            suppressed += 1;
            results.push({
              phone: phoneNumber,
              success: false,
              skipped: true,
              reason: 'suppressed'
            });
            continue;
          }
          const rowData =
            recipient?.attributes && typeof recipient.attributes === 'object'
              ? recipient.attributes
              : {};
          const contact = await this.resolveContactForRecipient({
            userId: broadcast.createdById,
            companyId: broadcast.companyId,
            phone: phoneNumber
          });

          if (!contact) {
            failed++;
            broadcast.stats.failed++;
            results.push({
              phone: phoneNumber,
              success: false,
              error: 'Contact record not found for compliance checks.'
            });
            continue;
          }
          
          let messageTextForInbox = broadcast.message;
          if (broadcast.templateName) {
            const normalizedTemplateName = String(broadcast.templateName || '').trim();
            if (!normalizedTemplateName) {
              results.push({
                phone: phoneNumber,
                success: false,
                error: 'Template name is required'
              });
              failed++;
              broadcast.stats.failed++;
              continue;
            }

            const templateValidation = validateTemplateOutboundSend(contact, {
              templateCategory
            });
            if (!templateValidation.ok) {
              failed++;
              broadcast.stats.failed++;
              results.push({
                phone: phoneNumber,
                success: false,
                error: templateValidation.error,
                policy: templateValidation.policy
              });
              continue;
            }

            const sendTemplateOutcome = await this.sendWithRetry(
              () =>
                whatsappService.sendTemplateMessage(
                  phoneNumber,
                  normalizedTemplateName,
                  broadcast.language || 'en_US',
                  recipient.variables || broadcast.variables || [],
                  resolvedCredentials,
                  true,
                  templateComponents
                ),
              retryPolicy
            );
            retried += Number(sendTemplateOutcome?.retriedCount || 0);
            if (!sendTemplateOutcome.success) {
              const errorMeta = sendTemplateOutcome.errorMeta || {};
              failed++;
              broadcast.stats.failed++;
              if (errorMeta.errorCode) {
                failureCodeBreakdown[errorMeta.errorCode] =
                  Number(failureCodeBreakdown[errorMeta.errorCode] || 0) + 1;
              }
              results.push({
                phone: phoneNumber,
                success: false,
                error: errorMeta.errorMessage || 'Template send failed',
                errorCode: errorMeta.errorCode || '',
                retryable: Boolean(errorMeta.retryable),
                attempts: sendTemplateOutcome.attempts || 1
              });
              continue;
            }
            result = sendTemplateOutcome.result;
            
            console.log(`📤 Template send result for ${phoneNumber}:`, {
              success: result.success,
              templateName: normalizedTemplateName,
              language: broadcast.language || 'en_US',
              variables: recipient.variables || broadcast.variables || [],
              error: result.error
            });
            
            messageTextForInbox = templatePreviewText
              ? this.processTemplateVariables(
                  templatePreviewText,
                  recipient.variables || broadcast.variables || [],
                  rowData
                )
              : `Template: ${normalizedTemplateName}`;
          } else if (broadcast.message) {
            const freeformValidation = validateFreeformOutboundSend(contact);
            if (!freeformValidation.ok) {
              failed++;
              broadcast.stats.failed++;
              results.push({
                phone: phoneNumber,
                success: false,
                error: freeformValidation.error,
                policy: freeformValidation.policy
              });
              continue;
            }

            // Process custom message with variable replacement
            const processedMessage = this.processTemplateVariables(
              broadcast.message,
              recipient.variables || broadcast.variables || [],
              rowData
            );
            const sendTextOutcome = await this.sendWithRetry(
              () => whatsappService.sendTextMessage(phoneNumber, processedMessage, resolvedCredentials),
              retryPolicy
            );
            retried += Number(sendTextOutcome?.retriedCount || 0);
            if (!sendTextOutcome.success) {
              const errorMeta = sendTextOutcome.errorMeta || {};
              failed++;
              broadcast.stats.failed++;
              if (errorMeta.errorCode) {
                failureCodeBreakdown[errorMeta.errorCode] =
                  Number(failureCodeBreakdown[errorMeta.errorCode] || 0) + 1;
              }
              results.push({
                phone: phoneNumber,
                success: false,
                error: errorMeta.errorMessage || 'Message send failed',
                errorCode: errorMeta.errorCode || '',
                retryable: Boolean(errorMeta.retryable),
                attempts: sendTextOutcome.attempts || 1
              });
              continue;
            }
            result = sendTextOutcome.result;
            messageTextForInbox = processedMessage;
          } else {
            results.push({
              phone: phoneNumber,
              success: false,
              error: 'No message or template specified'
            });
            failed++;
            broadcast.stats.failed++;
            continue;
          }

          if (result.success) {
            successful++;
            broadcast.stats.sent++;
            
            // Create or update conversation + create message so Team Inbox shows it
            const { conversation, message } = await this.updateConversation(
              phoneNumber,
              messageTextForInbox,
              result.data,
              broadcast._id,
              broadcast.createdById,
              broadcast.companyId
            );

            if (broadcast.templateName && templateCategory === 'marketing') {
              applyMarketingTemplateSent(contact, { now: new Date() });
              await contact.save();
            }

            await this.logBroadcastContactActivity({
              broadcast,
              contact,
              conversation,
              messageText: messageTextForInbox,
              templateCategory
            });

            if (typeof broadcaster === 'function' && conversation && message) {
              broadcaster({
                type: 'message_sent',
                conversation: conversation.toObject(),
                message: message.toObject()
              });
            }
            usageBatchCount += 1;
            if (usageBatchCount >= usageBatchSize) {
              await this.reportUsage(broadcast.companyId, usageBatchCount);
              usageBatchCount = 0;
            }
          } else {
            failed++;
            broadcast.stats.failed++;
            const errorMeta = this.classifySendError(result?.error || result);
            console.error(`❌ Failed to send to ${phoneNumber}:`, errorMeta.errorMessage);
            results.push({
              phone: phoneNumber,
              success: false,
              error: errorMeta.errorMessage,
              errorCode: errorMeta.errorCode,
              retryable: errorMeta.retryable
            });
            continue;
          }

          results.push({
            phone: phoneNumber,
            success: true,
            response: result.data
          });

          // Rate limiting between recipients
          await new Promise(resolve => setTimeout(resolve, sendDelayMs));
        } catch (error) {
          failed++;
          broadcast.stats.failed++;
          const errorMeta = this.classifySendError(error);
          results.push({
            phone: recipient.phone || recipient,
            success: false,
            error: errorMeta.errorMessage || error.message,
            errorCode: errorMeta.errorCode,
            retryable: errorMeta.retryable
          });
        }
        }
      }

      broadcast.analytics = {
        ...(broadcast.analytics || {}),
        suppressed: Number(broadcast?.analytics?.suppressed || 0) + suppressed,
        deferred: Number(broadcast?.analytics?.deferred || 0),
        retried: Number(broadcast?.analytics?.retried || 0) + retried,
        failureCodeBreakdown
      };
      broadcast.deliveryResults = results;
      broadcast.status = 'completed';
      broadcast.completedAt = new Date();
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_updated',
        action: 'completed',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });

      if (usageBatchCount > 0) {
        await this.reportUsage(broadcast.companyId, usageBatchCount);
      }

      // Note: Don't sync stats immediately after completion
      // Stats will be updated in real-time via message status updates
      // This prevents premature 100% read rates for first broadcasts

      return {
        success: true,
        data: {
          engine: 'broadcast_direct_meta_v2',
          broadcast,
          results,
          stats: {
            total: broadcast.recipients.length,
            successful,
            failed,
            suppressed,
            retried
          }
        }
      };
    } catch (error) {
      try {
        await Broadcast.findByIdAndUpdate(
          broadcastId,
          { $set: { status: 'failed', completedAt: new Date() } },
          { new: false }
        );
      } catch (_updateError) {
        // best effort only
      }
      return { success: false, error: error.message };
    }
  }

  async updateConversation(phone, message, whatsappResponse, broadcastId, userId, companyId) {
    try {
      let contact = await Contact.findOne({ userId, companyId, phone });
      if (!contact) {
        contact = await Contact.create({
          userId,
          companyId,
          phone,
          name: '',
          sourceType: 'incoming_message'
        });
      } else if (broadcastId && contact.sourceType !== 'incoming_message') {
        // If this contact is being used in broadcast message flow, mark source as message-origin.
        contact.sourceType = 'incoming_message';
        await contact.save();
      }

      let conversation = await Conversation.findOne({
        userId,
        companyId,
        contactPhone: phone,
        status: { $in: ['active', 'pending'] }
      });
      if (!conversation) {
        conversation = await Conversation.create({
          userId,
          companyId,
          contactId: contact._id,
          contactPhone: phone,
          contactName: contact.name,
          lastMessage: message,
          lastMessageTime: new Date(),
          lastMessageMediaType: '',
          lastMessageAttachmentName: '',
          lastMessageAttachmentPages: null,
          lastMessageFrom: 'agent',
          lastMessageWhatsappMessageId: whatsappMessageId || ''
        });
      } else {
        conversation.lastMessage = message;
        conversation.lastMessageTime = new Date();
        conversation.lastMessageMediaType = '';
        conversation.lastMessageAttachmentName = '';
        conversation.lastMessageAttachmentPages = null;
        conversation.lastMessageFrom = 'agent';
        conversation.lastMessageWhatsappMessageId = whatsappMessageId || '';
        await conversation.save();
      }

      const whatsappMessageId = whatsappResponse?.messages?.[0]?.id;
      const savedMessage = await Message.create({
        userId,
        companyId,
        conversationId: conversation._id,
        sender: 'agent',
        text: message,
        whatsappMessageId,
        status: 'sent',
        ...(broadcastId ? { broadcastId } : {})
      });

      return { conversation, message: savedMessage };
    } catch (error) {
      console.error('Error updating conversation:', error);
      return { conversation: null, message: null };
    }
  }

  async getBroadcasts(filters = {}) {
    try {
      // Keep list endpoint lightweight for fast overview updates.
      const broadcasts = await Broadcast.find(filters)
        .select('name status scheduledAt startedAt completedAt createdAt updatedAt recipientCount stats messageType templateName language')
        .sort({ createdAt: -1 })
        .lean();
      return { success: true, data: broadcasts };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getBroadcastById(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }
      const recipientDetails = await this.buildRecipientStatusDetails(broadcast);
      const data = broadcast.toObject ? broadcast.toObject() : broadcast;
      const statusBreakdown = recipientDetails.reduce(
        (accumulator, item) => {
          const status = String(item?.status || 'pending').toLowerCase();
          if (!accumulator[status]) {
            accumulator[status] = 0;
          }
          accumulator[status] += 1;
          return accumulator;
        },
        { pending: 0, sent: 0, delivered: 0, read: 0, failed: 0 }
      );
      const retryCandidates = this.buildRetryCandidateList(broadcast, recipientDetails);

      data.recipientDetails = recipientDetails;
      data.statusBreakdown = statusBreakdown;
      data.retrySummary = {
        retryCandidates: retryCandidates.length,
        canRetry: retryCandidates.length > 0,
        retryPolicy: this.normalizeRetryPolicy(data?.retryPolicy || {}),
        deliveryPolicy: this.normalizeDeliveryPolicy(data?.deliveryPolicy || {}),
        compliancePolicy: this.normalizeCompliancePolicy(data?.compliancePolicy || {}),
        analytics: {
          suppressed: Number(data?.analytics?.suppressed || 0),
          deferred: Number(data?.analytics?.deferred || 0),
          retried: Number(data?.analytics?.retried || 0),
          failureCodeBreakdown:
            data?.analytics?.failureCodeBreakdown &&
            typeof data.analytics.failureCodeBreakdown === 'object'
              ? data.analytics.failureCodeBreakdown
              : {}
        }
      };
      return { success: true, data };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getReliabilitySummary(filters = {}) {
    try {
      const query = { ...(filters || {}) };
      const createdAt = {};

      if (filters?.createdFrom) {
        const from = new Date(filters.createdFrom);
        if (!Number.isNaN(from.getTime())) {
          createdAt.$gte = from;
        }
      }
      if (filters?.createdTo) {
        const to = new Date(filters.createdTo);
        if (!Number.isNaN(to.getTime())) {
          createdAt.$lte = to;
        }
      }
      if (Object.keys(createdAt).length > 0) {
        query.createdAt = createdAt;
      }

      delete query.createdFrom;
      delete query.createdTo;

      const broadcasts = await Broadcast.find(query).lean();

      const summary = {
        campaigns: 0,
        recipientCount: 0,
        suppressed: 0,
        deferred: 0,
        retried: 0,
        skippedQuietHours: 0,
        failureCodeBreakdown: {}
      };

      (Array.isArray(broadcasts) ? broadcasts : []).forEach((broadcast) => {
        const analytics = broadcast?.analytics || {};
        const recipientCount = Number(
          broadcast?.recipientCount || (Array.isArray(broadcast?.recipients) ? broadcast.recipients.length : 0)
        );
        summary.campaigns += 1;
        summary.recipientCount += Number.isFinite(recipientCount) ? Math.max(0, recipientCount) : 0;
        summary.suppressed += Number(analytics?.suppressed || 0) || 0;
        summary.deferred += Number(analytics?.deferred || 0) || 0;
        summary.retried += Number(analytics?.retried || 0) || 0;
        summary.skippedQuietHours += Number(analytics?.skippedQuietHours || 0) || 0;

        const breakdown = analytics?.failureCodeBreakdown;
        if (breakdown && typeof breakdown === 'object') {
          Object.entries(breakdown).forEach(([code, count]) => {
            const key = String(code || '').trim();
            if (!key) return;
            summary.failureCodeBreakdown[key] =
              Number(summary.failureCodeBreakdown[key] || 0) + (Number(count || 0) || 0);
          });
        }
      });

      const topFailureCode = Object.entries(summary.failureCodeBreakdown)
        .map(([code, count]) => ({ code, count: Number(count || 0) }))
        .sort((a, b) => b.count - a.count)[0] || null;

      return {
        success: true,
        data: {
          ...summary,
          topFailureCode
        }
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async retryFailedRecipients(broadcastId, broadcaster, credentials = null) {
    try {
      const sourceBroadcast = await Broadcast.findById(broadcastId);
      if (!sourceBroadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (!['completed', 'failed'].includes(String(sourceBroadcast.status || '').toLowerCase())) {
        return {
          success: false,
          error: 'Retry is allowed only after a completed or failed broadcast'
        };
      }

      const recipientDetails = await this.buildRecipientStatusDetails(sourceBroadcast);
      const retryCandidates = this.buildRetryCandidateList(sourceBroadcast, recipientDetails);
      if (!retryCandidates.length) {
        return { success: false, error: 'No failed recipients available for retry' };
      }

      const priorRetryCount = await Broadcast.countDocuments({
        $or: [{ _id: sourceBroadcast._id }, { retryOfBroadcastId: sourceBroadcast._id }],
        createdById: sourceBroadcast.createdById,
        companyId: sourceBroadcast.companyId
      });
      const retryAttempt = Math.max(1, priorRetryCount);

      const retryBroadcast = await Broadcast.create({
        name: `${sourceBroadcast.name} - Retry ${retryAttempt}`,
        companyId: sourceBroadcast.companyId || null,
        messageType: sourceBroadcast.messageType,
        message: sourceBroadcast.message || '',
        templateName: sourceBroadcast.templateName || '',
        templateCategory: sourceBroadcast.templateCategory || '',
        templateContent: sourceBroadcast.templateContent || '',
        language: sourceBroadcast.language || 'en_US',
        recipients: retryCandidates.map((candidate) => ({
          phone: candidate.phone,
          name: candidate.name,
          variables: candidate.variables,
          attributes: candidate.attributes
        })),
        retryOfBroadcastId: sourceBroadcast._id,
        retryAttempt,
        deliveryPolicy: this.normalizeDeliveryPolicy(sourceBroadcast.deliveryPolicy || {}),
        retryPolicy: this.normalizeRetryPolicy(sourceBroadcast.retryPolicy || {}),
        compliancePolicy: this.normalizeCompliancePolicy(sourceBroadcast.compliancePolicy || {}),
        analytics: {
          suppressed: 0,
          deferred: 0,
          retried: 0,
          failureCodeBreakdown: {}
        },
        createdBy: sourceBroadcast.createdBy,
        createdById: sourceBroadcast.createdById,
        createdByEmail: sourceBroadcast.createdByEmail,
        authHeaderSnapshot: sourceBroadcast.authHeaderSnapshot,
        credentialsSnapshot: sourceBroadcast.credentialsSnapshot
      });

      const result = await this.sendBroadcast(retryBroadcast._id, broadcaster, credentials);
      if (!result?.success) {
        return {
          success: false,
          error: result?.error || 'Retry send failed'
        };
      }

      return {
        success: true,
        data: {
          sourceBroadcastId: sourceBroadcast._id,
          retryBroadcastId: retryBroadcast._id,
          retriedRecipients: retryCandidates.length,
          retryAttempt,
          result: result.data
        }
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  // Sync broadcast stats from actual messages in team inbox
  async syncBroadcastStats(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      // Find all messages sent from this broadcast
      const startTime = new Date(broadcast.startedAt || broadcast.createdAt);
      const endTime = new Date(broadcast.completedAt || Date.now());

      const broadcastIdQuery = {
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        sender: 'agent',
        broadcastId: broadcast._id,
        timestamp: { $gte: startTime, $lte: endTime }
      };

      let messages = await Message.find(broadcastIdQuery);

      // Backward compatibility for older records without broadcastId
      if (messages.length === 0) {
        const recipientPhones = (broadcast.recipients || [])
          .map(r => r.phone || r)
          .filter(Boolean);

        const conversations = recipientPhones.length
          ? await Conversation.find({ userId: broadcast.createdById, contactPhone: { $in: recipientPhones } })
          : [];

        const conversationIds = conversations.map(c => c._id);

        if (conversationIds.length > 0) {
          let convoQuery = {
            userId: broadcast.createdById,
            sender: 'agent',
            conversationId: { $in: conversationIds },
            timestamp: { $gte: startTime, $lte: endTime }
          };

          messages = await Message.find(convoQuery);

          if (messages.length > 0) {
            await Message.updateMany(
              { _id: { $in: messages.map(m => m._id) }, broadcastId: { $exists: false } },
              { $set: { broadcastId: broadcast._id } }
            );
          }
        }

        // Intentionally avoid broad text-based legacy fallback queries:
        // they can cross-match unrelated broadcasts and cause stat drops/fluctuations.
      }

      console.log(`?? Found ${messages.length} messages for broadcast "${broadcast.name}"`);

      // Count statuses with proper validation
      const stats = {
        sent: messages.length,
        // In WhatsApp, "read" implies message was delivered.
        delivered: messages.filter(msg => msg.status === 'delivered' || msg.status === 'read').length,
        read: messages.filter(msg => msg.status === 'read').length,
        failed: messages.filter(msg => msg.status === 'failed').length,
        replied: 0 // Will be calculated below
      };
      const deliveryFailedCount = Array.isArray(broadcast?.deliveryResults)
        ? broadcast.deliveryResults.filter((item) => item?.success === false && !item?.skipped).length
        : 0;
      stats.failed = Math.max(stats.failed, deliveryFailedCount, Number(broadcast?.stats?.failed || 0));

      // Debug: Log message statuses to identify issues
      console.log('🔍 DEBUG: Message statuses found:', {
        totalMessages: messages.length,
        statusBreakdown: {
          sent: messages.filter(msg => msg.status === 'sent').length,
          delivered: messages.filter(msg => msg.status === 'delivered' || msg.status === 'read').length,
          read: messages.filter(msg => msg.status === 'read').length,
          failed: messages.filter(msg => msg.status === 'failed').length,
          other: messages.filter(msg => !['sent', 'delivered', 'read', 'failed'].includes(msg.status)).length
        },
        messageDetails: messages.map(m => ({
          id: m._id,
          whatsappId: m.whatsappMessageId,
          status: m.status,
          timestamp: m.timestamp
        }))
      });


      // Count unique contacts who replied to this broadcast
      const conversationIds = Array.from(new Set(messages.map(m => String(m.conversationId))));
      const replyStartTime = messages.length > 0
        ? new Date(Math.min(...messages.map(m => new Date(m.timestamp).getTime())))
        : startTime;

      const replyMessages = await Message.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        conversationId: { $in: conversationIds },
        sender: 'contact',
        timestamp: { $gte: replyStartTime }
      });

      // Count unique conversations that have at least one reply
      const uniqueRepliedConversations = new Set(replyMessages.map(msg => msg.conversationId.toString()));
      stats.replied = uniqueRepliedConversations.size;

      console.log('📊 Message status breakdown:', {
        total: messages.length,
        statuses: messages.map(m => ({ id: m._id, status: m.status })),
        calculatedStats: stats
      });

      // Only update broadcast stats if they're different
      const currentStats = broadcast.stats || {};
      const statsChanged = 
        currentStats.sent !== stats.sent ||
        currentStats.delivered !== stats.delivered ||
        currentStats.read !== stats.read ||
        currentStats.failed !== stats.failed ||
        currentStats.replied !== stats.replied;
      let updatedBroadcast = null;

      if (statsChanged) {
        // Update only the stats field without touching other fields
        await Broadcast.updateOne(
          { _id: broadcastId },
          { $set: { stats: stats, updatedAt: new Date() } }
        );
        
        // Get updated broadcast with virtual fields
        updatedBroadcast = await Broadcast.findById(broadcastId);
        const repliedPercentage = updatedBroadcast.repliedPercentage;
        const repliedPercentageOfTotal = updatedBroadcast.repliedPercentageOfTotal;
        const readPercentage = updatedBroadcast.readPercentage;
        const readPercentageOfTotal = updatedBroadcast.readPercentageOfTotal;
        const deliveryRate = updatedBroadcast.deliveryRate;
        
        console.log(`📊 Updated stats for broadcast ${broadcast.name}:`, stats);
        console.log(`📊 Delivery rate: ${deliveryRate}%, Read rate: ${readPercentage}% of sent, ${readPercentageOfTotal}% of total recipients`);
        console.log(`📊 Replied percentage: ${repliedPercentage}% of sent, ${repliedPercentageOfTotal}% of total recipients`);
      } else {
        console.log(`📊 No stat changes for broadcast ${broadcast.name}`);
      }

      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_stats_updated',
        broadcastId: String(broadcastId),
        stats,
        broadcast: updatedBroadcast ? (updatedBroadcast.toObject ? updatedBroadcast.toObject() : updatedBroadcast) : null
      });

      return { success: true, data: { broadcast, stats, messagesFound: messages.length } };
    } catch (error) {
      console.error('Error syncing broadcast stats:', error);
      return { success: false, error: error.message };
    }
  }

  // Check for scheduled broadcasts that need to be sent
  async checkScheduledBroadcasts() {
    try {
      const now = new Date();
      let claimedCount = 0;

      while (true) {
        const claimed = await Broadcast.findOneAndUpdate(
          {
            status: 'scheduled',
            scheduledAt: { $lte: now }
          },
          {
            $set: {
              status: 'sending',
              startedAt: new Date(),
              updatedAt: new Date()
            }
          },
          {
            new: true,
            sort: { scheduledAt: 1 }
          }
        ).maxTimeMS(5000);

        if (!claimed) {
          break;
        }

        claimedCount += 1;
        try {
          const result = await this.sendBroadcast(claimed._id);
          if (!result?.success) {
            await Broadcast.findByIdAndUpdate(
              claimed._id,
              { $set: { status: 'failed', completedAt: new Date() } },
              { new: false }
            );
            console.error(
              `Scheduled broadcast failed: ${claimed.name}: ${result?.error || 'Unknown error'}`
            );
          }
        } catch (error) {
          await Broadcast.findByIdAndUpdate(
            claimed._id,
            { $set: { status: 'failed', completedAt: new Date() } },
            { new: false }
          );
          console.error(`Failed to send scheduled broadcast ${claimed.name}:`, error);
        }
      }

      if (claimedCount === 0) {
        console.log('No scheduled broadcasts ready to send');
      }
    } catch (error) {
      console.error('Error checking scheduled broadcasts:', error);
    }
  }

  async pauseBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (broadcast.status !== 'scheduled') {
        return { success: false, error: 'Only scheduled broadcasts can be paused' };
      }

      broadcast.status = 'paused';
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_updated',
        action: 'paused',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });

      return { success: true, data: broadcast };
    } catch (error) {
      console.error('Error pausing broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async resumeBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (broadcast.status !== 'paused') {
        return { success: false, error: 'Only paused broadcasts can be resumed' };
      }

      const now = new Date();
      if (broadcast.scheduledAt && broadcast.scheduledAt < now) {
        broadcast.scheduledAt = now;
      }

      broadcast.status = 'scheduled';
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_updated',
        action: 'resumed',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });

      return { success: true, data: broadcast };
    } catch (error) {
      console.error('Error resuming broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async cancelScheduledBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      if (!['scheduled', 'paused'].includes(broadcast.status)) {
        return { success: false, error: 'Only scheduled or paused broadcasts can be cancelled' };
      }

      broadcast.status = 'cancelled';
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_updated',
        action: 'cancelled',
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });

      return { success: true, data: broadcast };
    } catch (error) {
      console.error('Error cancelling broadcast:', error);
      return { success: false, error: error.message };
    }
  }

  async deleteBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: 'Broadcast not found' };
      }

      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: 'broadcast_deleted',
        broadcastId: String(broadcastId),
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast
      });

      // Delete the broadcast from database
      await Broadcast.findByIdAndDelete(broadcastId);
      
      return { success: true, message: 'Broadcast deleted successfully' };
    } catch (error) {
      console.error('Error deleting broadcast:', error);
      return { success: false, error: error.message };
    }
  }
}

module.exports = new BroadcastService();
