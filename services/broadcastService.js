const Broadcast = require("../models/Broadcast");
const Message = require("../models/Message");
const Conversation = require("../models/Conversation");
const Contact = require("../models/Contact");
const BroadcastDispatch = require("../models/BroadcastDispatch");
const LeadActivity = require("../models/LeadActivity");
const Template = require("../models/Template");
const whatsappService = require("./whatsappService");
const {
  syncConversationSummaryFromConversation,
} = require("./conversationSummaryService");
const { enqueueBroadcastSend } = require("../queues/broadcastQueue");
const { enqueueBroadcastInboxWrite } = require("../queues/broadcastInboxQueue");
const {
  getWhatsAppCredentialsForUser,
} = require("./userWhatsAppCredentialsService");
const {
  buildPhoneCandidates,
  buildPhoneLookupFilters,
} = require("./whatsappOutreach/conversationResolver");
const { buildContactSearchPlan } = require("../utils/contactSearchPlan");
const {
  buildConversationPhoneLookupFilter,
} = require("../utils/conversationIdentity");
const {
  CACHE_TTL_SECONDS,
  getOrSetCachedJson,
} = require("../utils/teamInboxCache");
const {
  toCleanString,
  validateTemplateOutboundSend,
  validateFreeformOutboundSend,
  applyMarketingTemplateSent,
} = require("./whatsappOutreach/policy");
const axios = require("axios");
const { createRedisConnection, isRedisDisabled } = require("../config/redis");
const mongoose = require("mongoose");

const broadcastRateLimiterRedis = createRedisConnection({
  maxRetriesPerRequest: 1,
  enableOfflineQueue: true,
});

const toQueryObjectId = (value) => {
  const normalized = String(value || "").trim();
  if (!normalized) return null;
  if (mongoose.Types.ObjectId.isValid(normalized)) {
    return new mongoose.Types.ObjectId(normalized);
  }
  return normalized;
};

const BROADCAST_RATE_LIMIT_MAX = Math.max(
  1,
  Number(process.env.BROADCAST_RATE_LIMIT_MAX || 25),
);
const BROADCAST_RATE_LIMIT_WINDOW_MS = Math.max(
  1000,
  Number(process.env.BROADCAST_RATE_LIMIT_WINDOW_MS || 60000),
);

const ADMIN_USAGE_ENDPOINT =
  process.env.ADMIN_USAGE_ENDPOINT || "/internal/usage/record";
const ADMIN_API_BASE_URLS = [
  process.env.ADMIN_API_BASE_URL,
  process.env.ADMIN_BACKEND_URL,
  "http://localhost:8000",
  "http://localhost:5000",
]
  .map((url) => (url || "").trim())
  .filter(Boolean)
  .filter((url, index, arr) => arr.indexOf(url) === index);
const ADMIN_INTERNAL_API_KEY = process.env.ADMIN_INTERNAL_API_KEY || "";

class BroadcastService {
  encodePaginationCursor(value = {}) {
    try {
      return Buffer.from(JSON.stringify(value || {}), "utf8").toString(
        "base64",
      );
    } catch (_error) {
      return "";
    }
  }

  decodePaginationCursor(cursor = "") {
    const normalized = String(cursor || "").trim();
    if (!normalized) return null;
    try {
      const json = Buffer.from(normalized, "base64").toString("utf8");
      const parsed = JSON.parse(json);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch (_error) {
      return null;
    }
  }

  emitBroadcastRealtimeEvent(broadcaster, payload) {
    if (typeof broadcaster !== "function") return;
    try {
      broadcaster(payload);
    } catch (error) {
      console.error("Broadcast realtime emit failed:", error?.message || error);
    }
  }

  async logBroadcastContactActivity({
    broadcast,
    contact,
    conversation,
    messageText = "",
    templateCategory = "",
  }) {
    try {
      if (!broadcast?._id || !contact?._id) return;

      const previewText = String(messageText || "").trim();
      await LeadActivity.create({
        userId: broadcast.createdById,
        companyId: broadcast.companyId || null,
        contactId: contact._id,
        conversationId: conversation?._id || null,
        type: "broadcast_sent",
        meta: {
          broadcastId: broadcast._id,
          broadcastName: toCleanString(broadcast?.name || ""),
          messageType:
            toCleanString(broadcast?.messageType || "text") || "text",
          templateName: toCleanString(broadcast?.templateName || ""),
          templateCategory: toCleanString(templateCategory || ""),
          messagePreview: previewText ? previewText.slice(0, 280) : "",
        },
        createdBy: String(broadcast.createdById || "").trim() || null,
      });
    } catch (error) {
      console.error(
        "Broadcast CRM activity log failed:",
        error?.message || error,
      );
    }
  }

  async resolveContactForRecipient({ userId, companyId, phone }) {
    if (!userId) return null;

    const phoneFilter = buildPhoneLookupFilters(phone);
    if (!phoneFilter) return null;

    return Contact.findOne({
      userId,
      ...(companyId ? { companyId } : {}),
      ...phoneFilter,
    });
  }

  async resolveBroadcastAudienceRecipients({
    broadcast,
    userId = null,
    companyId = null,
    recipientSubset = null,
  } = {}) {
    const campaignAudienceSource =
      broadcast?.audienceSource && typeof broadcast.audienceSource === "object"
        ? broadcast.audienceSource
        : {};
    const campaignAudienceSnapshot =
      broadcast?.audienceSnapshot &&
      typeof broadcast.audienceSnapshot === "object"
        ? broadcast.audienceSnapshot
        : {};
    const campaignBroadcastId = String(
      campaignAudienceSource?.campaignBroadcastId ||
        campaignAudienceSnapshot?.campaignBroadcastId ||
        campaignAudienceSnapshot?.sourceBroadcastId ||
        "",
    ).trim();

    if (
      String(campaignAudienceSource?.type || "")
        .trim()
        .toLowerCase() === "campaign" &&
      campaignBroadcastId
    ) {
      const baseBroadcast = await Broadcast.findOne({
        _id: campaignBroadcastId,
        ...(companyId || broadcast?.companyId
          ? { companyId: companyId || broadcast?.companyId }
          : {}),
      })
        .select("_id name companyId createdById recipientCount")
        .lean();

      if (!baseBroadcast) {
        return [];
      }

      const excludedPhones = new Set(
        [
          ...(Array.isArray(campaignAudienceSnapshot?.excludedPhones)
            ? campaignAudienceSnapshot.excludedPhones
            : []),
          ...(Array.isArray(campaignAudienceSnapshot?.excludedRecipientPhones)
            ? campaignAudienceSnapshot.excludedRecipientPhones
            : []),
        ]
          .map((phone) => this.normalizePhoneNumber(phone))
          .filter(Boolean),
      );

      const dispatchRecipients = await BroadcastDispatch.find({
        broadcastId: baseBroadcast._id,
        ...(companyId || baseBroadcast?.companyId
          ? { companyId: companyId || baseBroadcast.companyId }
          : {}),
      })
        .select(
          "recipientPhone recipientIndex messageText messageKind status sentAt failedAt whatsappMessageId chunkId chunkIndex",
        )
        .sort({ recipientIndex: 1, _id: 1 })
        .lean();

      const recipientMap = new Map();
      for (const dispatch of dispatchRecipients) {
        const phone = this.normalizePhoneNumber(dispatch?.recipientPhone || "");
        if (!phone || excludedPhones.has(phone)) {
          continue;
        }
        if (!recipientMap.has(phone)) {
          recipientMap.set(phone, {
            phone: String(dispatch?.recipientPhone || "").trim(),
            name: "",
            contactId: "",
            sourceType: "campaign",
            variables: [],
            attributes: {
              dispatchId: String(dispatch?._id || ""),
              broadcastId: String(dispatch?.broadcastId || ""),
              recipientIndex: Number(dispatch?.recipientIndex || 0),
              status: String(dispatch?.status || ""),
              sentAt: dispatch?.sentAt || null,
            },
          });
        }
      }

      const additionalRecipients = Array.isArray(broadcast?.recipients)
        ? broadcast.recipients
        : [];
      for (const recipient of additionalRecipients) {
        const phone = this.normalizePhoneNumber(recipient?.phone || "");
        if (!phone || excludedPhones.has(phone)) {
          continue;
        }
        if (!recipientMap.has(phone)) {
          recipientMap.set(phone, {
            phone: String(recipient?.phone || "").trim(),
            name: String(recipient?.name || "").trim(),
            contactId: String(
              recipient?.contactId || recipient?.attributes?._id || "",
            ).trim(),
            sourceType:
              String(recipient?.sourceType || "campaign").trim() || "campaign",
            variables: Array.isArray(recipient?.variables)
              ? recipient.variables
              : [],
            attributes: recipient?.attributes || {},
          });
        }
      }

      return Array.from(recipientMap.values()).filter((recipient) =>
        Boolean(this.normalizePhoneNumber(recipient?.phone || "")),
      );
    }

    if (Array.isArray(recipientSubset) && recipientSubset.length > 0) {
      return recipientSubset;
    }

    const existingRecipients = Array.isArray(broadcast?.recipients)
      ? broadcast.recipients
      : [];
    if (existingRecipients.length > 0) {
      return existingRecipients;
    }

    const audienceSource =
      broadcast?.audienceSource && typeof broadcast.audienceSource === "object"
        ? broadcast.audienceSource
        : {};
    const audienceSnapshot =
      broadcast?.audienceSnapshot &&
      typeof broadcast.audienceSnapshot === "object"
        ? broadcast.audienceSnapshot
        : {};
    const importJobId = String(
      audienceSource?.importJobId || audienceSnapshot?.importJobId || "",
    ).trim();
    const contactIds = Array.from(
      new Set(
        [
          ...(Array.isArray(audienceSource?.contactIds)
            ? audienceSource.contactIds
            : []),
          ...(Array.isArray(audienceSnapshot?.contactIds)
            ? audienceSnapshot.contactIds
            : []),
        ]
          .map((id) => String(id || "").trim())
          .filter(Boolean),
      ),
    );

    const query = {
      userId: userId || broadcast?.createdById || null,
      ...(companyId || broadcast?.companyId
        ? { companyId: companyId || broadcast?.companyId }
        : {}),
    };

    if (importJobId) {
      query.importJobId = importJobId;
    } else if (contactIds.length > 0) {
      query._id = { $in: contactIds };
    } else {
      return [];
    }

    const contacts = await Contact.find(query)
      .select(
        "_id name phone phoneDigits sourceType tags importJobId whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata",
      )
      .lean();

    return contacts
      .map((contact) => ({
        phone: String(contact?.phone || "").trim(),
        name: String(contact?.name || "").trim(),
        contactId: String(contact?._id || "").trim(),
        sourceType:
          String(contact?.sourceType || "imported").trim() || "imported",
        variables: [],
        attributes: contact,
      }))
      .filter((recipient) => Boolean(recipient.phone));
  }

  async resolveTemplateCategoryForBroadcast({ broadcast, credentials }) {
    const templateName = toCleanString(broadcast?.templateName || "");
    if (!templateName) return "";

    if (broadcast?.templateId) {
      const byId = await Template.findOne({
        _id: broadcast.templateId,
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
      })
        .select("category")
        .lean();
      if (byId?.category) return toCleanString(byId.category).toLowerCase();
    }

    const byName = await Template.findOne({
      userId: broadcast.createdById,
      companyId: broadcast.companyId,
      name: templateName,
    })
      .select("category")
      .lean();
    if (byName?.category) return toCleanString(byName.category).toLowerCase();

    try {
      const listResult = await whatsappService.getTemplateList(
        credentials || null,
      );
      if (!listResult?.success) return "";
      const templates = listResult?.data?.data || [];
      const requested = templateName.toLowerCase();
      const match = templates.find(
        (tpl) =>
          String(tpl?.name || "")
            .trim()
            .toLowerCase() === requested,
      );
      return toCleanString(match?.category || "").toLowerCase();
    } catch (_error) {
      return "";
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
            usageType: "whatsapp_message",
            count,
          },
          {
            headers: {
              "x-internal-api-key": ADMIN_INTERNAL_API_KEY,
            },
            timeout: 10000,
          },
        );
        return;
      } catch (error) {
        continue;
      }
    }
  }
  normalizePhoneNumber(phone) {
    return String(phone || "").replace(/\D/g, "");
  }

  normalizeRetryPolicy(policy = {}) {
    const maxAttempts = Math.max(
      1,
      Math.min(5, Number(policy?.maxAttempts || 2)),
    );
    const backoffSeconds = Math.max(
      0,
      Math.min(300, Number(policy?.backoffSeconds || 4)),
    );
    const retryableCodes = Array.isArray(policy?.retryableCodes)
      ? policy.retryableCodes
          .map((code) => String(code || "").trim())
          .filter(Boolean)
      : [];

    return {
      enabled: policy?.enabled !== false,
      maxAttempts,
      backoffSeconds,
      retryableCodes,
    };
  }

  normalizeDeliveryPolicy(policy = {}) {
    const quietHours =
      policy?.quietHours && typeof policy.quietHours === "object"
        ? policy.quietHours
        : {};
    const startHour = Math.max(
      0,
      Math.min(23, Number(quietHours?.startHour ?? 22)),
    );
    const endHour = Math.max(0, Math.min(23, Number(quietHours?.endHour ?? 8)));
    const timezone = String(quietHours?.timezone || "UTC").trim() || "UTC";
    const action =
      String(quietHours?.action || "defer").toLowerCase() === "skip"
        ? "skip"
        : "defer";
    const batchSize = Math.max(
      1,
      Math.min(50, Number(policy?.batchSize || 50)),
    );
    const batchDelaySeconds = Math.max(
      0,
      Math.min(3600, Number(policy?.batchDelaySeconds ?? 5)),
    );

    return {
      quietHours: {
        enabled: Boolean(quietHours?.enabled),
        startHour,
        endHour,
        timezone,
        action,
      },
      batchSize: Number.isFinite(batchSize) ? Math.trunc(batchSize) : 50,
      batchDelaySeconds: Number.isFinite(batchDelaySeconds)
        ? Math.trunc(batchDelaySeconds)
        : 5,
    };
  }

  normalizeBroadcastBatchPolicy(policy = {}) {
    const batchSize = Math.max(
      1,
      Math.min(50, Number(policy?.batchSize || 50)),
    );
    const batchDelaySeconds = Math.max(
      0,
      Math.min(3600, Number(policy?.batchDelaySeconds ?? 5)),
    );
    return {
      batchSize: Number.isFinite(batchSize) ? Math.trunc(batchSize) : 50,
      batchDelaySeconds: Number.isFinite(batchDelaySeconds)
        ? Math.trunc(batchDelaySeconds)
        : 5,
    };
  }

  normalizeTemplateVariables(variables = []) {
    if (!Array.isArray(variables)) return [];

    return variables
      .map((value) => {
        if (typeof value === "string") {
          return value.trim();
        }

        if (value && typeof value === "object") {
          return String(
            value.text ??
              value.value ??
              value.body ??
              value.content ??
              value.parameter ??
              value.name ??
              "",
          ).trim();
        }

        return String(value ?? "").trim();
      })
      .filter(Boolean);
  }

  extractTemplateVariableCount(templateText = "") {
    const matches = [...String(templateText || "").matchAll(/\{\{(\d+)\}\}/g)];
    if (matches.length === 0) return 0;

    return matches.reduce((maxValue, match) => {
      const numericValue = Number(match?.[1] || 0);
      return Number.isFinite(numericValue)
        ? Math.max(maxValue, numericValue)
        : maxValue;
    }, 0);
  }

  normalizeCompliancePolicy(policy = {}) {
    const phones = Array.isArray(policy?.suppressionListPhones)
      ? policy.suppressionListPhones
      : [];

    return {
      respectOptOut: policy?.respectOptOut !== false,
      suppressionListPhones: Array.from(
        new Set(
          phones
            .map((phone) => this.normalizePhoneNumber(phone))
            .filter(Boolean),
        ),
      ),
    };
  }

  normalizeBroadcastPolicies(broadcastData = {}) {
    const suppressionListPhonesFromRaw = String(
      broadcastData?.suppressionListRaw || "",
    )
      .split(/[\n,;\s]+/)
      .map((item) => this.normalizePhoneNumber(item))
      .filter(Boolean);

    const fallbackDeliveryPolicy = {
      quietHours: {
        enabled: Boolean(broadcastData?.quietHoursEnabled),
        startHour: broadcastData?.quietHoursStartHour,
        endHour: broadcastData?.quietHoursEndHour,
        timezone: broadcastData?.quietHoursTimezone,
        action: broadcastData?.quietHoursAction,
      },
    };

    const fallbackRetryPolicy = {
      enabled: broadcastData?.retryPolicyEnabled,
      maxAttempts: broadcastData?.retryMaxAttempts,
      backoffSeconds: broadcastData?.retryBackoffSeconds,
    };

    const fallbackCompliancePolicy = {
      respectOptOut: broadcastData?.respectOptOut,
      suppressionListPhones: Array.from(new Set(suppressionListPhonesFromRaw)),
    };

    return {
      ...broadcastData,
      retryPolicy: this.normalizeRetryPolicy({
        ...(fallbackRetryPolicy || {}),
        ...(broadcastData?.retryPolicy || {}),
      }),
      deliveryPolicy: this.normalizeDeliveryPolicy({
        ...(fallbackDeliveryPolicy || {}),
        ...(broadcastData?.deliveryPolicy || {}),
        batchSize:
          broadcastData?.deliveryPolicy?.batchSize ?? broadcastData?.batchSize,
        batchDelaySeconds:
          broadcastData?.deliveryPolicy?.batchDelaySeconds ??
          broadcastData?.batchDelaySeconds,
      }),
      compliancePolicy: this.normalizeCompliancePolicy({
        ...(fallbackCompliancePolicy || {}),
        ...(broadcastData?.compliancePolicy || {}),
      }),
      analytics: {
        suppressed: Number(broadcastData?.analytics?.suppressed || 0),
        deferred: Number(broadcastData?.analytics?.deferred || 0),
        retried: Number(broadcastData?.analytics?.retried || 0),
        failureCodeBreakdown:
          broadcastData?.analytics?.failureCodeBreakdown &&
          typeof broadcastData.analytics.failureCodeBreakdown === "object"
            ? broadcastData.analytics.failureCodeBreakdown
            : {},
      },
    };
  }

  getHourInTimezone(date, timezone) {
    try {
      const formatter = new Intl.DateTimeFormat("en-US", {
        hour: "numeric",
        hour12: false,
        timeZone: timezone || "UTC",
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

    const currentHour = this.getHourInTimezone(
      now,
      quietHours?.timezone || "UTC",
    );
    if (startHour < endHour) {
      return currentHour >= startHour && currentHour < endHour;
    }
    return currentHour >= startHour || currentHour < endHour;
  }

  computeNextAllowedTime(quietHours = {}, now = new Date()) {
    if (!quietHours?.enabled) return now;
    const timezone = quietHours?.timezone || "UTC";
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
    const maxAttempts = normalizedPolicy.enabled
      ? normalizedPolicy.maxAttempts
      : 1;
    const backoffMs = normalizedPolicy.backoffSeconds * 1000;
    let attempts = 0;
    let lastMeta = { errorCode: "", errorMessage: "", retryable: false };
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
            retriedCount,
          };
        }

        const meta = this.classifySendError(result?.error || result);
        const retryableByCode =
          meta.retryable ||
          (meta.errorCode &&
            normalizedPolicy.retryableCodes.includes(String(meta.errorCode)));
        lastMeta = { ...meta, retryable: retryableByCode };
        const canRetry =
          normalizedPolicy.enabled && retryableByCode && attempts < maxAttempts;
        if (!canRetry) break;

        retriedCount += 1;
        if (backoffMs > 0) {
          await new Promise((resolve) =>
            setTimeout(resolve, backoffMs * attempts),
          );
        }
      } catch (error) {
        const meta = this.classifySendError(error);
        const retryableByCode =
          meta.retryable ||
          (meta.errorCode &&
            normalizedPolicy.retryableCodes.includes(String(meta.errorCode)));
        lastMeta = { ...meta, retryable: retryableByCode };
        const canRetry =
          normalizedPolicy.enabled && retryableByCode && attempts < maxAttempts;
        if (!canRetry) break;

        retriedCount += 1;
        if (backoffMs > 0) {
          await new Promise((resolve) =>
            setTimeout(resolve, backoffMs * attempts),
          );
        }
      }
    }

    return {
      success: false,
      attempts,
      retriedCount,
      errorMeta: lastMeta,
    };
  }

  getStatusScore(status) {
    const normalized = String(status || "").toLowerCase();
    if (normalized === "read") return 4;
    if (normalized === "delivered") return 3;
    if (normalized === "sent") return 2;
    if (normalized === "failed") return 1;
    return 0;
  }

  async buildRecipientStatusDetails(broadcast) {
    const recipients = Array.isArray(broadcast?.recipients)
      ? broadcast.recipients
      : [];
    const detailsByPhone = new Map();
    recipients.forEach((recipient) => {
      const rawPhone = recipient?.phone || recipient;
      const normalizedPhone = this.normalizePhoneNumber(rawPhone);
      if (!normalizedPhone) return;
      detailsByPhone.set(normalizedPhone, {
        phone: rawPhone,
        name: recipient?.name || "",
        sent: false,
        delivered: false,
        read: false,
        failed: false,
        replied: false,
        replyCount: 0,
        status: "pending",
        lastSentAt: null,
        lastStatusAt: null,
        lastReplyAt: null,
        lastReplyText: "",
        lastFailureReason: "",
        lastFailureCode: "",
      });
    });

    const startTime = new Date(
      broadcast.startedAt || broadcast.createdAt || Date.now(),
    );
    const completedAt = new Date(broadcast.completedAt || startTime);
    const endTime = new Date(completedAt);
    endTime.setDate(endTime.getDate() + 1);

    let outboundMessages = await Message.find({
      userId: broadcast.createdById,
      companyId: broadcast.companyId,
      sender: "agent",
      broadcastId: broadcast._id,
    })
      .select("conversationId status timestamp text")
      .sort({ timestamp: 1 })
      .lean();

    // Legacy fallback: old records may not have broadcastId tagged.
    // In that case, find agent messages in recipient conversations during campaign window.
    if (outboundMessages.length === 0 && detailsByPhone.size > 0) {
      const recipientPhoneVariants = Array.from(detailsByPhone.keys());
      const recipientPhoneRegex = recipientPhoneVariants.map(
        (phone) => new RegExp(`${phone}$`),
      );
      const legacyConversations = await Conversation.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        $or: [
          { contactPhone: { $in: recipientPhoneVariants } },
          ...recipientPhoneRegex.map((phoneRegex) => ({
            contactPhone: phoneRegex,
          })),
        ],
      })
        .select("_id contactPhone contactName")
        .lean();

      const legacyConversationIds = legacyConversations.map((item) => item._id);
      if (legacyConversationIds.length > 0) {
        outboundMessages = await Message.find({
          userId: broadcast.createdById,
          companyId: broadcast.companyId,
          sender: "agent",
          conversationId: { $in: legacyConversationIds },
          timestamp: { $gte: startTime, $lte: endTime },
        })
          .select("conversationId status timestamp text")
          .sort({ timestamp: 1 })
          .lean();
      }
    }

    const conversationIds = Array.from(
      new Set(
        outboundMessages
          .map((message) => String(message.conversationId || ""))
          .filter(Boolean),
      ),
    );

    const conversations = conversationIds.length
      ? await Conversation.find({
          _id: { $in: conversationIds },
          companyId: broadcast.companyId,
        })
          .select("_id contactPhone contactName")
          .lean()
      : [];

    const conversationPhoneMap = new Map();
    conversations.forEach((conversation) => {
      conversationPhoneMap.set(String(conversation._id), {
        normalizedPhone: this.normalizePhoneNumber(conversation.contactPhone),
        rawPhone: conversation.contactPhone,
        contactName: conversation.contactName || "",
      });
    });

    outboundMessages.forEach((message) => {
      const conversationEntry = conversationPhoneMap.get(
        String(message.conversationId || ""),
      );
      const normalizedPhone = conversationEntry?.normalizedPhone;
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: conversationEntry?.rawPhone || normalizedPhone,
          name: conversationEntry?.contactName || "",
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: "pending",
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: "",
          lastFailureReason: "",
        });
      }

      const detail = detailsByPhone.get(normalizedPhone);
      const status = String(message.status || "sent").toLowerCase();
      const messageTime = message.timestamp
        ? new Date(message.timestamp)
        : null;

      detail.sent = true;
      if (
        !detail.lastSentAt ||
        (messageTime && messageTime > new Date(detail.lastSentAt))
      ) {
        detail.lastSentAt = messageTime;
      }

      if (!detail.name && conversationEntry?.contactName) {
        detail.name = conversationEntry.contactName;
      }
      if (!detail.phone && conversationEntry?.rawPhone) {
        detail.phone = conversationEntry.rawPhone;
      }

      if (status === "delivered" || status === "read") detail.delivered = true;
      if (status === "read") detail.read = true;
      if (status === "failed") detail.failed = true;

      const currentScore = this.getStatusScore(detail.status);
      const nextScore = this.getStatusScore(status);
      if (nextScore >= currentScore) {
        detail.status = status;
      }

      if (
        !detail.lastStatusAt ||
        (messageTime && messageTime > new Date(detail.lastStatusAt))
      ) {
        detail.lastStatusAt = messageTime;
      }
    });

    const firstSentTimeByConversation = new Map();
    outboundMessages.forEach((message) => {
      const key = String(message.conversationId || "");
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
          sender: "contact",
          conversationId: { $in: conversationIds },
          timestamp: { $gte: startTime },
        })
          .select("conversationId text timestamp")
          .sort({ timestamp: 1 })
          .lean()
      : [];

    incomingMessages.forEach((message) => {
      const conversationId = String(message.conversationId || "");
      const conversationEntry = conversationPhoneMap.get(conversationId);
      const normalizedPhone = conversationEntry?.normalizedPhone;
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: conversationEntry?.rawPhone || normalizedPhone,
          name: conversationEntry?.contactName || "",
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: "pending",
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: "",
        });
      }

      const firstSentTime = firstSentTimeByConversation.get(conversationId);
      const replyTime = message.timestamp ? new Date(message.timestamp) : null;
      if (firstSentTime && replyTime && replyTime < firstSentTime) return;

      const detail = detailsByPhone.get(normalizedPhone);
      detail.replied = true;
      detail.replyCount += 1;
      detail.lastReplyAt = replyTime || detail.lastReplyAt;
      detail.lastReplyText = String(message.text || "").trim();
    });

    const deliveryResults = Array.isArray(broadcast?.deliveryResults)
      ? broadcast.deliveryResults
      : [];
    deliveryResults.forEach((result) => {
      const normalizedPhone = this.normalizePhoneNumber(result?.phone || "");
      if (!normalizedPhone) return;

      if (!detailsByPhone.has(normalizedPhone)) {
        detailsByPhone.set(normalizedPhone, {
          phone: result?.phone || normalizedPhone,
          name: String(result?.name || "").trim(),
          sent: false,
          delivered: false,
          read: false,
          failed: false,
          replied: false,
          replyCount: 0,
          status: "pending",
          lastSentAt: null,
          lastStatusAt: null,
          lastReplyAt: null,
          lastReplyText: "",
          lastFailureReason: "",
        });
      }

      const detail = detailsByPhone.get(normalizedPhone);
      const status = String(result?.status || "").toLowerCase();
      const isFailed = result?.success === false && !result?.skipped;
      const isSkipped = Boolean(result?.skipped);

      if (isFailed) {
        detail.failed = true;
        detail.status = "failed";
        detail.lastFailureReason = String(
          result?.error || result?.reason || result?.policy?.error || "",
        ).trim();
        detail.lastFailureCode = String(
          result?.errorCode || result?.policy?.errorCode || "",
        ).trim();
        detail.lastStatusAt = detail.lastStatusAt || new Date();
      } else if (isSkipped) {
        detail.status = "pending";
        detail.lastFailureReason = String(result?.reason || "").trim();
        detail.lastFailureCode = String(
          result?.errorCode || result?.policy?.errorCode || "",
        ).trim();
      } else if (status === "sent" || result?.success === true) {
        detail.sent = true;
        if (!detail.status || detail.status === "pending") {
          detail.status = "sent";
        }
      }
    });

    // Final fallback: populate missing names from contacts table.
    const missingNamePhones = Array.from(detailsByPhone.entries())
      .filter(([, detail]) => !String(detail?.name || "").trim())
      .map(([normalizedPhone]) => normalizedPhone)
      .filter(Boolean);

    if (missingNamePhones.length > 0) {
      const contactPhoneRegex = missingNamePhones.map(
        (phone) => new RegExp(`${phone}$`),
      );
      const contacts = await Contact.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        $or: [
          { phone: { $in: missingNamePhones } },
          ...contactPhoneRegex.map((phoneRegex) => ({ phone: phoneRegex })),
        ],
      })
        .select("phone name")
        .lean();

      const contactNameByPhone = new Map();
      contacts.forEach((contact) => {
        const normalized = this.normalizePhoneNumber(contact?.phone);
        const name = String(contact?.name || "").trim();
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
      lastReplyAt: detail.lastReplyAt || null,
    }));
  }

  buildRetryCandidateList(broadcast, recipientDetails = []) {
    const recipients = Array.isArray(broadcast?.recipients)
      ? broadcast.recipients
      : [];
    const detailsByNormalizedPhone = new Map();

    recipientDetails.forEach((detail) => {
      const normalized = this.normalizePhoneNumber(detail?.phone || "");
      if (!normalized) return;
      detailsByNormalizedPhone.set(normalized, detail);
    });

    const seen = new Set();
    const retryCandidates = [];

    recipients.forEach((recipient) => {
      const normalized = this.normalizePhoneNumber(
        recipient?.phone || recipient,
      );
      if (!normalized || seen.has(normalized)) return;
      seen.add(normalized);

      const detail = detailsByNormalizedPhone.get(normalized);
      const status = String(detail?.status || "").toLowerCase();
      const shouldRetry =
        status === "failed" ||
        status === "pending" ||
        (!detail?.sent && !detail?.delivered && !detail?.read);

      if (!shouldRetry) return;

      retryCandidates.push({
        phone: recipient?.phone || recipient,
        name: recipient?.name || "",
        variables: Array.isArray(recipient?.variables)
          ? recipient.variables
          : [],
        attributes:
          recipient?.attributes && typeof recipient.attributes === "object"
            ? recipient.attributes
            : {},
        lastStatus: status || "pending",
      });
    });

    return retryCandidates;
  }

  async resolveCredentialsForBroadcast(broadcast, credentials = null) {
    if (credentials) return credentials;

    const authHeader = String(broadcast?.authHeaderSnapshot || "").trim();
    if (authHeader.startsWith("Bearer ")) {
      try {
        const fetched = await getWhatsAppCredentialsForUser({
          authHeader,
          userId: String(broadcast?.createdById || ""),
        });
        if (fetched) {
          return fetched;
        }
      } catch (error) {
        console.error(
          `Failed to fetch admin credentials for scheduled broadcast ${broadcast?._id}:`,
          error.message,
        );
      }
    }

    const snapshot = broadcast?.credentialsSnapshot || null;
    const accessToken = String(
      snapshot?.accessToken || snapshot?.whatsappToken || "",
    ).trim();
    const businessAccountId = String(
      snapshot?.businessAccountId || snapshot?.whatsappBusiness || "",
    ).trim();
    const phoneNumberId = String(
      snapshot?.phoneNumberId || snapshot?.whatsappId || "",
    ).trim();

    if (accessToken && businessAccountId && phoneNumberId) {
      return {
        accessToken,
        businessAccountId,
        phoneNumberId,
        whatsappToken: accessToken,
        whatsappBusiness: businessAccountId,
        whatsappId: phoneNumberId,
        twilioId: snapshot?.twilioId || null,
      };
    }

    const envAccessToken = String(
      process.env.WHATSAPP_ACCESS_TOKEN || "",
    ).trim();
    const envBusinessAccountId = String(
      process.env.WHATSAPP_BUSINESS_ACCOUNT_ID || "",
    ).trim();
    const envPhoneNumberId = String(
      process.env.WHATSAPP_PHONE_NUMBER_ID || "",
    ).trim();

    if (envAccessToken && envBusinessAccountId && envPhoneNumberId) {
      return {
        accessToken: envAccessToken,
        businessAccountId: envBusinessAccountId,
        phoneNumberId: envPhoneNumberId,
        whatsappToken: envAccessToken,
        whatsappBusiness: envBusinessAccountId,
        whatsappId: envPhoneNumberId,
        twilioId: null,
      };
    }

    return null;
  }

  async resolveTemplatePreviewTextFromMeta(
    templateName,
    language,
    credentials,
  ) {
    try {
      const listResult = await whatsappService.getTemplateList(
        credentials || null,
      );
      if (!listResult?.success) return null;

      const templates = listResult?.data?.data || [];
      const requested = String(templateName || "")
        .trim()
        .toLowerCase();
      const requestedLanguage = String(language || "")
        .trim()
        .toLowerCase();

      const matchedTemplate =
        templates.find(
          (t) =>
            String(t.name || "")
              .trim()
              .toLowerCase() === requested &&
            String(t.language || "")
              .trim()
              .toLowerCase() === requestedLanguage,
        ) ||
        templates.find(
          (t) =>
            String(t.name || "")
              .trim()
              .toLowerCase() === requested,
        );

      if (!matchedTemplate || !Array.isArray(matchedTemplate.components)) {
        return null;
      }

      const bodyComponent = matchedTemplate.components.find(
        (component) => String(component.type || "").toUpperCase() === "BODY",
      );
      const bodyText =
        bodyComponent?.text ||
        bodyComponent?.body_text ||
        (Array.isArray(bodyComponent?.example?.body_text)
          ? bodyComponent.example.body_text[0]
          : "");
      if (!bodyText) return null;

      return String(bodyText);
    } catch (error) {
      console.error(
        "Failed to resolve template preview text from Meta:",
        error.message,
      );
      return null;
    }
  }

  // Process template variables - matching Python reference format
  processTemplateVariables(templateContent, variables, rowData = {}) {
    let processedContent = String(templateContent || "");
    if (!processedContent) return "";

    if (rowData && typeof rowData === "object") {
      Object.keys(rowData).forEach((columnName) => {
        if (columnName === "phone") return;
        const placeholder = new RegExp(`\\{${columnName}\\}`, "g");
        processedContent = processedContent.replace(
          placeholder,
          rowData[columnName] || "",
        );
      });
    }

    // Support both {{1}} and {var1} formats like Python reference
    variables.forEach((varValue, index) => {
      // Replace {{1}} format
      const placeholder1 = new RegExp(`\\{\\{${index + 1}\\}\\}`, "g");
      processedContent = processedContent.replace(placeholder1, varValue);

      // Replace {var1} format
      const placeholder2 = new RegExp(`\\{var${index + 1}\\}`, "g");
      processedContent = processedContent.replace(placeholder2, varValue);
    });

    return processedContent;
  }

  classifySendError(errorPayload) {
    const raw = String(
      errorPayload?.error?.message ||
        errorPayload?.message ||
        errorPayload ||
        "",
    );
    const codeMatch = raw.match(/\b(13[0-9]{4}|63[0-9]{3})\b/);
    const errorCode = codeMatch ? codeMatch[1] : "";
    const retryableCodes = new Set([
      "131049",
      "131056",
      "131016",
      "131048",
      "63018",
    ]);
    const retryable =
      retryableCodes.has(errorCode) ||
      /timeout|temporar|temporarily|rate limit|try again/i.test(raw);

    return {
      errorCode,
      retryable,
      errorMessage: raw,
    };
  }

  getBroadcastRateLimitScopeKey({ broadcast = null, credentials = null } = {}) {
    const phoneNumberId = String(
      credentials?.phoneNumberId ||
        credentials?.whatsappId ||
        broadcast?.credentialsSnapshot?.phoneNumberId ||
        broadcast?.credentialsSnapshot?.whatsappId ||
        "",
    ).trim();
    if (phoneNumberId) {
      return `phone:${phoneNumberId}`;
    }

    const businessAccountId = String(
      credentials?.businessAccountId ||
        credentials?.whatsappBusiness ||
        broadcast?.credentialsSnapshot?.businessAccountId ||
        broadcast?.credentialsSnapshot?.whatsappBusiness ||
        "",
    ).trim();
    if (businessAccountId) {
      return `waba:${businessAccountId}`;
    }

    return `broadcast:${String(broadcast?._id || broadcast?.id || "unknown").trim()}`;
  }

  async enforceBroadcastRateLimit({
    broadcast = null,
    credentials = null,
    weight = 1,
  } = {}) {
    if (isRedisDisabled) {
      return { limited: false };
    }

    const scopeKey = this.getBroadcastRateLimitScopeKey({
      broadcast,
      credentials,
    });
    const redisKey = `broadcast:rate:${scopeKey}`;
    const increment = Math.max(1, Number(weight) || 1);

    const count = await broadcastRateLimiterRedis.incrby(redisKey, increment);
    if (count === increment) {
      await broadcastRateLimiterRedis.expire(
        redisKey,
        Math.ceil(BROADCAST_RATE_LIMIT_WINDOW_MS / 1000),
      );
    }

    if (count > BROADCAST_RATE_LIMIT_MAX) {
      const ttlSeconds = await broadcastRateLimiterRedis.ttl(redisKey);
      const retryAfterMs = Math.max(
        BROADCAST_RATE_LIMIT_WINDOW_MS,
        Number(ttlSeconds) > 0
          ? Number(ttlSeconds) * 1000
          : BROADCAST_RATE_LIMIT_WINDOW_MS,
      );
      const error = new Error("Broadcast rate limited");
      error.rateLimited = true;
      error.retryAfterMs = retryAfterMs;
      error.scopeKey = scopeKey;
      error.rateLimitKey = redisKey;
      throw error;
    }

    return {
      limited: false,
      count,
      scopeKey,
      rateLimitKey: redisKey,
    };
  }

  async getBroadcastRateLimitSnapshot({
    broadcastId = null,
    credentials = null,
  } = {}) {
    if (isRedisDisabled) {
      return {
        enabled: false,
        count: 0,
        ttlMs: 0,
        max: BROADCAST_RATE_LIMIT_MAX,
        windowMs: BROADCAST_RATE_LIMIT_WINDOW_MS,
        scopeKey: null,
        rateLimitKey: null,
      };
    }

    let broadcast = null;
    if (broadcastId) {
      broadcast = await Broadcast.findById(broadcastId)
        .select("_id credentialsSnapshot createdById companyId")
        .lean();
    }

    const snapshotCredentials =
      credentials ||
      (broadcast?.credentialsSnapshot
        ? {
            phoneNumberId: broadcast.credentialsSnapshot.phoneNumberId,
            whatsappId: broadcast.credentialsSnapshot.whatsappId,
            businessAccountId: broadcast.credentialsSnapshot.businessAccountId,
            whatsappBusiness: broadcast.credentialsSnapshot.whatsappBusiness,
          }
        : null);

    const scopeKey = this.getBroadcastRateLimitScopeKey({
      broadcast,
      credentials: snapshotCredentials,
    });
    const rateLimitKey = `broadcast:rate:${scopeKey}`;

    const [countRaw, ttlSeconds] = await Promise.all([
      broadcastRateLimiterRedis.get(rateLimitKey),
      broadcastRateLimiterRedis.ttl(rateLimitKey),
    ]);

    return {
      enabled: true,
      count: Math.max(0, Number(countRaw || 0) || 0),
      ttlMs: Math.max(0, Number(ttlSeconds || 0) || 0) * 1000,
      max: BROADCAST_RATE_LIMIT_MAX,
      windowMs: BROADCAST_RATE_LIMIT_WINDOW_MS,
      scopeKey,
      rateLimitKey,
    };
  }

  async createBroadcast(broadcastData, broadcaster = null) {
    try {
      broadcastData = this.normalizeBroadcastPolicies(broadcastData || {});
      const templateVariables = this.normalizeTemplateVariables(
        broadcastData.variables || broadcastData.templateParameters || [],
      );
      if (templateVariables.length > 0) {
        broadcastData.variables = templateVariables;
      }
      delete broadcastData.templateParameters;
      // If scheduledAt is provided, set status to 'scheduled'
      if (broadcastData.scheduledAt) {
        broadcastData.status = "scheduled";
        // Handle timezone properly - datetime-local comes without timezone info
        // Parse it as local time and preserve it exactly
        const scheduledDate = new Date(broadcastData.scheduledAt);

        // Check if the parsed date is valid
        if (isNaN(scheduledDate.getTime())) {
          return { success: false, error: "Invalid scheduled time format" };
        }
        if (scheduledDate.getTime() <= Date.now()) {
          return {
            success: false,
            error: "Scheduled time must be in the future",
          };
        }

        console.log(
          "📅 Original scheduledAt input:",
          broadcastData.scheduledAt,
        );
        console.log("📅 Parsed Date object:", scheduledDate);
        console.log("📅 Date string (local):", scheduledDate.toString());
        console.log("📅 Date string (UTC):", scheduledDate.toUTCString());
        console.log(
          "🔧 Timezone offset minutes:",
          scheduledDate.getTimezoneOffset(),
        );

        // Store the date as-is without timezone manipulation
        // MongoDB will store it in UTC and the comparison will work correctly
        broadcastData.scheduledAt = scheduledDate;
      }

      const broadcast = await Broadcast.create(broadcastData);
      console.log(
        "✅ Created broadcast with scheduledAt:",
        broadcast.scheduledAt,
      );
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: "broadcast_created",
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
      });
      return { success: true, data: broadcast };
    } catch (error) {
      console.error("❌ Error creating broadcast:", error);
      return { success: false, error: error.message };
    }
  }

  async getCampaignSelectionBroadcasts(filters = {}) {
    try {
      const companyId = filters?.companyId || null;
      const createdById = filters?.createdById || null;
      const statusFilter = String(filters?.status || "").trim();
      const search = String(filters?.search || "").trim();
      const limit = Math.max(1, Math.min(50, Number(filters?.limit || 20)));
      const cursor = this.decodePaginationCursor(filters?.cursor || "");

      const query = {};
      if (companyId) query.companyId = companyId;
      if (createdById) query.createdById = createdById;

      if (statusFilter) {
        const statuses = statusFilter
          .split(",")
          .map((value) => String(value || "").trim())
          .filter(Boolean);
        if (statuses.length === 1) {
          query.status = statuses[0];
        } else if (statuses.length > 1) {
          query.status = { $in: statuses };
        }
      } else {
        query.status = { $in: ["completed", "completed_with_errors"] };
      }

      if (search) {
        query.name = { $regex: search, $options: "i" };
      }

      if (cursor?.createdAt && cursor?.id) {
        const createdAt = new Date(cursor.createdAt);
        if (!Number.isNaN(createdAt.getTime())) {
          query.$or = [
            { createdAt: { $lt: createdAt } },
            { createdAt, _id: { $lt: cursor.id } },
          ];
        }
      }

      const rows = await Broadcast.find(query)
        .select(
          "name status scheduledAt startedAt completedAt createdAt updatedAt recipientCount stats messageType templateName language audienceSource",
        )
        .sort({ createdAt: -1, _id: -1 })
        .limit(limit + 1)
        .lean();

      const hasMore = rows.length > limit;
      const items = rows.slice(0, limit).map((broadcast) => ({
        ...broadcast,
        sentCount: Number(broadcast?.stats?.sent || 0),
        completedCount: Number(broadcast?.stats?.sent || 0),
      }));
      const lastItem = items[items.length - 1] || null;

      return {
        success: true,
        data: {
          items,
          meta: {
            hasMore,
            nextCursor:
              hasMore && lastItem
                ? this.encodePaginationCursor({
                    createdAt: lastItem.createdAt,
                    id: String(lastItem._id || ""),
                  })
                : "",
            limit,
            count: items.length,
          },
        },
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getBroadcastAudienceRecipients(broadcastId, filters = {}) {
    try {
      const broadcast = await Broadcast.findById(broadcastId)
        .select(
          "_id name companyId createdById recipientCount audienceSource audienceSnapshot recipients",
        )
        .lean();

      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      const companyId = filters?.companyId || broadcast.companyId || null;
      const createdById = filters?.createdById || broadcast.createdById || null;
      const search = String(filters?.search || "").trim();
      const limit = Math.max(1, Math.min(100, Number(filters?.limit || 50)));
      const cursor = this.decodePaginationCursor(filters?.cursor || "");
      const query = {
        broadcastId: broadcast._id,
        ...(companyId ? { companyId } : {}),
      };

      if (cursor?.recipientIndex !== undefined && cursor?.id) {
        const recipientIndex = Number(cursor.recipientIndex || 0);
        if (Number.isFinite(recipientIndex)) {
          query.$or = [
            { recipientIndex: { $gt: recipientIndex } },
            { recipientIndex, _id: { $gt: cursor.id } },
          ];
        }
      }

      if (search) {
        const searchPlan = buildContactSearchPlan(search);
        const contactQuery = {
          ...(companyId ? { companyId } : {}),
          ...(createdById ? { userId: createdById } : {}),
        };
        if (searchPlan.summaryClause) {
          Object.assign(contactQuery, searchPlan.summaryClause);
        }

        let contacts = [];
        if (Object.keys(contactQuery).length > 0) {
          contacts = await Contact.find(contactQuery)
            .select(
              "_id name phone phoneDigits email sourceType whatsappOptInStatus",
            )
            .limit(Math.max(limit * 10, 200))
            .lean();
        }

        if ((!contacts || contacts.length === 0) && searchPlan.fallbackClause) {
          const fallbackQuery = {
            ...(companyId ? { companyId } : {}),
            ...(createdById ? { userId: createdById } : {}),
            ...searchPlan.fallbackClause,
          };
          contacts = await Contact.find(fallbackQuery)
            .select(
              "_id name phone phoneDigits email sourceType whatsappOptInStatus",
            )
            .limit(Math.max(limit * 10, 200))
            .lean();
        }

        const candidatePhones = Array.from(
          new Set(
            (contacts || [])
              .map((contact) => String(contact?.phone || "").trim())
              .filter(Boolean),
          ),
        );

        if (candidatePhones.length === 0) {
          return {
            success: true,
            data: {
              items: [],
              meta: {
                hasMore: false,
                nextCursor: "",
                limit,
                count: 0,
                totalCount: 0,
              },
            },
          };
        }

        query.recipientPhone = { $in: candidatePhones };
      }

      const dispatches = await BroadcastDispatch.find(query)
        .select(
          "broadcastDispatchKey recipientPhone recipientIndex status sentAt failedAt whatsappMessageId messageText messageKind templateName templateLanguage createdAt updatedAt",
        )
        .sort({ recipientIndex: 1, _id: 1 })
        .limit(limit + 1)
        .lean();

      const hasMore = dispatches.length > limit;
      const pageDispatches = dispatches.slice(0, limit);
      const candidatePhones = pageDispatches
        .map((dispatch) => String(dispatch?.recipientPhone || "").trim())
        .filter(Boolean);
      const normalizedPhones = candidatePhones
        .map((phone) => this.normalizePhoneNumber(phone))
        .filter(Boolean);

      const contacts =
        candidatePhones.length > 0
          ? await Contact.find({
              ...(companyId ? { companyId } : {}),
              ...(createdById ? { userId: createdById } : {}),
              $or: [
                { phone: { $in: candidatePhones } },
                { phoneDigits: { $in: normalizedPhones } },
              ],
            })
              .select(
                "_id name phone phoneDigits email sourceType whatsappOptInStatus tags",
              )
              .lean()
          : [];

      const contactByPhone = new Map();
      for (const contact of contacts) {
        const exactPhone = String(contact?.phone || "").trim();
        const digitPhone = this.normalizePhoneNumber(contact?.phone || "");
        if (exactPhone && !contactByPhone.has(exactPhone)) {
          contactByPhone.set(exactPhone, contact);
        }
        if (digitPhone && !contactByPhone.has(digitPhone)) {
          contactByPhone.set(digitPhone, contact);
        }
      }

      const items = pageDispatches.map((dispatch) => {
        const exactPhone = String(dispatch?.recipientPhone || "").trim();
        const digitPhone = this.normalizePhoneNumber(exactPhone);
        const contact =
          contactByPhone.get(exactPhone) ||
          contactByPhone.get(digitPhone) ||
          null;
        return {
          dispatchId: String(dispatch?._id || ""),
          broadcastDispatchKey: String(dispatch?.broadcastDispatchKey || ""),
          recipientIndex: Number(dispatch?.recipientIndex || 0),
          phone: exactPhone,
          name: String(contact?.name || "").trim(),
          contactId: String(contact?._id || "").trim(),
          sourceType:
            String(contact?.sourceType || "campaign").trim() || "campaign",
          whatsappOptInStatus: String(
            contact?.whatsappOptInStatus || "",
          ).trim(),
          status: String(dispatch?.status || ""),
          sentAt: dispatch?.sentAt || null,
          failedAt: dispatch?.failedAt || null,
          messageText: String(dispatch?.messageText || "").trim(),
          messageKind: String(dispatch?.messageKind || "").trim(),
          templateName: String(dispatch?.templateName || "").trim(),
          templateLanguage: String(dispatch?.templateLanguage || "").trim(),
          data: contact || null,
        };
      });

      const lastItem = items[items.length - 1] || null;
      const totalCount = await BroadcastDispatch.countDocuments(query);

      return {
        success: true,
        data: {
          items,
          meta: {
            hasMore,
            nextCursor:
              hasMore && lastItem
                ? this.encodePaginationCursor({
                    recipientIndex: lastItem.recipientIndex,
                    id: lastItem.dispatchId,
                  })
                : "",
            limit,
            count: items.length,
            totalCount,
          },
        },
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async sendBroadcast(
    broadcastId,
    broadcaster,
    credentials = null,
    options = {},
  ) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      const recipientSubset = Array.isArray(options?.recipientSubset)
        ? options.recipientSubset
        : null;
      const skipFinalize = Boolean(options?.skipFinalize);

      const retryPolicy = this.normalizeRetryPolicy(
        broadcast?.retryPolicy || {},
      );
      const deliveryPolicy = this.normalizeDeliveryPolicy(
        broadcast?.deliveryPolicy || {},
      );
      const compliancePolicy = this.normalizeCompliancePolicy(
        broadcast?.compliancePolicy || {},
      );

      if (this.isWithinQuietHours(deliveryPolicy?.quietHours || {})) {
        const action = String(
          deliveryPolicy?.quietHours?.action || "defer",
        ).toLowerCase();
        if (action === "defer") {
          const nextAllowedAt = this.computeNextAllowedTime(
            deliveryPolicy?.quietHours || {},
          );
          broadcast.status = "scheduled";
          broadcast.scheduledAt = nextAllowedAt;
          broadcast.analytics = {
            ...(broadcast.analytics || {}),
            deferred: Number(broadcast?.analytics?.deferred || 0) + 1,
          };
          await broadcast.save();
          this.emitBroadcastRealtimeEvent(broadcaster, {
            type: "broadcast_updated",
            action: "deferred",
            broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
          });
          return {
            success: true,
            data: {
              deferred: true,
              reason: "quiet_hours",
              nextAttemptAt: nextAllowedAt,
              broadcast,
            },
          };
        }
        if (action === "skip") {
          broadcast.status = "completed";
          broadcast.completedAt = new Date();
          const skippedRecipients = Array.isArray(broadcast?.recipients)
            ? broadcast.recipients.length
            : 0;
          broadcast.analytics = {
            ...(broadcast.analytics || {}),
            skippedQuietHours:
              Number(broadcast?.analytics?.skippedQuietHours || 0) +
              skippedRecipients,
          };
          await broadcast.save();
          this.emitBroadcastRealtimeEvent(broadcaster, {
            type: "broadcast_updated",
            action: "quiet_hours_skip",
            broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
          });
          return {
            success: true,
            data: {
              skipped: true,
              reason: "quiet_hours_skip",
              skippedRecipients,
              broadcast,
            },
          };
        }
      }

      const resolvedCredentials = await this.resolveCredentialsForBroadcast(
        broadcast,
        credentials,
      );

      if (!resolvedCredentials) {
        return {
          success: false,
          error: "WhatsApp credentials are not configured for this user",
        };
      }

      console.log("🔍 Broadcast data being processed:", {
        _id: broadcast._id,
        name: broadcast.name,
        messageType: broadcast.messageType,
        templateName: broadcast.templateName,
        message: broadcast.message,
        language: broadcast.language,
        recipientsCount: broadcast.recipients?.length || 0,
      });

      if (!skipFinalize) {
        broadcast.status = "sending";
        broadcast.startedAt = new Date();
        await broadcast.save();
        this.emitBroadcastRealtimeEvent(broadcaster, {
          type: "broadcast_updated",
          action: "sending",
          broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
        });
      } else if (String(broadcast.status || "").toLowerCase() !== "sending") {
        await Broadcast.updateOne(
          { _id: broadcast._id },
          {
            $set: {
              status: "sending",
              startedAt: new Date(),
              updatedAt: new Date(),
            },
          },
        );
      }

      const results = [];
      const recordResult = skipFinalize
        ? () => {}
        : (entry) => results.push(entry);
      let successful = 0;
      let failed = 0;
      let suppressed = 0;
      let retried = 0;
      const failureCodeBreakdown =
        broadcast?.analytics?.failureCodeBreakdown &&
        typeof broadcast.analytics.failureCodeBreakdown === "object"
          ? { ...broadcast.analytics.failureCodeBreakdown }
          : {};
      let usageBatchCount = 0;
      const usageBatchSize = Number(process.env.BROADCAST_USAGE_BATCH || 50);
      const sendDelayMs = Math.max(
        0,
        Number(process.env.BROADCAST_SEND_DELAY_MS || 100),
      );
      const batchSize = Math.max(
        1,
        Math.min(50, Number(deliveryPolicy?.batchSize || 50)),
      );
      const batchDelayMs =
        Math.max(0, Number(deliveryPolicy?.batchDelaySeconds || 5)) * 1000;
      const suppressionSet = new Set(
        (compliancePolicy?.suppressionListPhones || [])
          .map((phone) => this.normalizePhoneNumber(phone))
          .filter(Boolean),
      );
      let templatePreviewText = broadcast.templateContent || null;
      const broadcastTemplateVariables = this.normalizeTemplateVariables(
        broadcast?.variables || broadcast?.templateParameters || [],
      );
      const explicitTemplateCategory = toCleanString(
        broadcast?.templateCategory || "",
      ).toLowerCase();
      let templateCategoryRaw = explicitTemplateCategory;
      if (!templateCategoryRaw && broadcast.messageType === "template") {
        templateCategoryRaw = await this.resolveTemplateCategoryForBroadcast({
          broadcast,
          credentials: resolvedCredentials,
        });
      }
      const templateCategory =
        toCleanString(templateCategoryRaw).toLowerCase() || "utility";
      const templateHeaderMediaUrl = String(broadcast?.mediaUrl || "").trim();
      const templateHeaderMediaType = String(broadcast?.mediaType || "")
        .trim()
        .toLowerCase();
      if (
        broadcast.templateName &&
        templateHeaderMediaType === "image" &&
        !templateHeaderMediaUrl
      ) {
        return {
          success: false,
          error:
            "This broadcast template requires an image header. Add an image URL before sending.",
        };
      }
      const templateComponents =
        broadcast.templateName &&
        templateHeaderMediaType === "image" &&
        templateHeaderMediaUrl
          ? [
              {
                type: "HEADER",
                parameters: [
                  {
                    type: "image",
                    image: { link: templateHeaderMediaUrl },
                  },
                ],
              },
            ]
          : null;

      // If templateContent is not stored, try to resolve from Meta
      if (!templatePreviewText && broadcast.templateName) {
        templatePreviewText = await this.resolveTemplatePreviewTextFromMeta(
          broadcast.templateName,
          broadcast.language || "en_US",
          resolvedCredentials,
        );
      }

      const requiredTemplateVariableCount = this.extractTemplateVariableCount(
        broadcast?.templateContent || templatePreviewText || "",
      );
      const hasAnyRecipientVariables = Array.isArray(broadcast?.recipients)
        ? broadcast.recipients.some(
            (recipient) =>
              Array.isArray(recipient?.variables) &&
              recipient.variables.length > 0,
          )
        : false;
      const hasBroadcastVariables = broadcastTemplateVariables.length > 0;
      if (
        broadcast.templateName &&
        requiredTemplateVariableCount > 0 &&
        !hasAnyRecipientVariables &&
        !hasBroadcastVariables
      ) {
        return {
          success: false,
          error: `Template "${broadcast.templateName}" needs ${requiredTemplateVariableCount} variable column(s) in the CSV file, but none were provided. Add var1, var2, etc. and try again.`,
        };
      }

      const recipients = await this.resolveBroadcastAudienceRecipients({
        broadcast,
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        recipientSubset,
      });
      for (
        let batchStart = 0;
        batchStart < recipients.length;
        batchStart += batchSize
      ) {
        const batchRecipients = recipients.slice(
          batchStart,
          batchStart + batchSize,
        );
        for (const recipient of batchRecipients) {
          try {
            const recipientVariables =
              Array.isArray(recipient?.variables) &&
              recipient.variables.length > 0
                ? recipient.variables
                : broadcastTemplateVariables;
            let result;
            const phoneNumber = recipient.phone || recipient;
            const normalizedPhone = this.normalizePhoneNumber(phoneNumber);
            const broadcastDispatchKey = `${broadcast._id}:${normalizedPhone || phoneNumber}`;
            const dispatchClaim = await this.claimBroadcastDispatch({
              broadcastDispatchKey,
              broadcastId: broadcast._id,
              userId: broadcast.createdById,
              companyId: broadcast.companyId,
              recipientPhone: phoneNumber,
              chunkId: String(options?.chunkId || ""),
              chunkIndex: Number(options?.chunkIndex || 0),
              recipientIndex: Number(
                batchStart + batchRecipients.indexOf(recipient) || 0,
              ),
            });
            if (dispatchClaim?.alreadyFinal || dispatchClaim?.locked) {
              recordResult({
                phone: phoneNumber,
                success: true,
                skipped: true,
                reason: dispatchClaim?.alreadyFinal
                  ? "already_dispatched"
                  : "locked_by_active_claim",
              });
              continue;
            }
            if (normalizedPhone && suppressionSet.has(normalizedPhone)) {
              suppressed += 1;
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "suppressed",
                errorMessage: "suppressed_by_compliance_policy",
              });
              recordResult({
                phone: phoneNumber,
                success: false,
                skipped: true,
                reason: "suppressed",
              });
              continue;
            }
            const rowData =
              recipient?.attributes && typeof recipient.attributes === "object"
                ? recipient.attributes
                : {};
            const contact = await this.resolveContactForRecipient({
              userId: broadcast.createdById,
              companyId: broadcast.companyId,
              phone: phoneNumber,
            });

            const requiresContactRecord =
              !broadcast.templateName || templateCategory === "marketing";

            if (!contact && requiresContactRecord) {
              failed++;
              broadcast.stats.failed++;
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "skipped",
                errorMessage: "contact_record_missing",
              });
              recordResult({
                phone: phoneNumber,
                success: false,
                error: "Contact record not found for compliance checks.",
              });
              continue;
            }

            let messageTextForInbox = broadcast.message;
            if (broadcast.templateName) {
              const normalizedTemplateName = String(
                broadcast.templateName || "",
              ).trim();
              if (!normalizedTemplateName) {
                await this.finalizeBroadcastDispatch({
                  broadcastDispatchKey,
                  status: "failed",
                  errorMessage: "template_name_missing",
                });
                recordResult({
                  phone: phoneNumber,
                  success: false,
                  error: "Template name is required",
                });
                failed++;
                broadcast.stats.failed++;
                continue;
              }

              const templateValidation = validateTemplateOutboundSend(contact, {
                templateCategory,
              });
              if (!templateValidation.ok) {
                failed++;
                broadcast.stats.failed++;
                await this.finalizeBroadcastDispatch({
                  broadcastDispatchKey,
                  status: "skipped",
                  errorMessage: templateValidation.error,
                });
                recordResult({
                  phone: phoneNumber,
                  success: false,
                  error: templateValidation.error,
                  policy: templateValidation.policy,
                });
                continue;
              }

              messageTextForInbox = templatePreviewText
                ? this.processTemplateVariables(
                    templatePreviewText,
                    this.normalizeTemplateVariables(recipientVariables),
                    rowData,
                  )
                : `Template: ${normalizedTemplateName}`;
              await this.enforceBroadcastRateLimit({
                broadcast,
                credentials: resolvedCredentials,
                weight: 1,
              });
              await this.noteBroadcastDispatchPayload({
                broadcastDispatchKey,
                messageText: messageTextForInbox,
                messageKind: "template",
                templateName: normalizedTemplateName,
                templateLanguage: broadcast.language || "en_US",
              });

              const sendTemplateOutcome = await this.sendWithRetry(
                () =>
                  whatsappService.sendTemplateMessage(
                    phoneNumber,
                    normalizedTemplateName,
                    broadcast.language || "en_US",
                    this.normalizeTemplateVariables(recipientVariables),
                    resolvedCredentials,
                    true,
                    templateComponents,
                  ),
                retryPolicy,
              );
              retried += Number(sendTemplateOutcome?.retriedCount || 0);
              result = sendTemplateOutcome.result;
              if (!sendTemplateOutcome.success) {
                const errorMeta = sendTemplateOutcome.errorMeta || {};
                failed++;
                broadcast.stats.failed++;
                await this.finalizeBroadcastDispatch({
                  broadcastDispatchKey,
                  status: "failed",
                  errorMessage:
                    errorMeta.errorMessage || "Template send failed",
                });
                if (errorMeta.errorCode) {
                  failureCodeBreakdown[errorMeta.errorCode] =
                    Number(failureCodeBreakdown[errorMeta.errorCode] || 0) + 1;
                }
                recordResult({
                  phone: phoneNumber,
                  success: false,
                  error: errorMeta.errorMessage || "Template send failed",
                  errorCode: errorMeta.errorCode || "",
                  retryable: Boolean(errorMeta.retryable),
                  attempts: sendTemplateOutcome.attempts || 1,
                });
                continue;
              }
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "sent",
                whatsappMessageId: result?.data?.messages?.[0]?.id || "",
              });

              console.log(`📤 Template send result for ${phoneNumber}:`, {
                success: sendTemplateOutcome.success,
                templateName: normalizedTemplateName,
                language: broadcast.language || "en_US",
                variables: this.normalizeTemplateVariables(recipientVariables),
                error: "",
              });
            } else if (broadcast.message) {
              const freeformValidation = validateFreeformOutboundSend(contact);
              if (!freeformValidation.ok) {
                failed++;
                broadcast.stats.failed++;
                await this.finalizeBroadcastDispatch({
                  broadcastDispatchKey,
                  status: "skipped",
                  errorMessage: freeformValidation.error,
                });
                recordResult({
                  phone: phoneNumber,
                  success: false,
                  error: freeformValidation.error,
                  policy: freeformValidation.policy,
                });
                continue;
              }

              // Process custom message with variable replacement
              const processedMessage = this.processTemplateVariables(
                broadcast.message,
                recipientVariables,
                rowData,
              );
              messageTextForInbox = processedMessage;
              await this.enforceBroadcastRateLimit({
                broadcast,
                credentials: resolvedCredentials,
                weight: 1,
              });
              await this.noteBroadcastDispatchPayload({
                broadcastDispatchKey,
                messageText: messageTextForInbox,
                messageKind: "text",
              });
              const sendTextOutcome = await this.sendWithRetry(
                () =>
                  whatsappService.sendTextMessage(
                    phoneNumber,
                    processedMessage,
                    resolvedCredentials,
                  ),
                retryPolicy,
              );
              retried += Number(sendTextOutcome?.retriedCount || 0);
              result = sendTextOutcome.result;
              if (!sendTextOutcome.success) {
                const errorMeta = sendTextOutcome.errorMeta || {};
                failed++;
                broadcast.stats.failed++;
                await this.finalizeBroadcastDispatch({
                  broadcastDispatchKey,
                  status: "failed",
                  errorMessage: errorMeta.errorMessage || "Message send failed",
                });
                if (errorMeta.errorCode) {
                  failureCodeBreakdown[errorMeta.errorCode] =
                    Number(failureCodeBreakdown[errorMeta.errorCode] || 0) + 1;
                }
                recordResult({
                  phone: phoneNumber,
                  success: false,
                  error: errorMeta.errorMessage || "Message send failed",
                  errorCode: errorMeta.errorCode || "",
                  retryable: Boolean(errorMeta.retryable),
                  attempts: sendTextOutcome.attempts || 1,
                });
                continue;
              }
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "sent",
                whatsappMessageId: result?.data?.messages?.[0]?.id || "",
              });
            } else {
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "failed",
                errorMessage: "No message or template specified",
              });
              recordResult({
                phone: phoneNumber,
                success: false,
                error: "No message or template specified",
              });
              failed++;
              broadcast.stats.failed++;
              continue;
            }

            if (result) {
              successful++;
              broadcast.stats.sent++;
              if (skipFinalize) {
                await this.enqueueBroadcastInboxWrite({
                  broadcastId: broadcast._id,
                  userId: broadcast.createdById,
                  companyId: broadcast.companyId,
                  phoneNumber,
                  message: messageTextForInbox,
                  whatsappResponse: result.data,
                  broadcastDispatchKey,
                  templateCategory,
                  contactId: contact?._id || "",
                });
              } else {
                // Keep the legacy synchronous path for direct/manual sends.
                const { conversation, message } = await this.updateConversation(
                  phoneNumber,
                  messageTextForInbox,
                  result.data,
                  broadcast._id,
                  broadcast.createdById,
                  broadcast.companyId,
                  broadcastDispatchKey,
                );

                if (
                  broadcast.templateName &&
                  templateCategory === "marketing" &&
                  contact
                ) {
                  applyMarketingTemplateSent(contact, { now: new Date() });
                  await contact.save();
                }

                if (contact) {
                  await this.logBroadcastContactActivity({
                    broadcast,
                    contact,
                    conversation,
                    messageText: messageTextForInbox,
                    templateCategory,
                  });
                }

                if (
                  typeof broadcaster === "function" &&
                  conversation &&
                  message
                ) {
                  broadcaster({
                    type: "message_sent",
                    conversation: conversation.toObject(),
                    message: message.toObject(),
                  });
                }
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
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "failed",
                errorMessage: errorMeta.errorMessage,
              });
              console.error(
                `❌ Failed to send to ${phoneNumber}:`,
                errorMeta.errorMessage,
              );
              recordResult({
                phone: phoneNumber,
                success: false,
                error: errorMeta.errorMessage,
                errorCode: errorMeta.errorCode,
                retryable: errorMeta.retryable,
              });
              continue;
            }

            recordResult({
              phone: phoneNumber,
              success: true,
              response: result.data,
            });

            // Rate limiting within the batch
            if (sendDelayMs > 0) {
              await new Promise((resolve) => setTimeout(resolve, sendDelayMs));
            }
          } catch (error) {
            if (error?.rateLimited) {
              throw error;
            }
            failed++;
            broadcast.stats.failed++;
            const errorMeta = this.classifySendError(error);
            try {
              await this.finalizeBroadcastDispatch({
                broadcastDispatchKey,
                status: "failed",
                errorMessage: errorMeta.errorMessage || error.message,
              });
            } catch (_dispatchFinalizeError) {
              // best effort only
            }
            recordResult({
              phone: recipient.phone || recipient,
              success: false,
              error: errorMeta.errorMessage || error.message,
              errorCode: errorMeta.errorCode,
              retryable: errorMeta.retryable,
            });
          }
        }

        const hasMoreBatches = batchStart + batchSize < recipients.length;
        if (hasMoreBatches && batchDelayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, batchDelayMs));
        }
      }

      broadcast.analytics = {
        ...(broadcast.analytics || {}),
        suppressed: Number(broadcast?.analytics?.suppressed || 0) + suppressed,
        deferred: Number(broadcast?.analytics?.deferred || 0),
        retried: Number(broadcast?.analytics?.retried || 0) + retried,
        failureCodeBreakdown,
      };
      const finalFailedCount = Array.isArray(results)
        ? results.filter((item) => item?.success === false && !item?.skipped).length
        : failed;
      const finalStatus = finalFailedCount > 0 ? "completed_with_errors" : "completed";
      if (!skipFinalize) {
        broadcast.deliveryResults = results;
        broadcast.status = finalStatus;
        broadcast.completedAt = new Date();
        await broadcast.save();
        this.emitBroadcastRealtimeEvent(broadcaster, {
          type: "broadcast_updated",
          action: finalStatus,
          broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
        });
      } else {
        await Broadcast.updateOne(
          { _id: broadcast._id },
          {
            $inc: {
              "stats.sent": successful,
              "stats.failed": failed,
              "analytics.suppressed": suppressed,
              "analytics.retried": retried,
            },
            $set: {
              status: finalStatus,
              completedAt: new Date(),
              updatedAt: new Date(),
            },
          },
        );
      }

      if (usageBatchCount > 0) {
        await this.reportUsage(broadcast.companyId, usageBatchCount);
      }

      // Note: Don't sync stats immediately after completion
      // Stats will be updated in real-time via message status updates
      // This prevents premature 100% read rates for first broadcasts

      return {
        success: true,
        data: {
          engine: "broadcast_direct_meta_v2",
          broadcast,
          results: skipFinalize ? [] : results,
          stats: {
            total: recipients.length,
            successful,
            failed,
            suppressed,
            retried,
          },
        },
      };
    } catch (error) {
      if (error?.rateLimited) {
        return {
          success: false,
          rateLimited: true,
          retryAfterMs: Number(
            error.retryAfterMs || BROADCAST_RATE_LIMIT_WINDOW_MS,
          ),
          error: error.message || "Broadcast rate limited",
        };
      }
      try {
        await Broadcast.findByIdAndUpdate(
          broadcastId,
          { $set: { status: "failed", completedAt: new Date() } },
          { new: false },
        );
      } catch (_updateError) {
        // best effort only
      }
      return { success: false, error: error.message };
    }
  }

  async updateConversation(
    phone,
    message,
    whatsappResponse,
    broadcastId,
    userId,
    companyId,
    broadcastDispatchKey = "",
  ) {
    try {
      const whatsappMessageId = whatsappResponse?.messages?.[0]?.id;
      let contact = await Contact.findOne({ userId, companyId, phone });
      if (!contact) {
        contact = await Contact.create({
          userId,
          companyId,
          phone,
          name: "",
          sourceType: "incoming_message",
        });
      } else if (broadcastId && contact.sourceType !== "incoming_message") {
        // If this contact is being used in broadcast message flow, mark source as message-origin.
        contact.sourceType = "incoming_message";
        await contact.save();
      }

      const conversationLookupFilter =
        buildConversationPhoneLookupFilter(phone);
      let conversation = await Conversation.findOne({
        userId,
        companyId,
        status: { $in: ["active", "pending"] },
        ...(conversationLookupFilter || { contactPhone: phone }),
      }).sort({ lastMessageTime: -1, updatedAt: -1, createdAt: -1 });
      if (!conversation) {
        conversation = await Conversation.create({
          userId,
          companyId,
          contactId: contact._id,
          contactPhone: phone,
          contactName: contact.name,
          lastMessage: message,
          lastMessageTime: new Date(),
          lastMessageMediaType: "",
          lastMessageAttachmentName: "",
          lastMessageAttachmentPages: null,
          lastMessageFrom: "agent",
          lastMessageWhatsappMessageId: whatsappMessageId || "",
        });
      } else {
        conversation.lastMessage = message;
        conversation.lastMessageTime = new Date();
        conversation.lastMessageMediaType = "";
        conversation.lastMessageAttachmentName = "";
        conversation.lastMessageAttachmentPages = null;
        conversation.lastMessageFrom = "agent";
        conversation.lastMessageWhatsappMessageId = whatsappMessageId || "";
        await conversation.save();
      }
      await syncConversationSummaryFromConversation(conversation);

      const savedMessage = await Message.create({
        userId,
        companyId,
        conversationId: conversation._id,
        sender: "agent",
        text: message,
        whatsappMessageId,
        status: "sent",
        ...(broadcastDispatchKey ? { broadcastDispatchKey } : {}),
        ...(broadcastId ? { broadcastId } : {}),
      });

      return { conversation, message: savedMessage };
    } catch (error) {
      console.error("Error updating conversation:", error);
      return { conversation: null, message: null };
    }
  }

  async enqueueBroadcastInboxWrite({
    broadcastId,
    userId,
    companyId,
    phoneNumber,
    message,
    whatsappResponse,
    broadcastDispatchKey = "",
    templateCategory = "",
    contactId = "",
    skipActivityLog = false,
  }) {
    return enqueueBroadcastInboxWrite({
      broadcastId,
      userId,
      companyId,
      phoneNumber,
      message,
      whatsappResponse,
      broadcastDispatchKey,
      templateCategory,
      contactId,
      skipActivityLog,
    });
  }

  async claimBroadcastDispatch({
    broadcastDispatchKey,
    broadcastId,
    userId,
    companyId,
    recipientPhone,
    chunkId = "",
    chunkIndex = 0,
    recipientIndex = 0,
  }) {
    const now = new Date();
    const staleWindowMs = Math.max(
      60_000,
      Number(process.env.BROADCAST_DISPATCH_STALE_MS || 5 * 60 * 1000),
    );
    const staleBefore = new Date(now.getTime() - staleWindowMs);

    const existing = await BroadcastDispatch.findOne({
      broadcastDispatchKey,
    }).lean();
    if (
      existing?.status === "sent" ||
      existing?.status === "suppressed" ||
      existing?.status === "skipped"
    ) {
      return { claimed: false, alreadyFinal: true, dispatch: existing };
    }

    if (
      existing?.status === "sending" &&
      existing?.claimedAt &&
      new Date(existing.claimedAt) > staleBefore
    ) {
      return { claimed: false, locked: true, dispatch: existing };
    }

    const nextRetryCount = Number(existing?.retryCount || 0) + 1;
    const nextStatus = existing ? "sending" : "pending";
    const dispatch = await BroadcastDispatch.findOneAndUpdate(
      { broadcastDispatchKey },
      {
        $setOnInsert: {
          broadcastDispatchKey,
          broadcastId,
          userId,
          companyId: companyId || null,
          recipientPhone,
          chunkId,
          chunkIndex,
          recipientIndex,
          createdAt: now,
        },
        $set: {
          status: "sending",
          claimedAt: now,
          lastAttemptAt: now,
          errorMessage: "",
          updatedAt: now,
        },
        $inc: {
          retryCount: existing ? 1 : 0,
        },
      },
      {
        new: true,
        upsert: true,
      },
    ).lean();

    return {
      claimed: true,
      dispatch,
      previousStatus: nextStatus,
      retryCount: nextRetryCount,
    };
  }

  async noteBroadcastDispatchPayload({
    broadcastDispatchKey,
    messageText = "",
    messageKind = "text",
    templateName = "",
    templateLanguage = "",
  }) {
    if (!broadcastDispatchKey) return;
    await BroadcastDispatch.updateOne(
      { broadcastDispatchKey },
      {
        $set: {
          messageText: String(messageText || ""),
          messageKind: String(messageKind || "text"),
          templateName: String(templateName || ""),
          templateLanguage: String(templateLanguage || ""),
          updatedAt: new Date(),
        },
      },
    );
  }

  async finalizeBroadcastDispatch({
    broadcastDispatchKey,
    status,
    whatsappMessageId = "",
    conversationId = null,
    messageId = null,
    errorMessage = "",
  }) {
    const now = new Date();
    const normalizedStatus = String(status || "").toLowerCase();
    const update = {
      status: normalizedStatus,
      lastAttemptAt: now,
      updatedAt: now,
      errorMessage: String(errorMessage || "").trim(),
    };
    if (normalizedStatus === "sent") {
      update.sentAt = now;
      update.whatsappMessageId = String(whatsappMessageId || "").trim();
      update.conversationId = conversationId || null;
      update.messageId = messageId || null;
    } else if (normalizedStatus === "failed") {
      update.failedAt = now;
    }

    await BroadcastDispatch.updateOne(
      { broadcastDispatchKey },
      {
        $set: update,
      },
    );
  }

  async repairBroadcastDispatchInbox({
    broadcastDispatchKey = "",
    whatsappMessageId = "",
    messageId = null,
  }) {
    const dispatchQuery = broadcastDispatchKey
      ? { broadcastDispatchKey }
      : whatsappMessageId
        ? { whatsappMessageId }
        : null;

    if (!dispatchQuery) {
      return {
        success: false,
        error: "Missing dispatch key or whatsapp message id",
      };
    }

    const dispatch = await BroadcastDispatch.findOne(dispatchQuery);
    if (!dispatch) {
      return { success: false, error: "Dispatch not found" };
    }

    if (dispatch.messageId) {
      return {
        success: true,
        data: { repaired: false, reason: "already_linked" },
      };
    }

    const existingMessage =
      (dispatch.broadcastDispatchKey
        ? await Message.findOne({
            broadcastDispatchKey: dispatch.broadcastDispatchKey,
          }).lean()
        : null) ||
      (whatsappMessageId
        ? await Message.findOne({ whatsappMessageId }).lean()
        : null);

    if (existingMessage) {
      await BroadcastDispatch.updateOne(
        { _id: dispatch._id },
        {
          $set: {
            messageId: existingMessage._id,
            conversationId: existingMessage.conversationId || null,
            whatsappMessageId:
              existingMessage.whatsappMessageId || whatsappMessageId || "",
            status: "sent",
            sentAt: existingMessage.timestamp || new Date(),
            updatedAt: new Date(),
          },
        },
      );
      return {
        success: true,
        data: { repaired: false, reason: "message_already_exists" },
      };
    }

    const broadcast = await Broadcast.findById(dispatch.broadcastId).lean();
    if (!broadcast) {
      return { success: false, error: "Broadcast not found for repair" };
    }

    const inboxText = String(dispatch.messageText || "").trim();
    if (!inboxText) {
      return {
        success: false,
        error: "Missing dispatch message text for repair",
      };
    }

    const phone = String(dispatch.recipientPhone || "").trim();
    const response = {
      messages: [
        { id: String(whatsappMessageId || dispatch.whatsappMessageId || "") },
      ],
    };
    const updateResult = await this.updateConversation(
      phone,
      inboxText,
      response,
      dispatch.broadcastId,
      dispatch.userId,
      dispatch.companyId,
      dispatch.broadcastDispatchKey,
    );

    if (!updateResult?.message?._id) {
      return { success: false, error: "Failed to reconstruct inbox message" };
    }

    if (messageId) {
      await BroadcastDispatch.updateOne(
        { _id: dispatch._id },
        {
          $set: {
            messageId,
            conversationId: updateResult.conversation?._id || null,
            whatsappMessageId:
              whatsappMessageId || dispatch.whatsappMessageId || "",
            status: "sent",
            sentAt: new Date(),
            updatedAt: new Date(),
          },
        },
      );
    } else {
      await BroadcastDispatch.updateOne(
        { _id: dispatch._id },
        {
          $set: {
            messageId: updateResult.message._id,
            conversationId: updateResult.conversation?._id || null,
            whatsappMessageId:
              whatsappMessageId || dispatch.whatsappMessageId || "",
            status: "sent",
            sentAt: new Date(),
            updatedAt: new Date(),
          },
        },
      );
    }

    return {
      success: true,
      data: {
        repaired: true,
        conversationId: String(updateResult.conversation?._id || ""),
        messageId: String(updateResult.message._id || ""),
      },
    };
  }

  async repairMissingBroadcastDispatchInboxes(limit = 50) {
    const staleBefore = new Date(
      Date.now() -
        Math.max(
          2 * 60 * 1000,
          Number(
            process.env.BROADCAST_DISPATCH_REPAIR_STALE_MS || 5 * 60 * 1000,
          ),
        ),
    );
    const dispatches = await BroadcastDispatch.find({
      status: "sent",
      $or: [{ messageId: { $exists: false } }, { messageId: null }],
      sentAt: { $lte: staleBefore },
    })
      .sort({ sentAt: 1 })
      .limit(Math.max(1, Number(limit) || 50))
      .lean();

    const repaired = [];
    for (const dispatch of dispatches) {
      try {
        const result = await this.repairBroadcastDispatchInbox({
          broadcastDispatchKey: dispatch.broadcastDispatchKey,
          whatsappMessageId: dispatch.whatsappMessageId || "",
        });
        if (result?.success && result?.data?.repaired) {
          repaired.push(dispatch.broadcastDispatchKey);
        }
      } catch (error) {
        console.error("Dispatch inbox repair failed:", error.message);
      }
    }

    return {
      success: true,
      data: {
        scanned: dispatches.length,
        repaired: repaired.length,
        repairedKeys: repaired,
      },
    };
  }

  async repairBroadcastDispatchInboxForBroadcast(broadcastId, limit = 50) {
    const normalizedBroadcastId = String(broadcastId || "").trim();
    if (!normalizedBroadcastId) {
      return { success: false, error: "Broadcast id is required" };
    }

    const staleBefore = new Date(
      Date.now() -
        Math.max(
          2 * 60 * 1000,
          Number(
            process.env.BROADCAST_DISPATCH_REPAIR_STALE_MS || 5 * 60 * 1000,
          ),
        ),
    );

    const dispatches = await BroadcastDispatch.find({
      broadcastId: normalizedBroadcastId,
      status: "sent",
      $or: [{ messageId: { $exists: false } }, { messageId: null }],
      sentAt: { $lte: staleBefore },
    })
      .sort({ sentAt: 1 })
      .limit(Math.max(1, Number(limit) || 50))
      .lean();

    const repaired = [];
    for (const dispatch of dispatches) {
      try {
        const result = await this.repairBroadcastDispatchInbox({
          broadcastDispatchKey: dispatch.broadcastDispatchKey,
          whatsappMessageId: dispatch.whatsappMessageId || "",
        });
        if (result?.success && result?.data?.repaired) {
          repaired.push(dispatch.broadcastDispatchKey);
        }
      } catch (error) {
        console.error("Broadcast dispatch repair failed:", error.message);
      }
    }

    return {
      success: true,
      data: {
        scanned: dispatches.length,
        repaired: repaired.length,
        repairedKeys: repaired,
      },
    };
  }

  async getBroadcasts(filters = {}) {
    try {
      const companyId = filters?.companyId || null;
      const createdById = filters?.createdById || null;
      const statusFilter = String(filters?.status || "").trim();
      const search = String(filters?.search || "").trim();
      const hasPagination =
        Number(filters?.limit || 0) > 0 || String(filters?.cursor || "").trim();
      const limit = Math.max(1, Math.min(100, Number(filters?.limit || 20)));
      const cursor = hasPagination
        ? this.decodePaginationCursor(filters?.cursor || "")
        : null;

      const query = {};
      if (companyId) query.companyId = companyId;
      if (createdById) query.createdById = createdById;

      if (statusFilter) {
        const statuses = statusFilter
          .split(",")
          .map((value) => String(value || "").trim())
          .filter(Boolean);
        if (statuses.length === 1) {
          query.status = statuses[0];
        } else if (statuses.length > 1) {
          query.status = { $in: statuses };
        }
      }

      if (search) {
        query.name = { $regex: search, $options: "i" };
      }

      if (hasPagination && cursor?.createdAt && cursor?.id) {
        const createdAt = new Date(cursor.createdAt);
        if (!Number.isNaN(createdAt.getTime())) {
          query.$or = [
            { createdAt: { $lt: createdAt } },
            { createdAt, _id: { $lt: cursor.id } },
          ];
        }
      }

      const projection =
        "name status scheduledAt startedAt completedAt createdAt updatedAt recipientCount stats messageType templateName language audienceSource";

      if (hasPagination) {
        const rows = await Broadcast.find(query)
          .select(projection)
          .sort({ createdAt: -1, _id: -1 })
          .limit(limit + 1)
          .lean();

        const needsStatsRepair = (broadcast = {}) => {
          const status = String(broadcast?.status || "").toLowerCase();
          const stats = broadcast?.stats || {};
          const sent = Number(stats.sent || 0);
          const delivered = Number(stats.delivered || 0);
          const read = Number(stats.read || 0);
          const failed = Number(stats.failed || 0);
          const replied = Number(stats.replied || 0);
          const recipientCount = Number(broadcast?.recipientCount || 0);
          const hasAnyStats =
            sent > 0 ||
            delivered > 0 ||
            read > 0 ||
            failed > 0 ||
            replied > 0;
          const deliveryStatsMissing =
            sent > 0 && delivered === 0 && read === 0 && failed === 0;
          const statsLookIncomplete =
            recipientCount > 0 &&
            (sent < recipientCount || deliveryStatsMissing);
          const statusRepairNeeded =
            status === "completed_with_errors" && failed > 0;

          return (
            ["completed", "completed_with_errors", "failed"].includes(status) &&
            (statsLookIncomplete ||
              (recipientCount > 0 && !hasAnyStats) ||
              statusRepairNeeded)
          );
        };

        const staleRows = Array.isArray(rows)
          ? rows.filter((broadcast) => needsStatsRepair(broadcast))
          : [];

        if (staleRows.length > 0) {
          for (const staleBroadcast of staleRows) {
            try {
              const repairResult = await this.syncBroadcastStats(
                staleBroadcast._id,
              );
              if (repairResult?.success && repairResult?.data?.stats) {
                staleBroadcast.stats = repairResult.data.stats;
              }
            } catch (_repairError) {
              // Best-effort repair only; keep list endpoint responsive.
            }
          }
        }

        const hasMore = rows.length > limit;
        const items = rows.slice(0, limit).map((broadcast) => ({
          ...broadcast,
          sentCount: Number(broadcast?.stats?.sent || 0),
          completedCount: Number(broadcast?.stats?.sent || 0),
        }));
        const lastItem = items[items.length - 1] || null;

        return {
          success: true,
          data: {
            items,
            meta: {
              hasMore,
              nextCursor:
                hasMore && lastItem
                  ? this.encodePaginationCursor({
                      createdAt: lastItem.createdAt,
                      id: String(lastItem._id || ""),
                    })
                  : "",
              limit,
              count: items.length,
            },
          },
        };
      }

      // Keep list endpoint lightweight for fast overview updates.
      const broadcasts = await Broadcast.find(filters)
        .select(projection)
        .sort({ createdAt: -1 })
        .lean();

      const staleCandidates = Array.isArray(broadcasts)
        ? broadcasts.filter((broadcast) => {
            const status = String(broadcast?.status || "").toLowerCase();
            const stats = broadcast?.stats || {};
            const sent = Number(stats.sent || 0);
            const delivered = Number(stats.delivered || 0);
            const read = Number(stats.read || 0);
            const failed = Number(stats.failed || 0);
            const replied = Number(stats.replied || 0);
            const recipientCount = Number(broadcast?.recipientCount || 0);
            const hasAnyStats =
              sent > 0 ||
              delivered > 0 ||
              read > 0 ||
              failed > 0 ||
              replied > 0;
            const deliveryStatsMissing =
              sent > 0 && delivered === 0 && read === 0 && failed === 0;
            const statsLookIncomplete =
              recipientCount > 0 &&
              (sent < recipientCount || deliveryStatsMissing);
            const statusRepairNeeded =
              status === "completed_with_errors" && failed > 0;
            return (
              ["completed", "completed_with_errors", "failed"].includes(
                status,
              ) &&
              (statsLookIncomplete ||
                (recipientCount > 0 && !hasAnyStats) ||
                statusRepairNeeded)
            );
          })
        : [];

      if (staleCandidates.length > 0) {
        for (const staleBroadcast of staleCandidates) {
          try {
            const repairResult = await this.syncBroadcastStats(
              staleBroadcast._id,
            );
            if (repairResult?.success && repairResult?.data?.stats) {
              staleBroadcast.stats = repairResult.data.stats;
            }
          } catch (_repairError) {
            // Best-effort repair only; keep list endpoint responsive.
          }
        }
      }

      return { success: true, data: broadcasts };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getBroadcastById(broadcastId) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }
      const recipientDetails =
        await this.buildRecipientStatusDetails(broadcast);
      const data = broadcast.toObject ? broadcast.toObject() : broadcast;
      const statusBreakdown = recipientDetails.reduce(
        (accumulator, item) => {
          const status = String(item?.status || "pending").toLowerCase();
          if (!accumulator[status]) {
            accumulator[status] = 0;
          }
          accumulator[status] += 1;
          return accumulator;
        },
        { pending: 0, sent: 0, delivered: 0, read: 0, failed: 0 },
      );
      const retryCandidates = this.buildRetryCandidateList(
        broadcast,
        recipientDetails,
      );

      data.recipientDetails = recipientDetails;
      data.statusBreakdown = statusBreakdown;
      data.retrySummary = {
        retryCandidates: retryCandidates.length,
        canRetry: retryCandidates.length > 0,
        retryPolicy: this.normalizeRetryPolicy(data?.retryPolicy || {}),
        deliveryPolicy: this.normalizeDeliveryPolicy(
          data?.deliveryPolicy || {},
        ),
        compliancePolicy: this.normalizeCompliancePolicy(
          data?.compliancePolicy || {},
        ),
        analytics: {
          suppressed: Number(data?.analytics?.suppressed || 0),
          deferred: Number(data?.analytics?.deferred || 0),
          retried: Number(data?.analytics?.retried || 0),
          failureCodeBreakdown:
            data?.analytics?.failureCodeBreakdown &&
            typeof data.analytics.failureCodeBreakdown === "object"
              ? data.analytics.failureCodeBreakdown
              : {},
        },
      };
      return { success: true, data };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getReliabilitySummary(filters = {}) {
    try {
      const normalizedFilters = {
        companyId: filters?.companyId || null,
        createdById: filters?.createdById || null,
        status: String(filters?.status || "").trim(),
        createdFrom: String(filters?.createdFrom || "").trim(),
        createdTo: String(filters?.createdTo || "").trim(),
      };
      const cacheScope = [
        `company:${normalizedFilters.companyId || "all"}`,
        `user:${normalizedFilters.createdById || "all"}`,
        `status:${normalizedFilters.status || "all"}`,
        `from:${normalizedFilters.createdFrom || "all"}`,
        `to:${normalizedFilters.createdTo || "all"}`,
      ].join("|");

      return await getOrSetCachedJson({
        namespace: "broadcasts",
        scope: cacheScope,
        versionGroup: "reliability-summary-v2",
        keyParts: ["dashboard"],
        ttlSeconds: Math.max(10, Number(CACHE_TTL_SECONDS?.summaryPages || 20)),
        loader: async () => {
          const query = {};
          if (normalizedFilters.companyId) {
            query.companyId = toQueryObjectId(normalizedFilters.companyId);
          }
          if (normalizedFilters.createdById) {
            query.createdById = toQueryObjectId(normalizedFilters.createdById);
          }
          if (normalizedFilters.status) {
            const statuses = normalizedFilters.status
              .split(",")
              .map((value) => String(value || "").trim())
              .filter(Boolean);
            if (statuses.length === 1) {
              query.status = statuses[0];
            } else if (statuses.length > 1) {
              query.status = { $in: statuses };
            }
          }

          const createdAt = {};
          if (normalizedFilters.createdFrom) {
            const from = new Date(normalizedFilters.createdFrom);
            if (!Number.isNaN(from.getTime())) {
              createdAt.$gte = from;
            }
          }
          if (normalizedFilters.createdTo) {
            const to = new Date(normalizedFilters.createdTo);
            if (!Number.isNaN(to.getTime())) {
              createdAt.$lte = to;
            }
          }
          if (Object.keys(createdAt).length > 0) {
            query.createdAt = createdAt;
          }

          const summaryPipeline = [
            { $match: query },
            {
              $project: {
                recipientCountResolved: {
                  $ifNull: [
                    "$recipientCount",
                    {
                      $size: {
                        $ifNull: ["$recipients", []],
                      },
                    },
                  ],
                },
                analytics: {
                  suppressed: { $ifNull: ["$analytics.suppressed", 0] },
                  deferred: { $ifNull: ["$analytics.deferred", 0] },
                  retried: { $ifNull: ["$analytics.retried", 0] },
                  skippedQuietHours: {
                    $ifNull: ["$analytics.skippedQuietHours", 0],
                  },
                  failureCodeBreakdown: {
                    $ifNull: ["$analytics.failureCodeBreakdown", {}],
                  },
                },
              },
            },
            {
              $facet: {
                totals: [
                  {
                    $group: {
                      _id: null,
                      campaigns: { $sum: 1 },
                      recipientCount: { $sum: "$recipientCountResolved" },
                      suppressed: { $sum: "$analytics.suppressed" },
                      deferred: { $sum: "$analytics.deferred" },
                      retried: { $sum: "$analytics.retried" },
                      skippedQuietHours: {
                        $sum: "$analytics.skippedQuietHours",
                      },
                    },
                  },
                ],
                failureCodes: [
                  {
                    $project: {
                      entries: {
                        $objectToArray: "$analytics.failureCodeBreakdown",
                      },
                    },
                  },
                  {
                    $unwind: {
                      path: "$entries",
                      preserveNullAndEmptyArrays: true,
                    },
                  },
                  {
                    $group: {
                      _id: "$entries.k",
                      count: { $sum: { $ifNull: ["$entries.v", 0] } },
                    },
                  },
                ],
              },
            },
          ];

          const aggregateResult = await Broadcast.aggregate(summaryPipeline);
          const totals = Array.isArray(aggregateResult?.[0]?.totals)
            ? aggregateResult[0].totals[0] || {}
            : {};
          const failureCodes = Array.isArray(aggregateResult?.[0]?.failureCodes)
            ? aggregateResult[0].failureCodes
            : [];
          const failureCodeBreakdown = failureCodes.reduce(
            (accumulator, item) => {
              const key = String(item?._id || "").trim();
              if (!key) return accumulator;
              accumulator[key] = Number(item?.count || 0) || 0;
              return accumulator;
            },
            {},
          );

          const topFailureCode =
            failureCodes
              .map((item) => ({
                code: String(item?._id || "").trim(),
                count: Number(item?.count || 0),
              }))
              .filter((item) => item.code)
              .sort((a, b) => b.count - a.count)[0] || null;

          return {
            success: true,
            data: {
              campaigns: Number(totals?.campaigns || 0) || 0,
              recipientCount: Number(totals?.recipientCount || 0) || 0,
              suppressed: Number(totals?.suppressed || 0) || 0,
              deferred: Number(totals?.deferred || 0) || 0,
              retried: Number(totals?.retried || 0) || 0,
              skippedQuietHours: Number(totals?.skippedQuietHours || 0) || 0,
              failureCodeBreakdown,
              topFailureCode,
            },
          };
        },
      });
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async getOverviewSummary(filters = {}) {
    try {
      const normalizedFilters = {
        companyId: filters?.companyId || null,
        createdById: filters?.createdById || null,
        status: String(filters?.status || "").trim(),
        createdFrom: String(filters?.createdFrom || "").trim(),
        createdTo: String(filters?.createdTo || "").trim(),
      };
      const cacheScope = [
        `company:${normalizedFilters.companyId || "all"}`,
        `user:${normalizedFilters.createdById || "all"}`,
        `status:${normalizedFilters.status || "all"}`,
        `from:${normalizedFilters.createdFrom || "all"}`,
        `to:${normalizedFilters.createdTo || "all"}`,
      ].join("|");

      return await getOrSetCachedJson({
        namespace: "broadcasts",
        scope: cacheScope,
        versionGroup: "overview-summary-v1",
        keyParts: ["dashboard"],
        ttlSeconds: Math.max(10, Number(CACHE_TTL_SECONDS?.summaryPages || 20)),
        loader: async () => {
          const query = {};
          if (normalizedFilters.companyId) {
            query.companyId = toQueryObjectId(normalizedFilters.companyId);
          }
          if (normalizedFilters.createdById) {
            query.createdById = toQueryObjectId(normalizedFilters.createdById);
          }
          if (normalizedFilters.status) {
            const statuses = normalizedFilters.status
              .split(",")
              .map((value) => String(value || "").trim())
              .filter(Boolean);
            if (statuses.length === 1) {
              query.status = statuses[0];
            } else if (statuses.length > 1) {
              query.status = { $in: statuses };
            }
          }

          const createdAt = {};
          if (normalizedFilters.createdFrom) {
            const from = new Date(normalizedFilters.createdFrom);
            if (!Number.isNaN(from.getTime())) {
              createdAt.$gte = from;
            }
          }
          if (normalizedFilters.createdTo) {
            const to = new Date(normalizedFilters.createdTo);
            if (!Number.isNaN(to.getTime())) {
              createdAt.$lte = to;
            }
          }
          if (Object.keys(createdAt).length > 0) {
            query.createdAt = createdAt;
          }

          const summaryPipeline = [
            { $match: query },
            {
              $project: {
                recipientCountResolved: {
                  $ifNull: [
                    "$recipientCount",
                    {
                      $size: {
                        $ifNull: ["$recipients", []],
                      },
                    },
                  ],
                },
                stats: {
                  sent: { $ifNull: ["$stats.sent", 0] },
                  delivered: { $ifNull: ["$stats.delivered", 0] },
                  read: { $ifNull: ["$stats.read", 0] },
                  replied: { $ifNull: ["$stats.replied", 0] },
                  failed: { $ifNull: ["$stats.failed", 0] },
                },
                status: { $ifNull: ["$status", ""] },
                analytics: {
                  suppressed: { $ifNull: ["$analytics.suppressed", 0] },
                  deferred: { $ifNull: ["$analytics.deferred", 0] },
                  retried: { $ifNull: ["$analytics.retried", 0] },
                  skippedQuietHours: {
                    $ifNull: ["$analytics.skippedQuietHours", 0],
                  },
                },
              },
            },
            {
              $facet: {
                totals: [
                  {
                    $group: {
                      _id: null,
                      campaigns: { $sum: 1 },
                      recipientCount: { $sum: "$recipientCountResolved" },
                      sent: { $sum: "$stats.sent" },
                      delivered: {
                        $sum: {
                          $cond: [
                            { $gte: ["$stats.delivered", "$stats.read"] },
                            "$stats.delivered",
                            "$stats.read",
                          ],
                        },
                      },
                      read: { $sum: "$stats.read" },
                      replied: { $sum: "$stats.replied" },
                      failed: { $sum: "$stats.failed" },
                      sending: {
                        $sum: {
                          $cond: [
                            { $eq: [{ $toLower: "$status" }, "sending"] },
                            "$recipientCountResolved",
                            0,
                          ],
                        },
                      },
                      processing: {
                        $sum: {
                          $cond: [
                            { $eq: [{ $toLower: "$status" }, "processing"] },
                            1,
                            0,
                          ],
                        },
                      },
                      queued: {
                        $sum: {
                          $cond: [
                            {
                              $in: [
                                { $toLower: "$status" },
                                ["queued", "scheduled"],
                              ],
                            },
                            1,
                            0,
                          ],
                        },
                      },
                      suppressed: { $sum: "$analytics.suppressed" },
                      deferred: { $sum: "$analytics.deferred" },
                      retried: { $sum: "$analytics.retried" },
                      skippedQuietHours: {
                        $sum: "$analytics.skippedQuietHours",
                      },
                    },
                  },
                ],
              },
            },
          ];

          const aggregateResult = await Broadcast.aggregate(summaryPipeline);
          const totals = Array.isArray(aggregateResult?.[0]?.totals)
            ? aggregateResult[0].totals[0] || {}
            : {};

          return {
            success: true,
            data: {
              campaigns: Number(totals?.campaigns || 0) || 0,
              recipientCount: Number(totals?.recipientCount || 0) || 0,
              sent: Number(totals?.sent || 0) || 0,
              delivered: Number(totals?.delivered || 0) || 0,
              read: Number(totals?.read || 0) || 0,
              replied: Number(totals?.replied || 0) || 0,
              failed: Number(totals?.failed || 0) || 0,
              sending: Number(totals?.sending || 0) || 0,
              processing: Number(totals?.processing || 0) || 0,
              queued: Number(totals?.queued || 0) || 0,
              suppressed: Number(totals?.suppressed || 0) || 0,
              deferred: Number(totals?.deferred || 0) || 0,
              retried: Number(totals?.retried || 0) || 0,
              skippedQuietHours:
                Number(totals?.skippedQuietHours || 0) || 0,
            },
          };
        },
      });
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async retryFailedRecipients(broadcastId, broadcaster, credentials = null) {
    try {
      const sourceBroadcast = await Broadcast.findById(broadcastId);
      if (!sourceBroadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      if (
        !["completed", "completed_with_errors", "failed"].includes(
          String(sourceBroadcast.status || "").toLowerCase(),
        )
      ) {
        return {
          success: false,
          error:
            "Retry is allowed only after a completed, partially completed, or failed broadcast",
        };
      }

      const recipientDetails =
        await this.buildRecipientStatusDetails(sourceBroadcast);
      const retryCandidates = this.buildRetryCandidateList(
        sourceBroadcast,
        recipientDetails,
      );
      if (!retryCandidates.length) {
        return {
          success: false,
          error: "No failed recipients available for retry",
        };
      }

      const priorRetryCount = await Broadcast.countDocuments({
        $or: [
          { _id: sourceBroadcast._id },
          { retryOfBroadcastId: sourceBroadcast._id },
        ],
        createdById: sourceBroadcast.createdById,
        companyId: sourceBroadcast.companyId,
      });
      const retryAttempt = Math.max(1, priorRetryCount);

      const retryBroadcast = await Broadcast.create({
        name: `${sourceBroadcast.name} - Retry ${retryAttempt}`,
        companyId: sourceBroadcast.companyId || null,
        messageType: sourceBroadcast.messageType,
        message: sourceBroadcast.message || "",
        templateName: sourceBroadcast.templateName || "",
        templateCategory: sourceBroadcast.templateCategory || "",
        templateContent: sourceBroadcast.templateContent || "",
        language: sourceBroadcast.language || "en_US",
        mediaUrl: sourceBroadcast.mediaUrl || "",
        mediaType: sourceBroadcast.mediaType || "",
        recipients: retryCandidates.map((candidate) => ({
          phone: candidate.phone,
          name: candidate.name,
          variables: candidate.variables,
          attributes: candidate.attributes,
        })),
        retryOfBroadcastId: sourceBroadcast._id,
        retryAttempt,
        deliveryPolicy: this.normalizeDeliveryPolicy(
          sourceBroadcast.deliveryPolicy || {},
        ),
        retryPolicy: this.normalizeRetryPolicy(
          sourceBroadcast.retryPolicy || {},
        ),
        compliancePolicy: this.normalizeCompliancePolicy(
          sourceBroadcast.compliancePolicy || {},
        ),
        analytics: {
          suppressed: 0,
          deferred: 0,
          retried: 0,
          failureCodeBreakdown: {},
        },
        createdBy: sourceBroadcast.createdBy,
        createdById: sourceBroadcast.createdById,
        createdByEmail: sourceBroadcast.createdByEmail,
        authHeaderSnapshot: sourceBroadcast.authHeaderSnapshot,
        credentialsSnapshot: sourceBroadcast.credentialsSnapshot,
      });

      const queueResult = await enqueueBroadcastSend({
        broadcastId: retryBroadcast._id,
        userId: retryBroadcast.createdById,
        companyId: retryBroadcast.companyId || null,
        delayMs: 0,
        reason: "broadcast_retry",
        fallbackProcess: () =>
          this.sendBroadcast(retryBroadcast._id, broadcaster, credentials),
      });
      if (!queueResult?.success) {
        return {
          success: false,
          error: queueResult?.error || "Retry send failed",
        };
      }

      return {
        success: true,
        data: {
          sourceBroadcastId: sourceBroadcast._id,
          retryBroadcastId: retryBroadcast._id,
          retriedRecipients: retryCandidates.length,
          retryAttempt,
          queueJobId: queueResult.data.jobId,
          queueStatus: queueResult.data.status,
        },
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async repairBroadcastTemplateHeaderAndRetry(
    broadcastId,
    { mediaUrl = "", mediaType = "image" } = {},
    broadcaster = null,
    credentials = null,
  ) {
    try {
      const sourceBroadcast = await Broadcast.findById(broadcastId);
      if (!sourceBroadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      const normalizedMediaUrl = String(mediaUrl || "").trim();
      const normalizedMediaType =
        String(mediaType || "image")
          .trim()
          .toLowerCase() || "image";
      if (!normalizedMediaUrl) {
        return {
          success: false,
          error:
            "A valid image header URL is required to repair this broadcast",
        };
      }

      sourceBroadcast.mediaUrl = normalizedMediaUrl;
      sourceBroadcast.mediaType = normalizedMediaType;
      sourceBroadcast.updatedAt = new Date();
      await sourceBroadcast.save();

      const retryResult = await this.retryFailedRecipients(
        broadcastId,
        broadcaster,
        credentials,
      );
      if (!retryResult.success) {
        return retryResult;
      }

      return {
        success: true,
        data: {
          repairedBroadcastId: sourceBroadcast._id,
          mediaUrl: normalizedMediaUrl,
          mediaType: normalizedMediaType,
          ...retryResult.data,
        },
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
        return { success: false, error: "Broadcast not found" };
      }

      // Find all messages sent from this broadcast
      const startTime = new Date(broadcast.startedAt || broadcast.createdAt);
      // Use the current time as the upper bound so late delivery/read webhooks
      // that arrive after completedAt still get counted during a sync.
      const endTime = new Date();

      const broadcastIdQuery = {
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        sender: "agent",
        broadcastId: broadcast._id,
        timestamp: { $gte: startTime, $lte: endTime },
      };

      let messages = await Message.find(broadcastIdQuery);
      const messageIds = new Set(
        messages.map((message) => String(message?._id || "")).filter(Boolean),
      );

      // Supplement with legacy conversation-based lookup so partially linked
      // broadcasts do not undercount sent/delivered/read/failed stats.
      const recipientPhones = (broadcast.recipients || [])
        .map((r) => r.phone || r)
        .map((phone) => String(phone || "").trim())
        .filter(Boolean);
      const recipientCount = Number(broadcast?.recipientCount || 0);
      const hasDeliveryStats = messages.some((msg) => {
        const status = String(msg?.status || "").toLowerCase();
        return ["sent", "delivered", "read", "failed"].includes(status);
      });
      const shouldSupplementLegacyRows =
        recipientPhones.length > 0 &&
        (messages.length === 0 ||
          (recipientCount > 0 && messages.length < recipientCount) ||
          !hasDeliveryStats);

      if (shouldSupplementLegacyRows) {
        const conversations = await Conversation.find({
          userId: broadcast.createdById,
          contactPhone: { $in: recipientPhones },
        });

        const conversationIds = conversations.map((c) => c._id);

        if (conversationIds.length > 0) {
          const convoMessages = await Message.find({
            userId: broadcast.createdById,
            sender: "agent",
            conversationId: { $in: conversationIds },
            timestamp: { $gte: startTime, $lte: endTime },
          });

          for (const message of convoMessages) {
            const messageKey = String(message?._id || "");
            if (!messageKey || messageIds.has(messageKey)) continue;
            messageIds.add(messageKey);
            messages.push(message);
          }
        }

        if (messages.length > 0) {
          await Message.updateMany(
            {
              _id: { $in: messages.map((m) => m._id) },
              broadcastId: { $exists: false },
              conversationId: {
                $in: recipientPhones.length > 0 ? conversationIds : [],
              },
            },
            { $set: { broadcastId: broadcast._id } },
          );
        }

        // Avoid broad text-based legacy fallback queries:
        // they can cross-match unrelated broadcasts and cause stat drops/fluctuations.
      }

      console.log(
        `?? Found ${messages.length} messages for broadcast "${broadcast.name}"`,
      );

      // Count statuses with proper validation
      const stats = {
        sent: messages.length,
        // In WhatsApp, "read" implies message was delivered.
        delivered: messages.filter(
          (msg) => msg.status === "delivered" || msg.status === "read",
        ).length,
        read: messages.filter((msg) => msg.status === "read").length,
        failed: messages.filter((msg) => msg.status === "failed").length,
        replied: 0, // Will be calculated below
      };
      const recipientDetails = await this.buildRecipientStatusDetails(broadcast);
      if (Array.isArray(recipientDetails) && recipientDetails.length > 0) {
        const resolvedStats = recipientDetails.reduce(
          (accumulator, detail) => {
            const status = String(detail?.status || "").toLowerCase();
            if (detail?.sent || ["sent", "delivered", "read", "failed"].includes(status)) {
              accumulator.sent += 1;
            }
            if (detail?.delivered || status === "delivered" || status === "read") {
              accumulator.delivered += 1;
            }
            if (detail?.read || status === "read") {
              accumulator.read += 1;
            }
            if (status === "failed") {
              accumulator.failed += 1;
            }
            if (detail?.replied) {
              accumulator.replied += 1;
            }
            return accumulator;
          },
          {
            sent: 0,
            delivered: 0,
            read: 0,
            failed: 0,
            replied: 0,
          },
        );

        stats.sent = Math.max(stats.sent, resolvedStats.sent);
        stats.delivered = Math.max(stats.delivered, resolvedStats.delivered);
        stats.read = Math.max(stats.read, resolvedStats.read);
        stats.failed = resolvedStats.failed;
        stats.replied = Math.max(stats.replied, resolvedStats.replied);
      }

      const currentStatus = String(broadcast?.status || "").toLowerCase();
      const resolvedFinalStatus = stats.failed > 0 ? "completed_with_errors" : "completed";

      // Debug: Log message statuses to identify issues
      console.log("🔍 DEBUG: Message statuses found:", {
        totalMessages: messages.length,
        statusBreakdown: {
          sent: messages.filter((msg) => msg.status === "sent").length,
          delivered: messages.filter(
            (msg) => msg.status === "delivered" || msg.status === "read",
          ).length,
          read: messages.filter((msg) => msg.status === "read").length,
          failed: messages.filter((msg) => msg.status === "failed").length,
          other: messages.filter(
            (msg) =>
              !["sent", "delivered", "read", "failed"].includes(msg.status),
          ).length,
        },
        messageDetails: messages.map((m) => ({
          id: m._id,
          whatsappId: m.whatsappMessageId,
          status: m.status,
          timestamp: m.timestamp,
        })),
      });

      // Count unique contacts who replied to this broadcast
      const conversationIds = Array.from(
        new Set(messages.map((m) => String(m.conversationId))),
      );
      const replyStartTime =
        messages.length > 0
          ? new Date(
              Math.min(...messages.map((m) => new Date(m.timestamp).getTime())),
            )
          : startTime;

      const replyMessages = await Message.find({
        userId: broadcast.createdById,
        companyId: broadcast.companyId,
        conversationId: { $in: conversationIds },
        sender: "contact",
        timestamp: { $gte: replyStartTime },
      });

      // Count unique conversations that have at least one reply
      const uniqueRepliedConversations = new Set(
        replyMessages.map((msg) => msg.conversationId.toString()),
      );
      stats.replied = uniqueRepliedConversations.size;

      console.log("📊 Message status breakdown:", {
        total: messages.length,
        statuses: messages.map((m) => ({ id: m._id, status: m.status })),
        calculatedStats: stats,
      });

      // Only update broadcast stats if they're different
      const currentStats = broadcast.stats || {};
      const statsChanged =
        currentStats.sent !== stats.sent ||
        currentStats.delivered !== stats.delivered ||
        currentStats.read !== stats.read ||
        currentStats.failed !== stats.failed ||
        currentStats.replied !== stats.replied;
      const statusChanged = currentStatus !== resolvedFinalStatus;
      let updatedBroadcast = null;

      if (statsChanged || statusChanged) {
        // Update only the stats field without touching other fields
        await Broadcast.updateOne(
          { _id: broadcastId },
          {
            $set: {
              stats: stats,
              status: resolvedFinalStatus,
              updatedAt: new Date(),
            },
          },
        );

        // Get updated broadcast with virtual fields
        updatedBroadcast = await Broadcast.findById(broadcastId);
        const repliedPercentage = updatedBroadcast.repliedPercentage;
        const repliedPercentageOfTotal =
          updatedBroadcast.repliedPercentageOfTotal;
        const readPercentage = updatedBroadcast.readPercentage;
        const readPercentageOfTotal = updatedBroadcast.readPercentageOfTotal;
        const deliveryRate = updatedBroadcast.deliveryRate;

        console.log(`📊 Updated stats for broadcast ${broadcast.name}:`, stats);
        console.log(
          `📊 Delivery rate: ${deliveryRate}%, Read rate: ${readPercentage}% of sent, ${readPercentageOfTotal}% of total recipients`,
        );
        console.log(
          `📊 Replied percentage: ${repliedPercentage}% of sent, ${repliedPercentageOfTotal}% of total recipients`,
        );
      } else {
        console.log(`📊 No stat changes for broadcast ${broadcast.name}`);
      }

      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: "broadcast_stats_updated",
        broadcastId: String(broadcastId),
        stats,
        broadcast: updatedBroadcast
          ? updatedBroadcast.toObject
            ? updatedBroadcast.toObject()
            : updatedBroadcast
          : null,
      });

      return {
        success: true,
        data: { broadcast, stats, messagesFound: messages.length },
      };
    } catch (error) {
      console.error("Error syncing broadcast stats:", error);
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
            status: "scheduled",
            scheduledAt: { $lte: now },
          },
          {
            $set: {
              status: "sending",
              startedAt: new Date(),
              updatedAt: new Date(),
            },
          },
          {
            new: true,
            sort: { scheduledAt: 1 },
          },
        ).maxTimeMS(5000);

        if (!claimed) {
          break;
        }

        claimedCount += 1;
        try {
          const queueResult = await enqueueBroadcastSend({
            broadcastId: claimed._id,
            userId: claimed.createdById,
            companyId: claimed.companyId || null,
            delayMs: 0,
            reason: "scheduler",
            fallbackProcess: () => this.sendBroadcast(claimed._id, null, null),
          });
          if (!queueResult?.success) {
            await Broadcast.findByIdAndUpdate(
              claimed._id,
              {
                $set: {
                  status: "failed",
                  queueLastError: queueResult?.error || "Queue failed",
                  completedAt: new Date(),
                },
              },
              { new: false },
            );
            console.error(
              `Scheduled broadcast queue failed: ${claimed.name}: ${queueResult?.error || "Unknown error"}`,
            );
          }
        } catch (error) {
          await Broadcast.findByIdAndUpdate(
            claimed._id,
            {
              $set: {
                status: "failed",
                queueLastError: error.message,
                completedAt: new Date(),
              },
            },
            { new: false },
          );
          console.error(
            `Failed to queue scheduled broadcast ${claimed.name}:`,
            error,
          );
        }
      }

      if (claimedCount === 0) {
        console.log("No scheduled broadcasts ready to send");
      }
    } catch (error) {
      console.error("Error checking scheduled broadcasts:", error);
    }
  }

  async pauseBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      if (broadcast.status !== "scheduled") {
        return {
          success: false,
          error: "Only scheduled broadcasts can be paused",
        };
      }

      broadcast.status = "paused";
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: "broadcast_updated",
        action: "paused",
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
      });

      return { success: true, data: broadcast };
    } catch (error) {
      console.error("Error pausing broadcast:", error);
      return { success: false, error: error.message };
    }
  }

  async resumeBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      if (broadcast.status !== "paused") {
        return {
          success: false,
          error: "Only paused broadcasts can be resumed",
        };
      }

      const now = new Date();
      if (broadcast.scheduledAt && broadcast.scheduledAt < now) {
        broadcast.scheduledAt = now;
      }

      broadcast.status = "scheduled";
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: "broadcast_updated",
        action: "resumed",
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
      });

      return { success: true, data: broadcast };
    } catch (error) {
      console.error("Error resuming broadcast:", error);
      return { success: false, error: error.message };
    }
  }

  async cancelScheduledBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      if (!["scheduled", "paused"].includes(broadcast.status)) {
        return {
          success: false,
          error: "Only scheduled or paused broadcasts can be cancelled",
        };
      }

      broadcast.status = "cancelled";
      await broadcast.save();
      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: "broadcast_updated",
        action: "cancelled",
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
      });

      return { success: true, data: broadcast };
    } catch (error) {
      console.error("Error cancelling broadcast:", error);
      return { success: false, error: error.message };
    }
  }

  async deleteBroadcast(broadcastId, broadcaster = null) {
    try {
      const broadcast = await Broadcast.findById(broadcastId);
      if (!broadcast) {
        return { success: false, error: "Broadcast not found" };
      }

      this.emitBroadcastRealtimeEvent(broadcaster, {
        type: "broadcast_deleted",
        broadcastId: String(broadcastId),
        broadcast: broadcast.toObject ? broadcast.toObject() : broadcast,
      });

      // Delete the broadcast from database
      await Broadcast.findByIdAndDelete(broadcastId);

      return { success: true, message: "Broadcast deleted successfully" };
    } catch (error) {
      console.error("Error deleting broadcast:", error);
      return { success: false, error: error.message };
    }
  }
}

module.exports = new BroadcastService();
