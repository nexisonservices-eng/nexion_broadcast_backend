const toCleanString = (value = '') => String(value || '').trim();

const toSafeDate = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const normalizeWhatsAppOptInStatus = (value, isBlocked = false) => {
  const normalized = toCleanString(value).toLowerCase();
  if (normalized === 'opted_in') return 'opted_in';
  if (normalized === 'opted_out') return 'opted_out';
  return isBlocked ? 'opted_out' : 'unknown';
};

const normalizeOptInScope = (value = '') => {
  const normalized = toCleanString(value).toLowerCase();
  if (normalized === 'marketing' || normalized === 'service' || normalized === 'both') {
    return normalized;
  }
  return 'unknown';
};

const marketingScopeAllowed = (scope = '') => {
  const normalized = normalizeOptInScope(scope);
  return normalized === 'marketing' || normalized === 'both';
};

const getMarketingRateLimitConfig = () => {
  const rawMax = Number(process.env.WHATSAPP_MARKETING_TEMPLATE_MAX_PER_24H || 1);
  const rawWindow = Number(process.env.WHATSAPP_MARKETING_TEMPLATE_WINDOW_HOURS || 24);
  return {
    max: Number.isFinite(rawMax) && rawMax > 0 ? Math.floor(rawMax) : 1,
    windowHours: Number.isFinite(rawWindow) && rawWindow > 0 ? rawWindow : 24
  };
};

const getMarketingRateLimitState = (contact = {}, { now = new Date(), max, windowHours } = {}) => {
  const config = getMarketingRateLimitConfig();
  const limit = Number.isFinite(max) && max > 0 ? Math.floor(max) : config.max;
  const windowHrs =
    Number.isFinite(windowHours) && windowHours > 0 ? windowHours : config.windowHours;
  const windowMs = windowHrs * 60 * 60 * 1000;

  const windowStart = toSafeDate(
    contact?.whatsappMarketingWindowStartedAt || contact?.whatsappMarketingLastSentAt
  );
  const sentCount = Number(contact?.whatsappMarketingSendCount || 0) || 0;

  if (!windowStart || Number.isNaN(windowStart.getTime())) {
    return {
      limited: false,
      remaining: limit,
      count: 0,
      windowStartAt: null,
      nextAllowedAt: null
    };
  }

  const expiresAt = new Date(windowStart.getTime() + windowMs);
  if (expiresAt.getTime() <= now.getTime()) {
    return {
      limited: false,
      remaining: limit,
      count: 0,
      windowStartAt: null,
      nextAllowedAt: null
    };
  }

  const remaining = Math.max(limit - sentCount, 0);
  const limited = remaining <= 0;

  return {
    limited,
    remaining,
    count: sentCount,
    windowStartAt: windowStart,
    nextAllowedAt: limited ? expiresAt : null
  };
};

const applyMarketingTemplateSent = (contact = {}, { now = new Date() } = {}) => {
  if (!contact || typeof contact !== 'object') return contact;
  const { windowHours } = getMarketingRateLimitConfig();
  const windowMs = windowHours * 60 * 60 * 1000;
  const windowStart = toSafeDate(contact?.whatsappMarketingWindowStartedAt);
  const hasActiveWindow =
    windowStart && !Number.isNaN(windowStart.getTime()) &&
    now.getTime() - windowStart.getTime() < windowMs;

  if (!hasActiveWindow) {
    contact.whatsappMarketingWindowStartedAt = now;
    contact.whatsappMarketingSendCount = 1;
  } else {
    const current = Number(contact.whatsappMarketingSendCount || 0) || 0;
    contact.whatsappMarketingSendCount = current + 1;
  }

  contact.whatsappMarketingLastSentAt = now;
  return contact;
};

const detectWhatsAppOptOutKeyword = (text = '') => {
  const normalized = toCleanString(text).toLowerCase().replace(/\s+/g, ' ');
  if (!normalized) return false;

  return new Set([
    'stop',
    'unsubscribe',
    'cancel',
    'remove',
    'opt out',
    'optout',
    'end',
    'quit',
    'no messages'
  ]).has(normalized);
};

const getWhatsAppMessagingPolicy = (contact = {}, options = {}) => {
  const now = options.now instanceof Date ? options.now : new Date();
  const normalizedOptInStatus = normalizeWhatsAppOptInStatus(
    contact?.whatsappOptInStatus,
    contact?.isBlocked
  );
  const normalizedOptInScope = normalizeOptInScope(contact?.whatsappOptInScope);
  const serviceWindowClosesAt = toSafeDate(contact?.serviceWindowClosesAt);
  const serviceWindowOpen = Boolean(
    serviceWindowClosesAt && serviceWindowClosesAt.getTime() > now.getTime()
  );
  const optedOut = normalizedOptInStatus === 'opted_out';
  const freeformAllowed = serviceWindowOpen && !optedOut;
  const templateOnly = !optedOut && !freeformAllowed;
  const templateCategory = toCleanString(options?.templateCategory).toLowerCase();
  const marketingTemplateAllowed =
    !optedOut && normalizedOptInStatus === 'opted_in' && marketingScopeAllowed(normalizedOptInScope);
  const marketingRateState = getMarketingRateLimitState(contact, {
    now,
    max: options?.marketingLimit,
    windowHours: options?.marketingWindowHours
  });
  const templateAllowed =
    !optedOut &&
    (templateCategory !== 'marketing' || marketingTemplateAllowed);

  let statusLabel = 'Template Only';
  if (optedOut) {
    statusLabel = 'Opted Out';
  } else if (freeformAllowed) {
    statusLabel = '24h Open';
  }

  return {
    normalizedOptInStatus,
    normalizedOptInScope,
    serviceWindowClosesAt,
    serviceWindowOpen,
    freeformAllowed,
    templateOnly,
    optedOut,
    templateAllowed,
    marketingTemplateAllowed,
    marketingRateLimited: marketingRateState.limited,
    marketingRateRemaining: marketingRateState.remaining,
    marketingNextAllowedAt: marketingRateState.nextAllowedAt,
    statusLabel
  };
};

const applyContactOptIn = (contact, { source = 'manual' } = {}) => {
  if (!contact || typeof contact !== 'object') return contact;
  contact.whatsappOptInStatus = 'opted_in';
  contact.whatsappOptInAt = new Date();
  contact.whatsappOptInSource = toCleanString(source) || 'manual';
  contact.whatsappOptOutAt = null;
  contact.isBlocked = false;
  return contact;
};

const applyContactOptOut = (contact, { source = 'manual' } = {}) => {
  if (!contact || typeof contact !== 'object') return contact;
  contact.whatsappOptInStatus = 'opted_out';
  contact.whatsappOptOutAt = new Date();
  if (!toCleanString(contact.whatsappOptInSource)) {
    contact.whatsappOptInSource = toCleanString(source) || 'manual';
  }
  contact.isBlocked = true;
  return contact;
};

const validateFreeformOutboundSend = (contact = {}) => {
  const policy = getWhatsAppMessagingPolicy(contact);
  if (policy.optedOut) {
    return {
      ok: false,
      policy,
      statusCode: 403,
      error: 'This contact has opted out of WhatsApp outreach.'
    };
  }

  if (!policy.freeformAllowed) {
    return {
      ok: false,
      policy,
      statusCode: 403,
      error:
        "Free-form WhatsApp messages are only allowed within 24 hours of the customer's last reply. Use an approved template instead."
    };
  }

  return { ok: true, policy };
};

const validateTemplateOutboundSend = (contact = {}, { templateCategory = '' } = {}) => {
  const normalizedCategory = toCleanString(templateCategory).toLowerCase();
  const policy = getWhatsAppMessagingPolicy(contact, {
    templateCategory: normalizedCategory
  });

  if (policy.optedOut) {
    return {
      ok: false,
      policy,
      statusCode: 403,
      error: 'This contact has opted out of WhatsApp outreach.'
    };
  }

  if (normalizedCategory === 'marketing' && !policy.marketingTemplateAllowed) {
    return {
      ok: false,
      policy,
      statusCode: 403,
      error:
        'Marketing template messages require a valid WhatsApp opt-in for this contact.'
    };
  }

  if (normalizedCategory === 'marketing' && policy.marketingRateLimited) {
    return {
      ok: false,
      policy,
      statusCode: 429,
      error:
        'Marketing template limit reached for this contact. Try again after the cooldown window.'
    };
  }

  return { ok: true, policy };
};

const buildBroadcastAudienceValidation = ({
  recipients = [],
  contactsByPhone = new Map(),
  messageType = 'template',
  templateCategory = ''
} = {}) => {
  const normalizedMessageType = toCleanString(messageType).toLowerCase();
  const normalizedTemplateCategory = toCleanString(templateCategory).toLowerCase();
  const eligibleRecipients = [];
  const invalidRecipients = [];
  const summary = {
    eligible: 0,
    invalid: 0,
    missingContact: 0,
    optedOut: 0,
    freeformWindowClosed: 0,
    missingMarketingOptIn: 0,
    marketingRateLimited: 0,
    invalidPhone: 0
  };

  for (const recipient of Array.isArray(recipients) ? recipients : []) {
    const phone = toCleanString(recipient?.phone);
    if (!phone) {
      summary.invalid += 1;
      summary.invalidPhone += 1;
      invalidRecipients.push({
        recipient,
        phone: '',
        reason: 'invalid_phone',
        error: 'Recipient phone number is missing.'
      });
      continue;
    }

    const matchedContact = contactsByPhone.get(phone) || null;
    if (!matchedContact) {
      summary.invalid += 1;
      summary.missingContact += 1;
      invalidRecipients.push({
        recipient,
        phone,
        reason: 'missing_contact',
        error: 'Contact record not found for this phone number.'
      });
      continue;
    }

    const validation =
      normalizedMessageType === 'template'
        ? validateTemplateOutboundSend(matchedContact, {
            templateCategory: normalizedTemplateCategory
          })
        : validateFreeformOutboundSend(matchedContact);

    if (!validation.ok) {
      summary.invalid += 1;
      if (validation.policy?.optedOut) {
        summary.optedOut += 1;
      } else if (normalizedMessageType === 'template') {
        if (validation.policy?.marketingRateLimited) {
          summary.marketingRateLimited += 1;
        } else {
          summary.missingMarketingOptIn += 1;
        }
      } else {
        summary.freeformWindowClosed += 1;
      }

      invalidRecipients.push({
        recipient,
        phone,
        contactId: matchedContact?._id || null,
        reason:
          validation.policy?.optedOut
            ? 'opted_out'
            : normalizedMessageType === 'template'
              ? validation.policy?.marketingRateLimited
                ? 'marketing_rate_limited'
                : 'missing_marketing_opt_in'
              : 'freeform_window_closed',
        error: validation.error
      });
      continue;
    }

    summary.eligible += 1;
    eligibleRecipients.push({
      ...recipient,
      phone,
      contactId: matchedContact?._id || null
    });
  }

  return {
    eligibleRecipients,
    invalidRecipients,
    summary
  };
};

module.exports = {
  toCleanString,
  normalizeWhatsAppOptInStatus,
  detectWhatsAppOptOutKeyword,
  getWhatsAppMessagingPolicy,
  applyContactOptIn,
  applyContactOptOut,
  validateFreeformOutboundSend,
  validateTemplateOutboundSend,
  normalizeOptInScope,
  marketingScopeAllowed,
  getMarketingRateLimitState,
  applyMarketingTemplateSent,
  buildBroadcastAudienceValidation
};
