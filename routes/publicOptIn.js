const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const express = require('express');
const mongoose = require('mongoose');
const Contact = require('../models/Contact');
const {
  applyContactOptIn,
  toCleanString
} = require('../services/whatsappOutreach/policy');
const { normalizePhoneDigits } = require('../services/whatsappOutreach/conversationResolver');
const { logConsentEvent } = require('../services/whatsappConsentLogService');

const router = express.Router();
const publicOptInRateWindowMs = Math.max(
  Number(process.env.PUBLIC_OPTIN_RATE_WINDOW_MS || 60_000),
  10_000
);
const publicOptInRateLimit = Math.max(Number(process.env.PUBLIC_OPTIN_RATE_LIMIT || 30), 5);
const publicOptInRateState = new Map();
const envFilePath = path.resolve(__dirname, '..', '.env');

const normalizeScope = (value = '') => {
  const normalized = toCleanString(value).toLowerCase();
  if (['marketing', 'service', 'both'].includes(normalized)) return normalized;
  return 'marketing';
};

const getRequestIp = (req) =>
  toCleanString(
    req.headers['x-forwarded-for']?.split(',')?.[0] ||
      req.ip ||
      req.socket?.remoteAddress
  );

const getRequestOrigin = (req) =>
  toCleanString(req.headers.origin || req.headers.referer);

const getRequestHost = (req) =>
  toCleanString(req.headers.host || req.hostname);

const isLocalDevOrigin = (value = '') => {
  try {
    const parsed = new URL(String(value || '').trim());
    return /^(localhost|127\.0\.0\.1)$/i.test(parsed.hostname);
  } catch {
    return false;
  }
};

const isLocalHostValue = (value = '') =>
  /(^|:\/\/)(localhost|127\.0\.0\.1)(:\d+)?(\/|$)/i.test(String(value || '').trim());

const isValidObjectId = (value = '') => mongoose.Types.ObjectId.isValid(String(value || '').trim());
const normalizeOptionalCompanyId = (value = '') => {
  const cleaned = toCleanString(value);
  if (!cleaned) return null;
  return isValidObjectId(cleaned) ? cleaned : null;
};

const getRateLimitKey = (req) => {
  const ip = getRequestIp(req) || 'unknown-ip';
  const userId = toCleanString(req.body?.userId) || 'unknown-user';
  return `${ip}::${userId}`;
};

const isRateLimited = (req) => {
  const key = getRateLimitKey(req);
  const now = Date.now();
  const state = publicOptInRateState.get(key);

  if (!state || now > state.resetAt) {
    publicOptInRateState.set(key, { count: 1, resetAt: now + publicOptInRateWindowMs });
    return false;
  }

  if (state.count >= publicOptInRateLimit) {
    return true;
  }

  state.count += 1;
  publicOptInRateState.set(key, state);
  return false;
};

const splitCandidateKeys = (value = '') =>
  String(value || '')
    .split(',')
    .map((item) => toCleanString(item))
    .filter(Boolean);

const readEnvFilePublicKeys = () => {
  try {
    if (!fs.existsSync(envFilePath)) return [];
    const parsed = dotenv.parse(fs.readFileSync(envFilePath));
    return splitCandidateKeys(parsed.WHATSAPP_OPTIN_PUBLIC_KEY);
  } catch {
    return [];
  }
};

const getConfiguredPublicKeys = () => {
  const keys = new Set([
    ...splitCandidateKeys(process.env.WHATSAPP_OPTIN_PUBLIC_KEY),
    ...readEnvFilePublicKeys()
  ]);

  return Array.from(keys);
};

const shouldAllowLocalDevOptIn = (req, configuredPublicKeys, requestPublicKey) => {
  if (String(process.env.NODE_ENV || '').trim().toLowerCase() === 'production') {
    return false;
  }

  if (!configuredPublicKeys.length) return false;

  const origin = getRequestOrigin(req);
  const referer = toCleanString(req.headers.referer);
  const localRequest =
    isLocalDevOrigin(origin) ||
    isLocalHostValue(origin) ||
    isLocalHostValue(referer);

  if (!localRequest) return false;

  const userId = toCleanString(req.body?.userId);
  const phone = normalizePhoneDigits(req.body?.phone);
  if (!userId || !phone) return false;

  return !requestPublicKey || !configuredPublicKeys.includes(requestPublicKey);
};

const buildPublicOptInPayload = (req) => {
  const body = req.body || {};
  const metadata =
    body.metadata && typeof body.metadata === 'object' && !Array.isArray(body.metadata)
      ? body.metadata
      : {};

  return {
    phone: normalizePhoneDigits(body.phone),
    name: toCleanString(body.name),
    email: toCleanString(body.email),
    source: toCleanString(body.source) || 'website_form',
    scope: normalizeScope(body.scope),
    consentText: toCleanString(body.consentText),
    pageUrl: toCleanString(body.pageUrl),
    proofType: toCleanString(body.proofType) || 'website_form',
    proofId: toCleanString(body.proofId),
    proofUrl: toCleanString(body.proofUrl),
    companyId: normalizeOptionalCompanyId(body.companyId),
    userId: toCleanString(body.userId),
    tags: Array.isArray(body.tags)
      ? body.tags.map((tag) => toCleanString(tag)).filter(Boolean)
      : [],
    capturedBy: toCleanString(body.capturedBy) || 'public_opt_in_form',
    userAgent: toCleanString(req.headers['user-agent']),
    ip: getRequestIp(req),
    metadata: {
      ...metadata,
      referrer: toCleanString(body.referrer || req.headers.referer),
      submittedAt: new Date().toISOString()
    }
  };
};

router.post('/whatsapp-opt-in', async (req, res) => {
  try {
    if (isRateLimited(req)) {
      return res.status(429).json({
        success: false,
        error: 'Too many opt-in requests. Please retry in a minute.'
      });
    }

    const configuredPublicKeys = getConfiguredPublicKeys();
    if (!configuredPublicKeys.length) {
      return res.status(503).json({
        success: false,
        error: 'Public WhatsApp opt-in is not configured on this server.'
      });
    }

    const requestPublicKey = toCleanString(
      req.headers['x-opt-in-public-key'] || req.body?.publicKey
    );
    const isValidRequestKey =
      requestPublicKey && configuredPublicKeys.includes(requestPublicKey);

    if (!isValidRequestKey && !shouldAllowLocalDevOptIn(req, configuredPublicKeys, requestPublicKey)) {
      return res.status(401).json({
        success: false,
        error: 'Invalid public opt-in key.'
      });
    }

    const payload = buildPublicOptInPayload(req);
    const consentChecked = req.body?.consentChecked === true || req.body?.consentChecked === 'true';

    if (!payload.userId) {
      return res.status(400).json({
        success: false,
        error: 'userId is required for public opt-in.'
      });
    }

    if (!payload.phone) {
      return res.status(400).json({
        success: false,
        error: 'phone is required for public opt-in.'
      });
    }

    if (!consentChecked) {
      return res.status(400).json({
        success: false,
        error: 'Explicit consent is required before WhatsApp opt-in can be saved.'
      });
    }

    if (!payload.consentText) {
      return res.status(400).json({
        success: false,
        error: 'consentText is required for public opt-in.'
      });
    }

    const scopedFilter = {
      userId: payload.userId,
      phone: payload.phone
    };

    if (payload.companyId) {
      scopedFilter.$or = [
        { companyId: payload.companyId },
        { companyId: null },
        { companyId: { $exists: false } }
      ];
    }

    let contact = await Contact.findOne(scopedFilter);

    if (!contact) {
      contact = new Contact({
        userId: payload.userId,
        companyId: payload.companyId || null,
        phone: payload.phone,
        name: payload.name || payload.phone,
        email: payload.email || '',
        tags: payload.tags,
        sourceType: 'public_opt_in'
      });
    } else {
      if (payload.name && !contact.name) {
        contact.name = payload.name;
      }
      if (payload.email && !contact.email) {
        contact.email = payload.email;
      }
      if (payload.tags.length) {
        const mergedTags = new Set([...(contact.tags || []), ...payload.tags]);
        contact.tags = Array.from(mergedTags);
      }
    }

    applyContactOptIn(contact, { source: payload.source });
    contact.isBlocked = false;
    contact.whatsappOptInSource = payload.source;
    contact.whatsappOptInScope = payload.scope;
    contact.whatsappOptInTextSnapshot = payload.consentText;
    contact.whatsappOptInProofType = payload.proofType;
    contact.whatsappOptInProofId = payload.proofId;
    contact.whatsappOptInProofUrl = payload.proofUrl;
    contact.whatsappOptInCapturedBy = payload.capturedBy;
    contact.whatsappOptInPageUrl = payload.pageUrl;
    contact.whatsappOptInIp = payload.ip;
    contact.whatsappOptInUserAgent = payload.userAgent;
    contact.whatsappOptInMetadata = payload.metadata;

    await contact.save();
    await logConsentEvent({
      contact,
      action: 'opt_in',
      payload: {
        source: payload.source,
        scope: payload.scope,
        consentText: payload.consentText,
        proofType: payload.proofType,
        proofId: payload.proofId,
        proofUrl: payload.proofUrl,
        capturedBy: payload.capturedBy,
        pageUrl: payload.pageUrl,
        ip: payload.ip,
        userAgent: payload.userAgent,
        metadata: payload.metadata
      }
    });

    return res.status(201).json({
      success: true,
      data: {
        contactId: contact._id,
        phone: contact.phone,
        whatsappOptInStatus: contact.whatsappOptInStatus,
        whatsappOptInAt: contact.whatsappOptInAt,
        whatsappOptInSource: contact.whatsappOptInSource
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

module.exports = router;
