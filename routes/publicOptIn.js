const express = require('express');
const Contact = require('../models/Contact');
const {
  applyContactOptIn,
  toCleanString
} = require('../services/whatsappOutreach/policy');
const { normalizePhoneDigits } = require('../services/whatsappOutreach/conversationResolver');

const router = express.Router();

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
    companyId: toCleanString(body.companyId) || null,
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
    const configuredPublicKey = toCleanString(process.env.WHATSAPP_OPTIN_PUBLIC_KEY);
    if (!configuredPublicKey) {
      return res.status(503).json({
        success: false,
        error: 'Public WhatsApp opt-in is not configured on this server.'
      });
    }

    const requestPublicKey = toCleanString(
      req.headers['x-opt-in-public-key'] || req.body?.publicKey
    );
    if (!requestPublicKey || requestPublicKey !== configuredPublicKey) {
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
