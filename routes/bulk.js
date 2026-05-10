const express = require('express');
const auth = require('../middleware/auth');
const requirePlanFeature = require('../middleware/planGuard');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const broadcastService = require('../services/broadcastService');
const { enqueueBroadcastSend } = require('../queues/broadcastQueue');
const Contact = require('../models/Contact');
const {
  buildPhoneCandidates: buildPhoneCandidatesFromResolver,
  buildPhoneLookupFilters
} = require('../services/whatsappOutreach/conversationResolver');
const {
  buildBroadcastAudienceValidation,
  toCleanString
} = require('../services/whatsappOutreach/policy');

const router = express.Router();
router.use(auth);

const buildPhoneCandidates = typeof buildPhoneCandidatesFromResolver === 'function'
  ? buildPhoneCandidatesFromResolver
  : (value = '') => {
      const rawValue = String(value || '').trim();
      const normalizedPhone = rawValue.replace(/\D/g, '');

      return Array.from(
        new Set(
          [
            rawValue,
            normalizedPhone,
            normalizedPhone ? `+${normalizedPhone}` : '',
            normalizedPhone.length > 10 ? normalizedPhone.slice(-10) : ''
          ].filter(Boolean)
        )
      );
    };

const buildScopedContactFilter = (req, extra = {}) => {
  const scopedConditions = [{ userId: req.user.id }];
  if (req.companyId) {
    scopedConditions.push({ companyId: req.companyId });
  }
  if (extra && Object.keys(extra).length > 0) {
    scopedConditions.push(extra);
  }

  return scopedConditions.length === 1 ? scopedConditions[0] : { $and: scopedConditions };
};

const buildContactsByPhoneMap = async (req, recipients = []) => {
  try {
    const phoneFilters = Array.from(
      new Set(
        (Array.isArray(recipients) ? recipients : [])
          .map((recipient) => String(recipient?.phone || '').trim())
          .filter(Boolean)
      )
    )
      .map((phone) => buildPhoneLookupFilters(phone))
      .filter(Boolean);

    if (!phoneFilters.length) {
      return new Map();
    }

    const contacts = await Contact.find(
      buildScopedContactFilter(req, phoneFilters.length === 1 ? phoneFilters[0] : { $or: phoneFilters })
    )
      .select(
        '_id phone isBlocked sourceType whatsappOptInStatus whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata whatsappOptOutAt serviceWindowClosesAt lastInboundMessageAt'
      )
      .lean();

    const contactsByPhone = new Map();
    for (const contact of contacts) {
      for (const candidate of buildPhoneCandidates(contact?.phone || '')) {
        if (!contactsByPhone.has(candidate)) {
          contactsByPhone.set(candidate, contact);
        }
      }
    }

    return contactsByPhone;
  } catch (error) {
    console.error('[bulk] phone lookup failed, continuing without contact enrichment:', {
      userId: req?.user?.id || null,
      companyId: req?.companyId || null,
      error: error?.message || error
    });
    return new Map();
  }
};

router.post('/validate-audience', requirePlanFeature('broadcastMessaging'), async (req, res) => {
  try {
    const recipients = Array.isArray(req.body?.recipients) ? req.body.recipients : [];
    const messageType = toCleanString(req.body?.messageType || req.body?.message_type || 'template');
    const templateCategory = toCleanString(req.body?.templateCategory || '') || 'utility';

    if (!recipients.length) {
      return res.status(400).json({
        success: false,
        error: 'Recipients are required for validation'
      });
    }

    const contactsByPhone = await buildContactsByPhoneMap(req, recipients);
    const validation = buildBroadcastAudienceValidation({
      recipients,
      contactsByPhone,
      messageType,
      templateCategory
    });

    return res.json({
      success: true,
      data: {
        ...validation,
        canProceed: recipients.length > 0
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message || 'Failed to validate audience'
    });
  }
});

router.post(
  '/send',
  requirePlanFeature('broadcastMessaging'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      const {
        broadcast_name,
        name,
        messageType = 'text',
        message_type,
        templateName,
        template_name,
        templateCategory = '',
        templateContent = '',
        template_content = '',
        language = 'en_US',
        customMessage = '',
        custom_message = '',
        recipients = [],
        deliveryPolicy = {},
        retryPolicy = {},
        compliancePolicy = {},
        scheduledAt = null
      } = req.body || {};

      const resolvedRecipients = Array.isArray(recipients) ? recipients : [];
      if (!resolvedRecipients.length) {
        return res.status(400).json({
          success: false,
          error: 'Recipients data is required'
        });
      }

      const created = await broadcastService.createBroadcast({
        name: String(broadcast_name || name || `Bulk Send - ${new Date().toISOString()}`).trim(),
        companyId: req.companyId || null,
        messageType: String(message_type || messageType || 'text').trim() || 'text',
        message: String(custom_message || customMessage || '').trim(),
        templateName: String(template_name || templateName || '').trim() || null,
        templateCategory: String(templateCategory || '').trim().toLowerCase() || 'utility',
        templateContent: String(template_content || templateContent || '').trim(),
        language: String(language || 'en_US').trim() || 'en_US',
        recipients: resolvedRecipients,
        recipientCount: resolvedRecipients.length,
        audienceSource: req.body?.audienceSource && typeof req.body.audienceSource === 'object'
          ? req.body.audienceSource
          : {},
        audienceSnapshot: req.body?.audienceSnapshot && typeof req.body.audienceSnapshot === 'object'
          ? req.body.audienceSnapshot
          : {},
        createdBy: req.user.username || req.user.email || req.user.id,
        createdByEmail: req.user.email,
        createdById: req.user.id,
        deliveryPolicy,
        retryPolicy,
        compliancePolicy,
        scheduledAt: scheduledAt || undefined
      });

      if (!created?.success || !created?.data?._id) {
        return res.status(400).json({
          success: false,
          error: created?.error || 'Failed to create broadcast'
        });
      }

      const queueResult = await enqueueBroadcastSend({
        broadcastId: created.data._id,
        userId: req.user.id,
        companyId: req.companyId || null,
        delayMs: 0,
        reason: 'bulk_send',
        fallbackProcess: () => broadcastService.sendBroadcast(created.data._id, null, req.whatsappCredentials || null)
      });

      if (!queueResult?.success) {
        return res.status(400).json(queueResult);
      }

      return res.status(202).json({
        success: true,
        queued: true,
        broadcastId: created.data._id,
        jobId: queueResult.data.jobId,
        total_sent: resolvedRecipients.length,
        successful: 0,
        failed: 0,
        results: [],
        message: 'Broadcast queued. Sending will continue in the background.'
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message
      });
    }
  }
);

module.exports = router;
