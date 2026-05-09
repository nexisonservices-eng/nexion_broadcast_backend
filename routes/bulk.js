const express = require('express');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const requirePlanFeature = require('../middleware/planGuard');
const broadcastService = require('../services/broadcastService');
const { enqueueBroadcastSend } = require('../queues/broadcastQueue');
const Contact = require('../models/Contact');
const { buildPhoneCandidates } = require('../services/whatsappOutreach/conversationResolver');
const {
  buildBroadcastAudienceValidation,
  toCleanString
} = require('../services/whatsappOutreach/policy');

const router = express.Router();
router.use(auth);

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
  const allPhoneCandidates = Array.from(
    new Set(
      (Array.isArray(recipients) ? recipients : [])
        .flatMap((recipient) => buildPhoneCandidates(recipient?.phone || ''))
        .filter(Boolean)
    )
  );

  if (!allPhoneCandidates.length) {
    return new Map();
  }

  const contacts = await Contact.find(
    buildScopedContactFilter(req, { phone: { $in: allPhoneCandidates } })
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
};

const parseCsvLine = (line = '') => {
  const values = [];
  let current = '';
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];

    if (char === '"') {
      const isEscapedQuote = line[index + 1] === '"';
      if (isEscapedQuote) {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      values.push(current.trim());
      current = '';
      continue;
    }

    current += char;
  }

  values.push(current.trim());
  return values;
};

const normalizeHeaderKey = (value = '') =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ');

const findPhoneColumnIndex = (headers = []) => {
  if (!Array.isArray(headers) || headers.length === 0) return 0;

  const preferredPatterns = [
    /whatsapp\s*number/,
    /\bphone\s*number\b/,
    /\bmobile\s*number\b/,
    /\bwhatsapp\b/,
    /\bphone\b/,
    /\bmobile\b/,
    /\bmsisdn\b/,
    /\bnumber\b/
  ];

  for (const pattern of preferredPatterns) {
    const matchIndex = headers.findIndex((header) => pattern.test(normalizeHeaderKey(header)));
    if (matchIndex >= 0) return matchIndex;
  }

  return 0;
};

const isTemplateVariableHeader = (header = '') => /^var\d+$/i.test(String(header || '').trim());

const parseCSV = async (csvData, hasHeaders) => {
  const results = [];
  const lines = String(csvData || '')
    .split('\n')
    .map((line) => line.replace(/\r$/, ''))
    .filter((line) => line.trim());

  if (!lines.length) {
    return results;
  }

  if (hasHeaders) {
    const headers = parseCsvLine(lines[0]);
    const phoneColumnIndex = findPhoneColumnIndex(headers);
    const phoneColumn = headers[phoneColumnIndex] || headers[0];
    const dataLines = lines.slice(1);

    dataLines.forEach((line) => {
      const values = parseCsvLine(line);
      const row = {};
      headers.forEach((header, headerIndex) => {
        row[header] = values[headerIndex] || '';
      });

      const phone = String(row[phoneColumn] || '').trim();
      if (!phone) return;

      const variables = [];
      headers.forEach((header, headerIndex) => {
        if (headerIndex === phoneColumnIndex) return;
        if (!isTemplateVariableHeader(header)) return;
        const value = row[header];
        if (value !== undefined && value !== null && String(value).trim()) {
          variables.push(String(value));
        }
      });

      results.push({
        phone,
        variables,
        data: row
      });
    });
    return results;
  }

  lines.forEach((line) => {
    const values = parseCsvLine(line);
    const phone = String(values[0] || '').trim();
    if (!phone) return;

    results.push({
      phone,
      variables: [],
      data: { phone }
    });
  });

  return results;
};

router.post('/upload', async (req, res) => {
  try {
    const { csvData } = req.body;
    if (!csvData) {
      return res.status(400).json({
        success: false,
        message: 'CSV data is required'
      });
    }

    const csvText = Buffer.from(csvData, 'base64').toString('utf8');
    const lines = csvText.split('\n').filter((line) => line.trim());
    const firstRow = lines[0] || '';
    const hasHeaders = /[a-zA-Z]/.test(firstRow) && !/^[\d+\-\s()]+$/.test(firstRow);
    const recipients = await parseCSV(csvText, hasHeaders);

    if (!recipients.length) {
      return res.status(400).json({
        success: false,
        message: 'No valid recipients found in CSV'
      });
    }

    return res.json({
      success: true,
      recipients,
      count: recipients.length,
      hasHeaders,
      csvData: recipients.map((recipient) => ({
        phone: recipient.phone,
        variables: recipient.variables,
        fullData: recipient.data
      }))
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

router.post(
  '/validate-audience',
  requirePlanFeature('broadcastMessaging'),
  async (req, res) => {
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
  }
);

router.post(
  '/send',
  requirePlanFeature('broadcastMessaging'),
  requireWhatsAppCredentials,
  async (req, res) => {
    try {
      const {
        message_type,
        template_name,
        language,
        custom_message,
        broadcast_name,
        recipients,
        messageType,
        customMessage,
        templateName,
        templateContent,
        templateCategory = '',
        mediaUrl = '',
        mediaType = '',
        deliveryPolicy,
        retryPolicy,
        compliancePolicy
      } = req.body;

      const msgType = message_type || messageType || (templateName ? 'template' : 'text');
      const normalizedTemplateCategory = toCleanString(templateCategory) || 'utility';
      const rawTemplateName = template_name || templateName;
      const finalTemplateName = rawTemplateName ? String(rawTemplateName).trim().toLowerCase() : '';
      const customMsg = custom_message || customMessage || '';

      let parsedRecipients = [];
      if (Array.isArray(recipients) && recipients.length > 0) {
        parsedRecipients = recipients;
      } else if (req.files && req.files.csv_file) {
        const csvFile = req.files.csv_file;
        const csvData = csvFile.data.toString('utf8');
        const lines = csvData.split('\n').filter((line) => line.trim());
        const firstRow = lines[0] || '';
        const hasHeaders = /[a-zA-Z]/.test(firstRow) && !/^[\d+\-\s()]+$/.test(firstRow);
        parsedRecipients = await parseCSV(csvData, hasHeaders);
      } else {
        return res.status(400).json({
          success: false,
          message: 'Recipients data or CSV file is required'
        });
      }

      if (!parsedRecipients.length) {
        return res.status(400).json({
          success: false,
          message: 'No valid recipients found'
        });
      }

      const contactsByPhone = await buildContactsByPhoneMap(req, parsedRecipients);
      const audienceValidation = buildBroadcastAudienceValidation({
        recipients: parsedRecipients,
        contactsByPhone,
        messageType: msgType,
        templateCategory: normalizedTemplateCategory
      });

      const broadcastRecipients = parsedRecipients.map((recipient) => {
        const fullData = recipient?.data || recipient?.fullData || {};
        return {
          phone: String(recipient?.phone || '').trim(),
          name: String(recipient?.name || '').trim(),
          contactId: String(recipient?.contactId || '').trim() || null,
          sourceType: String(recipient?.sourceType || fullData?.sourceType || '').trim() || null,
          variables: Array.isArray(recipient?.variables) ? recipient.variables : [],
          attributes: fullData && typeof fullData === 'object' ? fullData : {}
        };
      });
      const hasContactsAudience = broadcastRecipients.some((recipient) => Boolean(String(recipient?.contactId || '').trim()));
      const defaultAudienceMode = hasContactsAudience ? 'contacts' : 'csv';
      const defaultAudienceLabel = hasContactsAudience ? 'Selected CRM contacts' : 'CSV upload';
      const defaultAudienceType = hasContactsAudience ? 'contacts' : 'csv';
      const selectedContactIds = broadcastRecipients
        .map((recipient) => String(recipient?.contactId || '').trim())
        .filter(Boolean);

      const created = await broadcastService.createBroadcast({
        name: broadcast_name || `Bulk Send - ${new Date().toISOString()}`,
        companyId: req.companyId || null,
        messageType: msgType,
        message: customMsg,
        templateName: finalTemplateName || null,
        templateCategory: normalizedTemplateCategory,
        templateContent: String(templateContent || '').trim(),
        mediaUrl: String(mediaUrl || '').trim(),
        mediaType: String(mediaType || '').trim(),
        language: language || 'en_US',
        recipients: broadcastRecipients,
        audienceSource:
          req.body?.audienceSource && typeof req.body.audienceSource === 'object'
            ? req.body.audienceSource
            : {
                mode: defaultAudienceMode,
                label: defaultAudienceLabel,
                type: defaultAudienceType,
                segmentId: '',
                sourceName: hasContactsAudience ? 'crm_contacts' : 'csv_upload',
                uploadedFileName: String(req.body?.audienceSource?.uploadedFileName || '').trim(),
                recipientCount: broadcastRecipients.length,
                selectedContactCount: selectedContactIds.length,
                hasContactIds: selectedContactIds.length > 0
              },
        audienceSnapshot:
          req.body?.audienceSnapshot && typeof req.body.audienceSnapshot === 'object'
            ? req.body.audienceSnapshot
            : {
                mode: defaultAudienceMode,
                label: defaultAudienceLabel,
                sourceType: defaultAudienceType,
                uploadedFileName: String(req.body?.audienceSnapshot?.uploadedFileName || '').trim(),
                recipientCount: broadcastRecipients.length,
                selectedContactCount: selectedContactIds.length,
                contactIds: selectedContactIds
              },
        createdBy: req.user.username || req.user.email || req.user.id,
        createdByEmail: req.user.email,
        createdById: req.user.id,
        deliveryPolicy,
        retryPolicy,
        compliancePolicy
      });

      if (!created?.success || !created?.data?._id) {
        return res.status(400).json({
          success: false,
          message: created?.error || 'Failed to create broadcast'
        });
      }

      const queueResult = await enqueueBroadcastSend({
        broadcastId: created.data._id,
        userId: req.user.id,
        companyId: req.companyId || null,
        delayMs: 0,
        reason: 'bulk_upload_send'
      });

      if (!queueResult.success) {
        return res.status(400).json(queueResult);
      }

      return res.status(202).json({
        success: true,
        queued: true,
        engine: 'bulk_broadcast_unified_v3',
        broadcastId: created.data._id,
        jobId: queueResult.data.jobId,
        total_sent: broadcastRecipients.length,
        successful: 0,
        failed: 0,
        results: [],
        audienceValidation,
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
