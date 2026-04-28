const express = require('express');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const requirePlanFeature = require('../middleware/planGuard');
const broadcastService = require('../services/broadcastService');
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
      '_id phone isBlocked whatsappOptInStatus whatsappOptInAt whatsappOptOutAt serviceWindowClosesAt'
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
    const phoneColumn = headers[0];
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
      headers.slice(1).forEach((header) => {
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
      const templateCategory = toCleanString(req.body?.templateCategory || '') || 'marketing';

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
          canProceed: validation.summary.eligible > 0
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
        deliveryPolicy,
        retryPolicy,
        compliancePolicy
      } = req.body;

      const msgType = message_type || messageType || (templateName ? 'template' : 'text');
      const normalizedTemplateCategory = toCleanString(templateCategory) || 'marketing';
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

      if (!audienceValidation.summary.eligible) {
        return res.status(400).json({
          success: false,
          message: 'No eligible recipients found for this WhatsApp campaign',
          audienceValidation
        });
      }

      const eligibleRecipients = audienceValidation.eligibleRecipients.map((recipient) => {
        const fullData = recipient?.data || recipient?.fullData || {};
        return {
          phone: String(recipient?.phone || '').trim(),
          name: String(recipient?.name || '').trim(),
          variables: Array.isArray(recipient?.variables) ? recipient.variables : [],
          attributes: fullData && typeof fullData === 'object' ? fullData : {}
        };
      });

      const created = await broadcastService.createBroadcast({
        name: broadcast_name || `Bulk Send - ${new Date().toISOString()}`,
        companyId: req.companyId || null,
        messageType: msgType,
        message: customMsg,
        templateName: finalTemplateName || null,
        templateCategory: normalizedTemplateCategory,
        templateContent: String(templateContent || '').trim(),
        language: language || 'en_US',
        recipients: eligibleRecipients,
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

      const broadcaster = (payload) => {
        const sendToUser = req.app?.locals?.sendToUser;
        if (typeof sendToUser === 'function') {
          sendToUser(String(req.user.id), payload);
        }
      };

      const sent = await broadcastService.sendBroadcast(
        created.data._id,
        broadcaster,
        req.whatsappCredentials || null
      );

      if (!sent?.success) {
        return res.status(400).json({
          success: false,
          message: sent?.error || 'Bulk send failed',
          broadcastId: created.data._id
        });
      }

      return res.json({
        success: true,
        engine: 'bulk_broadcast_unified_v3',
        broadcastId: created.data._id,
        total_sent: eligibleRecipients.length,
        successful: Number(sent?.data?.stats?.successful || 0),
        failed: Number(sent?.data?.stats?.failed || 0),
        results: Array.isArray(sent?.data?.results) ? sent.data.results : [],
        audienceValidation
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
