const express = require('express');
const fs = require('fs');
const os = require('os');
const path = require('path');
const multer = require('multer');
const auth = require('../middleware/auth');
const requireWhatsAppCredentials = require('../middleware/requireWhatsAppCredentials');
const requirePlanFeature = require('../middleware/planGuard');
const broadcastService = require('../services/broadcastService');
const { enqueueBroadcastSend } = require('../queues/broadcastQueue');
const { enqueueCsvImport } = require('../queues/csvImportQueue');
const { processCsvImport } = require('../services/csvImportProcessor');
const CsvImportJob = require('../models/CsvImportJob');
const Contact = require('../models/Contact');
const { buildPhoneCandidates } = require('../services/whatsappOutreach/conversationResolver');
const {
  buildBroadcastAudienceValidation,
  toCleanString
} = require('../services/whatsappOutreach/policy');

const router = express.Router();
router.use(auth);

const csvImportUploadDir = path.join(os.tmpdir(), 'nexion-csv-imports');
fs.mkdirSync(csvImportUploadDir, { recursive: true });
const csvImportUpload = multer({
  storage: multer.diskStorage({
    destination: csvImportUploadDir,
    filename: (_req, file, cb) => {
      const safeName = String(file.originalname || 'csv-import.csv')
        .replace(/[^a-zA-Z0-9_.-]+/g, '_')
        .slice(0, 80);
      cb(null, `${Date.now()}-${Math.random().toString(36).slice(2, 10)}-${safeName}`);
    }
  }),
  limits: {
    fileSize: Math.max(5 * 1024 * 1024, Number(process.env.CSV_IMPORT_FILE_SIZE_LIMIT || 50 * 1024 * 1024))
  }
});

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

router.post('/imports', csvImportUpload.single('csv_file'), async (req, res) => {
  const uploadedFile = req.file || null;
  const originalFileName = String(uploadedFile?.originalname || '').trim();
  const storedFileName = String(uploadedFile?.filename || '').trim();
  const filePath = String(uploadedFile?.path || '').trim();

  if (!uploadedFile || !filePath) {
    return res.status(400).json({
      success: false,
      message: 'CSV file is required'
    });
  }

  try {
    const job = await CsvImportJob.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      originalFileName,
      storedFileName,
      filePath,
      status: 'queued',
      currentStage: 'queued',
      queueJobId: ''
    });

    const queueResult = await enqueueCsvImport({
      importJobId: job._id,
      userId: req.user.id,
      companyId: req.companyId || null,
      filePath,
      originalFileName
    });

    if (!queueResult.success) {
      await CsvImportJob.findByIdAndUpdate(job._id, {
        status: 'failed',
        currentStage: 'failed',
        errorMessage: queueResult.error || 'Failed to queue CSV import',
        completedAt: new Date()
      });
      return res.status(400).json(queueResult);
    }

    await CsvImportJob.findByIdAndUpdate(job._id, {
      queueJobId: queueResult.data.jobId,
      updatedAt: new Date()
    });

    if (String(process.env.DISABLE_REDIS || process.env.REDIS_DISABLED || '').trim().toLowerCase() === 'true') {
      void processCsvImport({
        id: queueResult.data.jobId,
        data: {
          importJobId: String(job._id),
          userId: String(req.user.id),
          companyId: String(req.companyId || ''),
          filePath,
          originalFileName
        }
      }).catch(async (error) => {
        await CsvImportJob.findByIdAndUpdate(job._id, {
          status: 'failed',
          currentStage: 'failed',
          errorMessage: String(error?.message || error || 'CSV import failed'),
          completedAt: new Date()
        });
      });
    }

    return res.status(202).json({
      success: true,
      queued: true,
      importJob: {
        id: String(job._id),
        status: 'queued',
        currentStage: 'queued',
        originalFileName,
        totalRows: 0,
        processedRows: 0,
        successCount: 0,
        failedCount: 0,
        duplicateCount: 0,
        skippedCount: 0,
        percentComplete: 0
      },
      message: 'CSV import queued. Processing will continue in the background.'
    });
  } catch (error) {
    if (filePath) {
      try {
        fs.unlinkSync(filePath);
      } catch {
        // ignore cleanup errors
      }
    }
    return res.status(500).json({
      success: false,
      message: error.message || 'Failed to queue CSV import'
    });
  }
});

router.get('/imports', async (req, res) => {
  try {
    const limit = Math.min(20, Math.max(1, Number(req.query.limit || 8)));
    const status = String(req.query.status || '').trim();
    const query = {
      userId: req.user.id,
      ...(req.companyId ? { companyId: req.companyId } : {})
    };
    if (status) {
      query.status = status;
    }

    const jobs = await CsvImportJob.find(query)
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean();

    return res.json({
      success: true,
      data: {
        jobs
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || 'Failed to load CSV import history'
    });
  }
});

router.get('/imports/:id', async (req, res) => {
  try {
    const job = await CsvImportJob.findOne({
      _id: req.params.id,
      userId: req.user.id,
      ...(req.companyId ? { companyId: req.companyId } : {})
    }).lean();

    if (!job) {
      return res.status(404).json({
        success: false,
        message: 'CSV import job not found'
      });
    }

    return res.json({
      success: true,
      data: {
        job
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || 'Failed to load CSV import job'
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

      const reqAudienceSource = req.body?.audienceSource && typeof req.body.audienceSource === 'object'
        ? req.body.audienceSource
        : {};
      const reqAudienceSnapshot = req.body?.audienceSnapshot && typeof req.body.audienceSnapshot === 'object'
        ? req.body.audienceSnapshot
        : {};
      const compactImportJobId = String(
        reqAudienceSource.importJobId ||
        reqAudienceSnapshot.importJobId ||
        req.body?.importJobId ||
        ''
      ).trim();
      const compactContactIds = Array.from(
        new Set(
          [
            ...(Array.isArray(reqAudienceSource.contactIds) ? reqAudienceSource.contactIds : []),
            ...(Array.isArray(reqAudienceSnapshot.contactIds) ? reqAudienceSnapshot.contactIds : [])
          ].map((contactId) => String(contactId || '').trim()).filter(Boolean)
        )
      );
      const useCompactAudience = Boolean(compactImportJobId || compactContactIds.length);

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
      } else if (!useCompactAudience) {
        return res.status(400).json({
          success: false,
          message: 'Recipients data or CSV file is required'
        });
      }

      if (!parsedRecipients.length && !useCompactAudience) {
        return res.status(400).json({
          success: false,
          message: 'No valid recipients found'
        });
      }

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
      const selectedContactIds = broadcastRecipients
        .map((recipient) => String(recipient?.contactId || '').trim())
        .filter(Boolean);
      const compactRecipientCount = useCompactAudience
        ? compactImportJobId
          ? await Contact.countDocuments(
              buildScopedContactFilter(req, { importJobId: compactImportJobId })
            )
          : compactContactIds.length
        : broadcastRecipients.length;
      const compactSelectedContactCount = useCompactAudience
        ? compactContactIds.length
        : selectedContactIds.length;
      const compactSourceName = compactImportJobId
        ? 'csv_import_job'
        : hasContactsAudience || compactContactIds.length
          ? 'crm_contacts'
          : 'csv_upload';
      const compactAudienceMode = compactImportJobId
        ? 'csv_import_job'
        : compactContactIds.length
          ? 'contacts'
          : hasContactsAudience
            ? 'contacts'
            : 'csv';
      const compactAudienceLabel = compactImportJobId
        ? 'CSV import job'
        : compactContactIds.length
          ? 'Selected CRM contacts'
          : hasContactsAudience
            ? 'Selected CRM contacts'
            : 'CSV upload';
      const compactAudienceType = compactImportJobId
        ? 'csv_import_job'
        : compactContactIds.length
          ? 'contacts'
          : hasContactsAudience
            ? 'contacts'
            : 'csv';

      const audienceValidation = useCompactAudience
        ? {
            valid: true,
            mode: compactAudienceMode,
            summary: {
              recipientCount: compactRecipientCount,
              selectedContactCount: compactSelectedContactCount,
              importJobId: compactImportJobId
            },
            eligibleRecipients: [],
            invalidRecipients: [],
            duplicateRecipients: []
          }
        : buildBroadcastAudienceValidation({
            recipients: parsedRecipients,
            contactsByPhone: await buildContactsByPhoneMap(req, parsedRecipients),
            messageType: msgType,
            templateCategory: normalizedTemplateCategory
          });

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
        recipients: useCompactAudience ? [] : broadcastRecipients,
        recipientCount: compactRecipientCount,
        audienceSource:
          useCompactAudience
            ? {
                ...reqAudienceSource,
                mode: compactAudienceMode,
                label: compactAudienceLabel,
                type: compactAudienceType,
                segmentId: String(reqAudienceSource.segmentId || '').trim(),
                sourceName: compactSourceName,
                uploadedFileName: String(reqAudienceSource.uploadedFileName || '').trim(),
                recipientCount: compactRecipientCount,
                selectedContactCount: compactSelectedContactCount,
                hasContactIds: compactContactIds.length > 0 || Boolean(compactImportJobId),
                importJobId: compactImportJobId,
                contactIds: compactContactIds
              }
            : reqAudienceSource,
        audienceSnapshot:
          useCompactAudience
            ? {
                ...reqAudienceSnapshot,
                mode: compactAudienceMode,
                label: compactAudienceLabel,
                sourceType: compactAudienceType,
                uploadedFileName: String(reqAudienceSnapshot.uploadedFileName || '').trim(),
                recipientCount: compactRecipientCount,
                selectedContactCount: compactSelectedContactCount,
                contactIds: compactContactIds,
                importJobId: compactImportJobId
              }
            : reqAudienceSnapshot,
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
        reason: 'bulk_upload_send',
        fallbackProcess: () =>
          broadcastService.sendBroadcast(
            created.data._id,
            null,
            req.whatsappCredentials || null
          )
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
        total_sent: useCompactAudience ? compactRecipientCount : broadcastRecipients.length,
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
