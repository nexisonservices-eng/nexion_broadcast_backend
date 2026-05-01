const express = require('express');
const auth = require('../middleware/auth');
const Contact = require('../models/Contact');
const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');
const ConsentExportJob = require('../models/ConsentExportJob');
const { generateConsentExport } = require('../services/consentExportService');
const { applyContactOptOut, toCleanString } = require('../services/whatsappOutreach/policy');
const { logConsentEvent } = require('../services/whatsappConsentLogService');

const router = express.Router();
router.use(auth);

const isSuperAdminRole = (role = '') => {
  const normalizedRole = String(role || '').trim().toLowerCase();
  return normalizedRole === 'super_admin' || normalizedRole === 'superadmin';
};

const ensureTenantCompanyId = (req, res) => {
  if (isSuperAdminRole(req.user?.role)) return true;
  if (toCleanString(req.companyId)) return true;
  res.status(400).json({ success: false, error: 'companyId is required for tenant-scoped requests.' });
  return false;
};

const buildConsentFilters = (req) => {
  const filters = {};
  const requestedUserId = toCleanString(req.query?.userId);
  const requestedCompanyId = toCleanString(req.query?.companyId);
  const requestedPhone = toCleanString(req.query?.phone);
  const requestedAction = toCleanString(req.query?.action);
  const startDate = toCleanString(req.query?.startDate);
  const endDate = toCleanString(req.query?.endDate);
  const includeArchived = String(req.query?.includeArchived || '').toLowerCase() === 'true';

  if (isSuperAdminRole(req.user?.role)) {
    if (requestedUserId) filters.userId = requestedUserId;
    if (requestedCompanyId) filters.companyId = requestedCompanyId;
  } else {
    filters.userId = req.user.id;
    filters.companyId = req.companyId;
  }

  if (requestedPhone) {
    filters.phone = { $regex: requestedPhone, $options: 'i' };
  }
  if (requestedAction) {
    filters.action = requestedAction;
  }

  if (startDate || endDate) {
    const createdAt = {};
    if (startDate) {
      const parsed = new Date(startDate);
      if (!Number.isNaN(parsed.getTime())) {
        createdAt.$gte = parsed;
      }
    }
    if (endDate) {
      const parsed = new Date(endDate);
      if (!Number.isNaN(parsed.getTime())) {
        parsed.setHours(23, 59, 59, 999);
        createdAt.$lte = parsed;
      }
    }
    if (Object.keys(createdAt).length) {
      filters.createdAt = createdAt;
    }
  }

  if (!includeArchived) {
    filters.isArchived = { $ne: true };
  }

  return filters;
};

const exportCooldownMap = new Map();

const getExportCooldownKey = (req) => {
  const userId = toCleanString(req.user?.id);
  const ip =
    toCleanString(req.headers['x-forwarded-for']?.split(',')?.[0]) ||
    toCleanString(req.ip);
  return `${userId || 'anon'}::${ip || 'unknown'}`;
};

const hasExportCooldown = (req) => {
  const key = getExportCooldownKey(req);
  const last = exportCooldownMap.get(key) || 0;
  return Date.now() - last < 5000;
};

const setExportCooldown = (req) => {
  exportCooldownMap.set(getExportCooldownKey(req), Date.now());
};

const buildMissingProofFilters = (req) => {
  const filters = {
    whatsappOptInStatus: 'opted_in',
    $or: [
      { whatsappOptInProofType: { $exists: false } },
      { whatsappOptInProofType: null },
      { whatsappOptInProofType: '' },
      { whatsappOptInTextSnapshot: { $exists: false } },
      { whatsappOptInTextSnapshot: null },
      { whatsappOptInTextSnapshot: '' }
    ]
  };

  const requestedSourceType = toCleanString(req.query?.sourceType);
  const requestedUserId = toCleanString(req.query?.userId);
  const requestedCompanyId = toCleanString(req.query?.companyId);
  const includeImportedOnly = String(req.query?.importedOnly || 'true').toLowerCase() !== 'false';

  if (includeImportedOnly) {
    filters.sourceType = { $in: ['imported', 'manual', 'public_opt_in', 'meta_lead_ads'] };
    filters.whatsappOptInSource = { $in: ['import', 'csv_import', 'manual', 'public_opt_in', 'meta_lead_ads'] };
  }

  if (requestedSourceType) {
    filters.sourceType = requestedSourceType;
  }

  if (isSuperAdminRole(req.user?.role)) {
    if (requestedUserId) filters.userId = requestedUserId;
    if (requestedCompanyId) filters.companyId = requestedCompanyId;
  } else {
    filters.userId = req.user.id;
    filters.companyId = req.companyId;
  }

  return filters;
};

// CSV building is handled in consentExportService.

router.get('/', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;
    const page = Math.max(1, Number(req.query?.page || 1));
    const limit = Math.min(100, Math.max(10, Number(req.query?.limit || 20)));
    const skip = (page - 1) * limit;

    const filters = buildConsentFilters(req);

    const [items, total] = await Promise.all([
      WhatsAppConsentLog.find(filters)
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean(),
      WhatsAppConsentLog.countDocuments(filters)
    ]);

    return res.json({
      success: true,
      data: items,
      page,
      limit,
      total,
      totalPages: Math.max(1, Math.ceil(total / limit))
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/export', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;
    if (hasExportCooldown(req)) {
      return res.status(429).json({ success: false, error: 'Export rate limited. Try again shortly.' });
    }
    const filters = buildConsentFilters(req);
    const { csv, checksum, count } = await generateConsentExport({ filters, limit: 5000 });
    if (count === 5000) {
      res.setHeader('X-Consent-Export-Limit', '5000');
    }
    res.setHeader('X-Consent-Export-Checksum', checksum);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="whatsapp-consent-logs.csv"');
    setExportCooldown(req);
    return res.status(200).send(csv);
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/force-opt-out', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;
    const contactId = toCleanString(req.body?.contactId);
    if (!contactId) {
      return res.status(400).json({ success: false, error: 'contactId is required.' });
    }

    const isSuperAdmin = isSuperAdminRole(req.user?.role);
    const contact = await Contact.findOne(
      isSuperAdmin
        ? { _id: contactId }
        : {
            _id: contactId,
            userId: req.user.id,
            companyId: req.companyId
          }
    );

    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found.' });
    }

    applyContactOptOut(contact, { source: 'admin_force' });
    await contact.save();
    await logConsentEvent({
      contact,
      action: 'opt_out',
      payload: {
        source: 'admin_force',
        scope: contact.whatsappOptInScope,
        consentText: contact.whatsappOptInTextSnapshot,
        proofType: contact.whatsappOptInProofType,
        proofId: contact.whatsappOptInProofId,
        proofUrl: contact.whatsappOptInProofUrl,
        capturedBy: contact.whatsappOptInCapturedBy,
        pageUrl: contact.whatsappOptInPageUrl,
        ip: contact.whatsappOptInIp,
        userAgent: contact.whatsappOptInUserAgent,
        metadata: contact.whatsappOptInMetadata
      }
    });

    return res.json({
      success: true,
      data: {
        contactId: contact._id,
        whatsappOptInStatus: contact.whatsappOptInStatus,
        whatsappOptOutAt: contact.whatsappOptOutAt
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/export-email', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;
    const email = toCleanString(req.body?.email);
    if (!email) {
      return res.status(400).json({ success: false, error: 'email is required.' });
    }

    const filters = buildConsentFilters(req);
    const job = await ConsentExportJob.create({
      userId: req.user?.id || null,
      companyId: req.companyId,
      requestedBy: req.user?.email || req.user?.id || 'unknown',
      email,
      filters,
      status: 'queued'
    });

    return res.json({
      success: true,
      data: {
        jobId: job._id,
        status: job.status
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/export-jobs', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;
    const filters = {};
    if (isSuperAdminRole(req.user?.role)) {
      if (req.query?.userId) filters.userId = toCleanString(req.query.userId);
      if (req.query?.companyId) filters.companyId = toCleanString(req.query.companyId);
    } else {
      filters.userId = req.user.id;
      if (req.companyId) filters.companyId = req.companyId;
    }

    const jobs = await ConsentExportJob.find(filters)
      .sort({ createdAt: -1 })
      .limit(50)
      .lean();

    return res.json({ success: true, data: jobs });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/export-jobs/:id/download', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;
    const jobId = toCleanString(req.params.id);
    if (!jobId) {
      return res.status(400).json({ success: false, error: 'Job id is required.' });
    }

    const isSuperAdmin = isSuperAdminRole(req.user?.role);
    const job = await ConsentExportJob.findOne(
      isSuperAdmin
        ? { _id: jobId }
        : {
            _id: jobId,
            userId: req.user.id,
            ...(req.companyId
              ? { companyId: req.companyId }
              : {})
          }
    ).lean();

    if (!job) {
      return res.status(404).json({ success: false, error: 'Export job not found.' });
    }

    if (job.status !== 'completed') {
      return res.status(409).json({ success: false, error: 'Export job is not completed yet.' });
    }

    const { csv, checksum } = await generateConsentExport({
      filters: job.filters || {},
      limit: Number(process.env.CONSENT_EXPORT_MAX_ROWS || 5000)
    });

    res.setHeader('X-Consent-Export-Checksum', checksum);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="whatsapp-consent-logs_${jobId}.csv"`
    );
    return res.status(200).send(csv);
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/review/missing-proof', async (req, res) => {
  try {
    if (!ensureTenantCompanyId(req, res)) return;

    const page = Math.max(1, Number(req.query?.page || 1));
    const limit = Math.min(100, Math.max(10, Number(req.query?.limit || 25)));
    const skip = (page - 1) * limit;
    const filters = buildMissingProofFilters(req);

    const projection = [
      '_id',
      'name',
      'phone',
      'email',
      'sourceType',
      'whatsappOptInStatus',
      'whatsappOptInSource',
      'whatsappOptInAt',
      'whatsappOptInTextSnapshot',
      'whatsappOptInProofType',
      'whatsappOptInProofId',
      'whatsappOptInCapturedBy',
      'whatsappOptInPageUrl',
      'createdAt',
      'updatedAt'
    ].join(' ');

    const [items, total, prooflessCount, textlessCount] = await Promise.all([
      Contact.find(filters).select(projection).sort({ updatedAt: -1, createdAt: -1 }).skip(skip).limit(limit).lean(),
      Contact.countDocuments(filters),
      Contact.countDocuments({
        ...filters,
        $or: [{ whatsappOptInProofType: { $in: [null, ''] } }, { whatsappOptInProofType: { $exists: false } }]
      }),
      Contact.countDocuments({
        ...filters,
        $or: [{ whatsappOptInTextSnapshot: { $in: [null, ''] } }, { whatsappOptInTextSnapshot: { $exists: false } }]
      })
    ]);

    return res.json({
      success: true,
      data: items,
      summary: {
        total,
        prooflessCount,
        textlessCount
      },
      page,
      limit,
      totalPages: Math.max(1, Math.ceil(total / limit))
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
