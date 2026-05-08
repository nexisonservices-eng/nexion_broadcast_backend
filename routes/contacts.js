const express = require('express');
const Contact = require('../models/Contact');
const auth = require('../middleware/auth');
const { normalizeRole, isTenantWideRole } = require('../utils/accessControl');
const {
  applyContactOptIn,
  applyContactOptOut,
  getWhatsAppMessagingPolicy,
  toCleanString
} = require('../services/whatsappOutreach/policy');
const { logConsentEvent } = require('../services/whatsappConsentLogService');

const router = express.Router();
router.use(auth);

const emitCrmRealtimeEvent = (req, payload = {}) => {
  const sendToUser = req?.app?.locals?.sendToUser;
  if (typeof sendToUser !== 'function') return;

  const userId = toCleanString(req?.user?.id);
  if (!userId) return;

  sendToUser(userId, {
    type: 'crm_changed',
    scope: 'crm',
    timestamp: new Date().toISOString(),
    ...payload,
    contactId: toCleanString(payload?.contactId),
    phone: toCleanString(payload?.phone),
    action: toCleanString(payload?.action)
  });
};

const CONTACT_LIST_FIELDS = [
  '_id',
  'name',
  'phone',
  'email',
  'tags',
  'stage',
  'status',
  'source',
  'ownerId',
  'sourceType',
  'lastContact',
  'lastContactAt',
  'nextFollowUpAt',
  'isBlocked',
  'whatsappOptInStatus',
  'whatsappOptInAt',
  'whatsappOptInSource',
  'whatsappOptInScope',
  'whatsappOptInTextSnapshot',
  'whatsappOptInProofType',
  'whatsappOptInProofId',
  'whatsappOptInProofUrl',
  'whatsappOptInCapturedBy',
  'whatsappOptInPageUrl',
  'whatsappOptInIp',
  'whatsappOptInUserAgent',
  'whatsappOptInMetadata',
  'whatsappMarketingWindowStartedAt',
  'whatsappMarketingSendCount',
  'whatsappMarketingLastSentAt',
  'whatsappOptOutAt',
  'lastInboundMessageAt',
  'serviceWindowClosesAt',
  'leadScore',
  'createdAt',
  'updatedAt'
].join(' ');

const ALLOWED_CONTACT_SOURCE_TYPES = new Set([
  'manual',
  'imported',
  'incoming_message',
  'incoming_call',
  'public_opt_in',
  'meta_lead_ads'
]);

const normalizePhoneNumber = (value) => String(value || '').replace(/\D/g, '');

const isValidPhoneNumber = (value) => {
  const digits = normalizePhoneNumber(value);
  return digits.length >= 10 && digits.length <= 15;
};

const getPhoneLookupCandidates = (value) => {
  const normalized = normalizePhoneNumber(value);
  if (!normalized) return [];
  const values = [normalized];
  if (normalized.length > 10) {
    values.push(normalized.slice(-10));
  }
  return Array.from(new Set(values));
};

const buildPhoneMatchFilter = (value) => {
  const values = getPhoneLookupCandidates(value);
  if (!values.length) return null;
  return { phone: { $in: values } };
};

const buildBulkPhoneMatchFilter = (phones = []) => {
  const lookupPhones = Array.from(
    new Set(
      (Array.isArray(phones) ? phones : [])
        .map((phone) => getPhoneLookupCandidates(phone))
        .flat()
        .filter(Boolean)
    )
  );

  if (!lookupPhones.length) return null;
  return { phone: { $in: lookupPhones } };
};

const buildScopeCondition = (req) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const tenantWide = isTenantWideRole(normalizedRole);
  const scopeCandidates = [];

  if (req.companyId) {
    // Tenant-wide roles can see the full workspace for their company.
    if (tenantWide) return { companyId: req.companyId };
    scopeCandidates.push({ companyId: req.companyId });
  }

  if (req?.user?.id && !tenantWide) {
    scopeCandidates.push({ userId: req.user.id });
  }

  // Tenant-wide role without company scope: keep unscoped (legacy superadmin contexts).
  if (tenantWide && !req.companyId) return {};

  if (!scopeCandidates.length) return {};
  return scopeCandidates.length === 1 ? scopeCandidates[0] : { $or: scopeCandidates };
};

const buildScopedContactFilter = (req, extra = {}) => {
  const scopedConditions = [];
  const scopeCondition = buildScopeCondition(req);
  if (Object.keys(scopeCondition).length > 0) {
    scopedConditions.push(scopeCondition);
  }

  if (extra && Object.keys(extra).length > 0) {
    scopedConditions.push(extra);
  }

  if (!scopedConditions.length) return {};
  return scopedConditions.length === 1 ? scopedConditions[0] : { $and: scopedConditions };
};

const toContactResponse = (contact) => {
  if (!contact || typeof contact.toObject !== 'function') return contact;
  return contact.toObject();
};

const getPreferredPhoneValue = (contact = {}) => {
  const values = getPhoneLookupCandidates(contact?.phone);
  return values[0] || '';
};

const getMergedTags = (...tagSets) =>
  Array.from(
    new Set(
      tagSets
        .flatMap((value) => (Array.isArray(value) ? value : []))
        .map((tag) => String(tag || '').trim())
        .filter(Boolean)
    )
  );

const parseTagList = (value) =>
  String(value || '')
    .split(',')
    .map((tag) => tag.trim())
    .filter(Boolean);

const normalizeImportFieldKey = (value = '') =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');

const getImportedFieldValue = (contactData = {}, aliases = []) => {
  if (!contactData || typeof contactData !== 'object') return '';

  const normalizedLookup = new Map(
    Object.entries(contactData).map(([key, value]) => [normalizeImportFieldKey(key), value])
  );

  for (const alias of Array.isArray(aliases) ? aliases : []) {
    const rawValue = normalizedLookup.get(normalizeImportFieldKey(alias));
    if (rawValue === undefined || rawValue === null) continue;
    const cleanedValue = typeof rawValue === 'string' ? rawValue.trim() : rawValue;
    if (cleanedValue === '') continue;
    return cleanedValue;
  }

  return '';
};

const normalizeImportedContactData = (contactData = {}) => {
  if (!contactData || typeof contactData !== 'object') return {};

  const normalizedTags = (() => {
    const rawTags = getImportedFieldValue(contactData, [
      'tags',
      'tag',
      'Tag',
      'contactTags',
      'contact tags'
    ]);
    if (Array.isArray(rawTags)) {
      return rawTags.map((tag) => String(tag || '').trim()).filter(Boolean);
    }
    return parseTagList(rawTags);
  })();

  return {
    ...contactData,
    name: toCleanString(
      getImportedFieldValue(contactData, ['name', 'firstName', 'first name', 'fullName', 'full name'])
    ),
    phone: toCleanString(
      getImportedFieldValue(contactData, [
        'phone',
        'whatsappNumber',
        'whatsapp number',
        'mobile',
        'mobile number',
        'contactNumber',
        'contact number',
        'phoneNumber',
        'phone number'
      ])
    ),
    email: toCleanString(getImportedFieldValue(contactData, ['email', 'emailAddress', 'email address'])),
    status: toCleanString(
      getImportedFieldValue(contactData, ['status', 'whatsappOptInStatus', 'optInStatus', 'opt in status'])
    ),
    source: toCleanString(getImportedFieldValue(contactData, ['source', 'importSource', 'import source'])),
    sourceType: toCleanString(
      getImportedFieldValue(contactData, ['sourceType', 'source type', 'contactSourceType'])
    ),
    scope: toCleanString(
      getImportedFieldValue(contactData, [
        'scope',
        'consentScope',
        'consent scope',
        'whatsappOptInScope',
        'whatsapp opt in scope'
      ])
    ),
    capturedBy: toCleanString(
      getImportedFieldValue(contactData, ['capturedBy', 'captured by', 'whatsappOptInCapturedBy'])
    ),
    lineNumber: getImportedFieldValue(contactData, ['lineNumber', 'line number']) || contactData.lineNumber || null,
    metadata:
      contactData?.metadata && typeof contactData.metadata === 'object' && !Array.isArray(contactData.metadata)
        ? contactData.metadata
        : null,
    tags: normalizedTags
  };
};

const normalizeContactInput = (payload = {}, fallbackSourceType = 'manual') => {
  const next = { ...payload };
  if (payload.phone !== undefined) {
    next.phone = getPreferredPhoneValue(payload);
  }
  if (Array.isArray(payload.tags)) {
    next.tags = payload.tags.map((tag) => String(tag || '').trim()).filter(Boolean);
  }
  const requestedSourceType = toCleanString(payload.sourceType).toLowerCase();
  next.sourceType = ALLOWED_CONTACT_SOURCE_TYPES.has(requestedSourceType)
    ? requestedSourceType
    : fallbackSourceType;
  return next;
};

const normalizeOptInScope = (value = '') => {
  const normalized = toCleanString(value).toLowerCase();
  if (['marketing', 'service', 'both'].includes(normalized)) return normalized;
  return 'unknown';
};

const buildOptInAuditPayload = (body = {}, req) => ({
  source: toCleanString(body?.source) || 'manual',
  scope: normalizeOptInScope(body?.scope),
  textSnapshot: toCleanString(body?.consentText || body?.textSnapshot),
  proofType: toCleanString(body?.proofType),
  proofId: toCleanString(body?.proofId),
  proofUrl: toCleanString(body?.proofUrl),
  capturedBy: toCleanString(body?.capturedBy),
  pageUrl: toCleanString(body?.pageUrl),
  ip: toCleanString(
    req.headers['x-forwarded-for']?.split(',')?.[0] ||
      req.ip ||
      req.socket?.remoteAddress
  ),
  userAgent: toCleanString(req.headers['user-agent']),
  metadata:
    body?.metadata && typeof body.metadata === 'object' && !Array.isArray(body.metadata)
      ? body.metadata
      : null
});

const normalizeImportStatus = (value = '') =>
  toCleanString(value)
    .toLowerCase()
    .replace(/[\s_]+/g, '-');

const buildImportedConsentReferenceId = (normalizedPhone = '', lineNumber = null) => {
  const phoneDigits = normalizePhoneNumber(normalizedPhone).slice(-4) || 'contact';
  const rowToken = String(lineNumber || 'row').trim() || 'row';
  const timestampToken = Date.now().toString(36);
  const randomToken = Math.random().toString(36).slice(2, 8);
  return `landing-page-import-${rowToken}-${phoneDigits}-${timestampToken}-${randomToken}`;
};

const setContactField = (contact, key, value) => {
  if (!contact || typeof contact !== 'object') return;
  if (typeof contact.set === 'function') {
    contact.set(key, value);
    return;
  }
  contact[key] = value;
};

const applyImportedLandingPageConsent = (contact, {
  referenceId = '',
  scope = 'marketing',
  lineNumber = null
} = {}) => {
  if (!contact || typeof contact !== 'object') return contact;
  setContactField(contact, 'whatsappOptInStatus', 'opted_in');
  setContactField(contact, 'whatsappOptInAt', new Date());
  setContactField(contact, 'whatsappOptInSource', 'landing_page');
  setContactField(contact, 'whatsappOptInScope', normalizeOptInScope(scope));
  setContactField(
    contact,
    'whatsappOptInTextSnapshot',
    contact.whatsappOptInTextSnapshot || 'Consent captured via website landing page.'
  );
  setContactField(contact, 'whatsappOptInProofType', contact.whatsappOptInProofType || 'import_record');
  setContactField(contact, 'whatsappOptInProofId', referenceId || contact.whatsappOptInProofId || '');
  setContactField(contact, 'whatsappOptInCapturedBy', contact.whatsappOptInCapturedBy || 'csv_import');
  setContactField(contact, 'whatsappOptInMetadata', {
    ...(contact.whatsappOptInMetadata && typeof contact.whatsappOptInMetadata === 'object'
      ? contact.whatsappOptInMetadata
      : {}),
    importLineNumber: lineNumber || contact.lineNumber || null,
    importSource: 'csv_import',
    consentSource: 'landing_page'
  });
  setContactField(contact, 'whatsappOptInPageUrl', contact.whatsappOptInPageUrl || '');
  setContactField(contact, 'whatsappOptInIp', contact.whatsappOptInIp || '');
  setContactField(contact, 'whatsappOptInUserAgent', contact.whatsappOptInUserAgent || '');
  setContactField(contact, 'isBlocked', false);
  return contact;
};

const buildImportedLandingPageConsentUpdate = (contact, {
  referenceId = '',
  scope = 'marketing',
  lineNumber = null
} = {}) => ({
  whatsappOptInStatus: 'opted_in',
  whatsappOptInAt: contact?.whatsappOptInAt || new Date(),
  whatsappOptInSource: 'landing_page',
  whatsappOptInScope: normalizeOptInScope(scope),
  whatsappOptInTextSnapshot:
    contact?.whatsappOptInTextSnapshot || 'Consent captured via website landing page.',
  whatsappOptInProofType: contact?.whatsappOptInProofType || 'import_record',
  whatsappOptInProofId: referenceId || contact?.whatsappOptInProofId || '',
  whatsappOptInCapturedBy: contact?.whatsappOptInCapturedBy || 'csv_import',
  whatsappOptInMetadata: {
    ...(contact?.whatsappOptInMetadata && typeof contact.whatsappOptInMetadata === 'object'
      ? contact.whatsappOptInMetadata
      : {}),
    importLineNumber: lineNumber || contact?.lineNumber || null,
    importSource: 'csv_import',
    consentSource: 'landing_page'
  },
  whatsappOptInPageUrl: contact?.whatsappOptInPageUrl || '',
  whatsappOptInIp: contact?.whatsappOptInIp || '',
  whatsappOptInUserAgent: contact?.whatsappOptInUserAgent || '',
  isBlocked: false
});

const normalizeContactConsentForResponse = (contact = {}) => {
  const policy = getWhatsAppMessagingPolicy(contact);
  const normalizedOptInStatus =
    policy.normalizedOptInStatus === 'opted_in'
      ? 'opted_in'
      : policy.normalizedOptInStatus === 'opted_out'
        ? 'opted_out'
        : 'unknown';
  const normalizedOptInSource =
    contact.whatsappOptInSource ||
    (normalizedOptInStatus === 'opted_in' ? 'landing_page' : null);

  return {
    ...contact,
    whatsappOptInStatus: normalizedOptInStatus,
    whatsappOptInSource: normalizedOptInSource
  };
};

const applyOptInAuditPayload = (contact, auditPayload) => {
  if (!contact || !auditPayload) return;
  setContactField(contact, 'whatsappOptInSource', auditPayload.source);
  setContactField(contact, 'whatsappOptInScope', auditPayload.scope);
  setContactField(contact, 'whatsappOptInTextSnapshot', auditPayload.textSnapshot);
  setContactField(contact, 'whatsappOptInProofType', auditPayload.proofType);
  setContactField(contact, 'whatsappOptInProofId', auditPayload.proofId);
  setContactField(contact, 'whatsappOptInProofUrl', auditPayload.proofUrl);
  setContactField(contact, 'whatsappOptInCapturedBy', auditPayload.capturedBy);
  setContactField(contact, 'whatsappOptInPageUrl', auditPayload.pageUrl);
  setContactField(contact, 'whatsappOptInIp', auditPayload.ip);
  setContactField(contact, 'whatsappOptInUserAgent', auditPayload.userAgent);
  setContactField(contact, 'whatsappOptInMetadata', auditPayload.metadata);
};

const clearWhatsAppConsentFields = (contact, { source = 'audit_review' } = {}) => {
  if (!contact || typeof contact !== 'object') return contact;
  setContactField(contact, 'whatsappOptInStatus', 'unknown');
  setContactField(contact, 'whatsappOptInAt', null);
  setContactField(contact, 'whatsappOptOutAt', null);
  setContactField(contact, 'whatsappOptInSource', toCleanString(source) || 'audit_review');
  setContactField(contact, 'whatsappOptInScope', 'unknown');
  setContactField(contact, 'whatsappOptInTextSnapshot', '');
  setContactField(contact, 'whatsappOptInProofType', '');
  setContactField(contact, 'whatsappOptInProofId', '');
  setContactField(contact, 'whatsappOptInProofUrl', '');
  setContactField(contact, 'whatsappOptInCapturedBy', '');
  setContactField(contact, 'whatsappOptInPageUrl', '');
  setContactField(contact, 'whatsappOptInIp', '');
  setContactField(contact, 'whatsappOptInUserAgent', '');
  setContactField(contact, 'whatsappOptInMetadata', null);
  return contact;
};

// Get all contacts
router.get('/', async (req, res) => {
  try {
    const { search, tags } = req.query;
    const requestedOptInStatus = String(
      req.query.whatsappOptInStatus || req.query.optInStatus || ''
    ).trim().toLowerCase();
    const requestedSourceType = String(req.query.sourceType || '').trim().toLowerCase();
    const marketingEligibleOnly =
      String(req.query.marketingEligible || req.query.marketingEligibleOnly || '').trim().toLowerCase() ===
      'true';
    const hasWhatsApp =
      String(req.query.hasWhatsApp || '').trim().toLowerCase() === 'true';
    const parsedLimit = Number(req.query.limit);
    const parsedPage = Number(req.query.page);
    const limit = Number.isFinite(parsedLimit) && parsedLimit > 0 ? Math.min(Math.floor(parsedLimit), 200) : null;
    const page = Number.isFinite(parsedPage) && parsedPage > 0 ? Math.floor(parsedPage) : 1;
    const queryConditions = [];

    if (search) {
      queryConditions.push({
        $or: [
          { name: { $regex: search, $options: 'i' } },
          { phone: { $regex: search, $options: 'i' } },
          { email: { $regex: search, $options: 'i' } }
        ]
      });
    }
    
    if (tags) {
      queryConditions.push({ tags: { $in: parseTagList(tags) } });
    }

    if (requestedOptInStatus && requestedOptInStatus !== 'all') {
      queryConditions.push({ whatsappOptInStatus: requestedOptInStatus });
    }

    if (requestedSourceType && requestedSourceType !== 'all') {
      queryConditions.push({ sourceType: requestedSourceType });
    }

    if (marketingEligibleOnly) {
      queryConditions.push({
        whatsappOptInStatus: 'opted_in',
        isBlocked: false,
        whatsappOptInScope: { $in: ['marketing', 'both'] }
      });
    }

    if (hasWhatsApp) {
      queryConditions.push({
        phone: { $exists: true, $nin: [null, ''] }
      });
    }

    const extraFilters =
      queryConditions.length === 0 ? {} : queryConditions.length === 1 ? queryConditions[0] : { $and: queryConditions };
    const filters = buildScopedContactFilter(req, extraFilters);
    let contactQuery = Contact.find(filters)
      .select(CONTACT_LIST_FIELDS)
      .sort({ lastContact: -1, createdAt: -1 });

    if (limit) {
      const skip = Math.max(0, (page - 1) * limit);
      contactQuery = contactQuery.skip(skip).limit(limit);
    }

    let contacts = await contactQuery.lean();
    let totalCount = await Contact.countDocuments(filters);

    // Backward compatibility: older contacts were saved without user/company scope.
    // If scoped query returns nothing, surface legacy contacts so existing data doesn't disappear.
    if (!contacts.length) {
      const legacyConditions = [
        {
          $or: [
            { userId: { $exists: false } },
            { userId: null },
            { userId: '' }
          ]
        }
      ];

      if (req.companyId) {
        legacyConditions.push({
          $or: [
            { companyId: req.companyId },
            { companyId: { $exists: false } },
            { companyId: null },
            { companyId: '' }
          ]
        });
      }

      if (search) {
        legacyConditions.push({
          $or: [
            { name: { $regex: search, $options: 'i' } },
            { phone: { $regex: search, $options: 'i' } },
            { email: { $regex: search, $options: 'i' } }
          ]
        });
      }

      if (tags) {
        legacyConditions.push({ tags: { $in: parseTagList(tags) } });
      }

      const legacyFilters =
        legacyConditions.length === 1 ? legacyConditions[0] : { $and: legacyConditions };

      contacts = await Contact.find(legacyFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort({ lastContact: -1, createdAt: -1 })
        .lean();
      totalCount = await Contact.countDocuments(legacyFilters);
    }

    // Final recovery fallback for legacy datasets with inconsistent scope metadata.
    // Keeps search/tags behavior but removes scope constraints so contacts don't vanish.
    if (!contacts.length) {
      const globalConditions = [];
      if (search) {
        globalConditions.push({
          $or: [
            { name: { $regex: search, $options: 'i' } },
            { phone: { $regex: search, $options: 'i' } },
            { email: { $regex: search, $options: 'i' } }
          ]
        });
      }
      if (tags) {
        globalConditions.push({ tags: { $in: parseTagList(tags) } });
      }
      const globalFilters =
        globalConditions.length === 0
          ? {}
          : globalConditions.length === 1
            ? globalConditions[0]
            : { $and: globalConditions };
      contacts = await Contact.find(globalFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort({ lastContact: -1, createdAt: -1 })
        .lean();
      totalCount = await Contact.countDocuments(globalFilters);
    }

    contacts = contacts.map(normalizeContactConsentForResponse);

    res.set('X-Total-Count', String(totalCount || 0));
    res.json(contacts);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

router.post('/lookup', async (req, res) => {
  try {
    const phones = Array.isArray(req.body?.phones) ? req.body.phones : [];
    const phoneFilter = buildBulkPhoneMatchFilter(phones);

    if (!phoneFilter) {
      return res.json({ success: true, data: [] });
    }

    const filters = buildScopedContactFilter(req, phoneFilter);
    const contacts = await Contact.find(filters)
      .select(CONTACT_LIST_FIELDS)
      .lean();

    res.json({ success: true, data: contacts });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/:id/whatsapp-status', async (req, res) => {
  try {
    const contact = await Contact.findOne(
      buildScopedContactFilter(req, { _id: req.params.id })
    )
      .select(CONTACT_LIST_FIELDS)
      .lean();

    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    return res.json({
      success: true,
      data: {
        contact: normalizeContactConsentForResponse(contact),
        policy: getWhatsAppMessagingPolicy(contact)
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/:id/whatsapp-consent-audit', async (req, res) => {
  try {
    const contact = await Contact.findOne(
      buildScopedContactFilter(req, { _id: req.params.id })
    )
      .select(CONTACT_LIST_FIELDS)
      .lean();

    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const policy = getWhatsAppMessagingPolicy(contact);
    const normalizedContact = normalizeContactConsentForResponse(contact);
    const normalizedAuditStatus =
      policy.normalizedOptInStatus === 'opted_in'
        ? 'opted-in'
        : policy.normalizedOptInStatus === 'opted_out'
          ? 'opted-out'
          : 'unknown';
    const normalizedAuditSource =
      normalizedContact.whatsappOptInSource ||
      (normalizedAuditStatus === 'opted-in' ? 'landing_page' : null);

    return res.json({
      success: true,
      data: {
        contactId: normalizedContact._id,
        phone: normalizedContact.phone,
        whatsappOptInStatus: normalizedAuditStatus,
        whatsappOptInAt: normalizedContact.whatsappOptInAt || null,
        whatsappOptInSource: normalizedAuditSource,
        whatsappOptInScope: normalizedContact.whatsappOptInScope || null,
        whatsappOptInTextSnapshot: normalizedContact.whatsappOptInTextSnapshot || null,
        whatsappOptInProofType: normalizedContact.whatsappOptInProofType || null,
        whatsappOptInProofId: normalizedContact.whatsappOptInProofId || null,
        whatsappOptInProofUrl: normalizedContact.whatsappOptInProofUrl || null,
        whatsappOptInCapturedBy: normalizedContact.whatsappOptInCapturedBy || null,
        whatsappOptInPageUrl: normalizedContact.whatsappOptInPageUrl || null,
        whatsappOptInIp: normalizedContact.whatsappOptInIp || null,
        whatsappOptInUserAgent: normalizedContact.whatsappOptInUserAgent || null,
        whatsappOptInMetadata: normalizedContact.whatsappOptInMetadata || null,
        whatsappOptOutAt: normalizedContact.whatsappOptOutAt || null
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/whatsapp-opt-in', async (req, res) => {
  try {
    const contact = await Contact.findOne(buildScopedContactFilter(req, { _id: req.params.id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const auditPayload = buildOptInAuditPayload(req.body, req);
    if (!auditPayload.textSnapshot) {
      return res.status(400).json({
        success: false,
        error: 'Consent text is required before marking a contact as opted in.'
      });
    }
    if (!auditPayload.proofType) {
      return res.status(400).json({
        success: false,
        error: 'Proof type is required before marking a contact as opted in.'
      });
    }

    applyContactOptIn(contact, {
      source: auditPayload.source
    });
    applyOptInAuditPayload(contact, auditPayload);
    await contact.save();
    await logConsentEvent({
      contact,
      action: 'opt_in',
      payload: {
        source: auditPayload.source,
        scope: auditPayload.scope,
        consentText: auditPayload.textSnapshot,
        proofType: auditPayload.proofType,
        proofId: auditPayload.proofId,
        proofUrl: auditPayload.proofUrl,
        capturedBy: auditPayload.capturedBy,
        pageUrl: auditPayload.pageUrl,
        ip: auditPayload.ip,
        userAgent: auditPayload.userAgent,
        metadata: auditPayload.metadata
      }
    });

    emitCrmRealtimeEvent(req, {
      action: 'contact_opted_in',
      contactId: contact._id,
      phone: contact.phone
    });

    return res.json({
      success: true,
      data: {
        contact: toContactResponse(contact),
        policy: getWhatsAppMessagingPolicy(contact)
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/whatsapp-opt-out', async (req, res) => {
  try {
    const contact = await Contact.findOne(buildScopedContactFilter(req, { _id: req.params.id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    applyContactOptOut(contact, {
      source: toCleanString(req.body?.source) || 'manual'
    });
    await contact.save();
    await logConsentEvent({
      contact,
      action: 'opt_out',
      payload: {
        source: toCleanString(req.body?.source) || 'manual',
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

    emitCrmRealtimeEvent(req, {
      action: 'contact_opted_out',
      contactId: contact._id,
      phone: contact.phone
    });

    return res.json({
      success: true,
      data: {
        contact: toContactResponse(contact),
        policy: getWhatsAppMessagingPolicy(contact)
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/:id/whatsapp-reset-consent', async (req, res) => {
  try {
    const contact = await Contact.findOne(buildScopedContactFilter(req, { _id: req.params.id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    clearWhatsAppConsentFields(contact, {
      source: toCleanString(req.body?.source) || 'audit_review'
    });
    await contact.save();

    await logConsentEvent({
      contact,
      action: 'review_reset',
      payload: {
        source: toCleanString(req.body?.source) || 'audit_review',
        scope: 'unknown',
        consentText: '',
        proofType: '',
        proofId: '',
        proofUrl: '',
        capturedBy: toCleanString(req.body?.capturedBy || req.user?.email || req.user?.username || ''),
        pageUrl: '',
        ip: toCleanString(
          req.headers['x-forwarded-for']?.split(',')?.[0] ||
            req.ip ||
            req.socket?.remoteAddress
        ),
        userAgent: toCleanString(req.headers['user-agent']),
        metadata: {
          reason: toCleanString(req.body?.reason || 'Re-capture consent requested from audit review')
        }
      }
    });

    emitCrmRealtimeEvent(req, {
      action: 'contact_consent_reset',
      contactId: contact._id,
      phone: contact.phone
    });

    return res.json({
      success: true,
      data: {
        contact: toContactResponse(contact),
        policy: getWhatsAppMessagingPolicy(contact)
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

// Create new contact
router.post('/', async (req, res) => {
  try {
    const normalizedPayload = normalizeContactInput(req.body, 'manual');
    if (!normalizedPayload.phone) {
      return res.status(400).json({ error: 'Phone number is required' });
    }
    if (!isValidPhoneNumber(normalizedPayload.phone)) {
      return res.status(400).json({
        error: 'Phone number must contain 10 to 15 digits and be correctly formatted.'
      });
    }

    const existingContact = await Contact.findOne(
      buildScopedContactFilter(req, buildPhoneMatchFilter(normalizedPayload.phone) || {})
    );

    if (existingContact) {
      const mergedTags = getMergedTags(existingContact.tags, normalizedPayload.tags);
      const nextName = toCleanString(normalizedPayload.name) || existingContact.name;
      const nextEmail = toCleanString(normalizedPayload.email) || existingContact.email;
      const nextSource = toCleanString(normalizedPayload.source) || existingContact.source;

      existingContact.name = nextName;
      existingContact.email = nextEmail;
      existingContact.tags = mergedTags;
      existingContact.source = nextSource;
      existingContact.phone = getPreferredPhoneValue(normalizedPayload) || existingContact.phone;
      if (
        normalizedPayload.sourceType &&
        existingContact.sourceType === 'manual' &&
        normalizedPayload.sourceType !== 'manual'
      ) {
        existingContact.sourceType = normalizedPayload.sourceType;
      }

      if (
        normalizedPayload.customFields &&
        typeof normalizedPayload.customFields === 'object' &&
        !Array.isArray(normalizedPayload.customFields)
      ) {
        existingContact.customFields = {
          ...(existingContact.customFields && typeof existingContact.customFields === 'object'
            ? existingContact.customFields
            : {}),
          ...normalizedPayload.customFields
        };
      }

      await existingContact.save();
      emitCrmRealtimeEvent(req, {
        action: 'contact_updated',
        contactId: existingContact._id,
        phone: existingContact.phone
      });
      return res.status(200).json(toContactResponse(existingContact));
    }

    const contact = await Contact.create({
      ...normalizedPayload,
      userId: req.user.id,
      companyId: req.companyId || null,
      sourceType: normalizedPayload.sourceType || 'manual'
    });
    emitCrmRealtimeEvent(req, {
      action: 'contact_created',
      contactId: contact._id,
      phone: contact.phone
    });
    res.status(201).json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Import multiple contacts
router.post('/import', async (req, res) => {
  try {
    const { contacts } = req.body;
    
    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ error: 'Invalid contacts data' });
    }

    console.log(`📥 Importing ${contacts.length} contacts`);
    
    const results = {
      success: 0,
      failed: 0,
      warnings: [],
      errors: []
    };

    // Process contacts in batches to avoid overwhelming the database
    const batchSize = 50;
    for (let i = 0; i < contacts.length; i += batchSize) {
      const batch = contacts.slice(i, i + batchSize);
      
      for (const contactData of batch) {
        try {
          const normalizedContactData = normalizeImportedContactData(contactData);

          // Validate required fields
          if (!normalizedContactData.phone) {
            results.failed++;
            results.errors.push({
              line: normalizedContactData.lineNumber || 'Unknown',
              error: 'Phone number is required',
              data: normalizedContactData
            });
            continue;
          }

          const normalizedPhone = getPreferredPhoneValue(normalizedContactData);
          if (!normalizedPhone) {
            results.failed++;
            results.errors.push({
              line: normalizedContactData.lineNumber || 'Unknown',
              error: 'Phone number is required',
              data: normalizedContactData
            });
            continue;
          }
          if (!isValidPhoneNumber(normalizedPhone)) {
            results.failed++;
            results.errors.push({
              line: normalizedContactData.lineNumber || 'Unknown',
              error: 'Phone number must contain 10 to 15 digits and be correctly formatted.',
              data: normalizedContactData
            });
            continue;
          }

          // Check for duplicate phone numbers
          const existingContact = await Contact.findOne(
            buildScopedContactFilter(req, buildPhoneMatchFilter(normalizedPhone) || {})
          );
          const normalizedStatus = normalizeImportStatus(normalizedContactData.status);
          const effectiveImportStatus = 'opted-in';
          const importedConsentReferenceId = buildImportedConsentReferenceId(
            normalizedPhone,
            normalizedContactData.lineNumber
          );
          if (existingContact) {
            existingContact.name = toCleanString(normalizedContactData.name) || existingContact.name;
            existingContact.email = toCleanString(normalizedContactData.email) || existingContact.email;
            existingContact.tags = getMergedTags(existingContact.tags, normalizedContactData.tags);
            existingContact.phone = normalizedPhone;
            existingContact.sourceType = existingContact.sourceType || 'imported';
            applyImportedLandingPageConsent(existingContact, {
              referenceId: importedConsentReferenceId,
              scope: normalizedContactData.scope || 'marketing',
              lineNumber: normalizedContactData.lineNumber
            });
            await existingContact.save();
            await Contact.collection.updateOne(
              { _id: existingContact._id },
              { $set: buildImportedLandingPageConsentUpdate(existingContact, {
                referenceId: importedConsentReferenceId,
                scope: normalizedContactData.scope || 'marketing',
                lineNumber: normalizedContactData.lineNumber
              }) }
            );
            results.success++;
            emitCrmRealtimeEvent(req, {
              action: 'contact_updated',
              contactId: existingContact._id,
              phone: existingContact.phone
            });
            continue;
          }

          // Create contact
          const contact = await Contact.create({
            userId: req.user.id,
            companyId: req.companyId || null,
            name: normalizedContactData.name || '',
            phone: normalizedPhone,
            email: normalizedContactData.email || '',
            tags: Array.isArray(normalizedContactData.tags) ? normalizedContactData.tags : [],
            isBlocked: false,
            sourceType: 'imported',
            lastContact: new Date(),
            createdAt: new Date(),
            updatedAt: new Date()
          });

          applyImportedLandingPageConsent(contact, {
            referenceId: importedConsentReferenceId,
            scope: normalizedContactData.scope || 'marketing',
            lineNumber: normalizedContactData.lineNumber
          });

          await contact.save();
          await Contact.collection.updateOne(
            { _id: contact._id },
            { $set: buildImportedLandingPageConsentUpdate(contact, {
              referenceId: importedConsentReferenceId,
              scope: normalizedContactData.scope || 'marketing',
              lineNumber: normalizedContactData.lineNumber
            }) }
          );
          results.success++;
          emitCrmRealtimeEvent(req, {
            action: 'contact_created',
            contactId: contact._id,
            phone: contact.phone
          });
          
        } catch (error) {
          results.failed++;
          results.errors.push({
            line: contactData.lineNumber || 'Unknown',
            error: error.message,
            data: contactData
          });
        }
      }
    }

    console.log(`✅ Import completed: ${results.success} successful, ${results.failed} failed`);
    
    if (results.failed > 0) {
      console.log('❌ Import errors:', results.errors);
    }

    if (results.success > 0) {
      emitCrmRealtimeEvent(req, {
        action: 'contacts_imported',
        importedCount: results.success,
        failedCount: results.failed
      });
    }

    res.json({
      success: true,
      message: `Import completed: ${results.success} contacts imported successfully${results.failed > 0 ? `, ${results.failed} failed` : ''}`,
      results: {
        imported: results.success,
        failed: results.failed,
        warnings: results.warnings,
        errors: results.errors
      }
    });

  } catch (error) {
    console.error('❌ Import error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Import failed: ' + error.message 
    });
  }
});

// Update contact
router.put('/:id', async (req, res) => {
  try {
    const normalizedPayload = normalizeContactInput(req.body, 'manual');
    const existingContact = await Contact.findOne({
      _id: req.params.id,
      userId: req.user.id,
      ...(req.companyId ? { companyId: req.companyId } : {})
    });

    if (!existingContact) {
      return res.status(404).json({ error: 'Contact not found' });
    }

    const updatePayload = { ...normalizedPayload };
    const normalizedPhone = getPreferredPhoneValue(normalizedPayload);
    if (normalizedPayload.phone !== undefined && !normalizedPhone) {
      return res.status(400).json({ error: 'Phone number is required' });
    }
    if (normalizedPhone && !isValidPhoneNumber(normalizedPhone)) {
      return res.status(400).json({
        error: 'Phone number must contain 10 to 15 digits and be correctly formatted.'
      });
    }
    if (normalizedPhone) {
      const duplicateFilter = buildScopedContactFilter(req, {
        _id: { $ne: req.params.id },
        ...(buildPhoneMatchFilter(normalizedPhone) || {})
      });
      const duplicateContact = await Contact.findOne(duplicateFilter);
      if (duplicateContact) {
        return res.status(409).json({ error: 'Phone number already exists' });
      }
      updatePayload.phone = normalizedPhone;
    }
    if (updatePayload.customFields && typeof updatePayload.customFields === 'object') {
      updatePayload.customFields = {
        ...(existingContact.customFields && typeof existingContact.customFields === 'object' ? existingContact.customFields : {}),
        ...updatePayload.customFields
      };
    }

    const requestedOptInStatus = String(updatePayload.whatsappOptInStatus || '').trim().toLowerCase();
    if (requestedOptInStatus === 'opted_in') {
      const auditPayload = buildOptInAuditPayload(req.body, req);
      const existingOptInStatus = String(existingContact.whatsappOptInStatus || '').trim().toLowerCase();

      if (existingOptInStatus !== 'opted_in') {
        if (!auditPayload.textSnapshot) {
          return res.status(400).json({
            error: 'Consent text is required before marking a contact as opted in.'
          });
        }

        if (!auditPayload.proofType) {
          return res.status(400).json({
            error: 'Proof type is required before marking a contact as opted in.'
          });
        }
      }

      updatePayload.whatsappOptInStatus = 'opted_in';
      updatePayload.whatsappOptInAt = updatePayload.whatsappOptInAt || new Date();
      updatePayload.whatsappOptOutAt = null;
      updatePayload.isBlocked = false;
      updatePayload.whatsappOptInSource =
        toCleanString(updatePayload.whatsappOptInSource) || auditPayload.source || 'manual';
      updatePayload.whatsappOptInScope = normalizeOptInScope(updatePayload.whatsappOptInScope || auditPayload.scope);

      if (auditPayload.textSnapshot) {
        updatePayload.whatsappOptInTextSnapshot = auditPayload.textSnapshot;
      }
      if (auditPayload.proofType) {
        updatePayload.whatsappOptInProofType = auditPayload.proofType;
      }
      if (auditPayload.proofId) {
        updatePayload.whatsappOptInProofId = auditPayload.proofId;
      }
      if (auditPayload.proofUrl) {
        updatePayload.whatsappOptInProofUrl = auditPayload.proofUrl;
      }
      if (auditPayload.capturedBy) {
        updatePayload.whatsappOptInCapturedBy = auditPayload.capturedBy;
      }
      if (auditPayload.pageUrl) {
        updatePayload.whatsappOptInPageUrl = auditPayload.pageUrl;
      }
      if (auditPayload.ip) {
        updatePayload.whatsappOptInIp = auditPayload.ip;
      }
      if (auditPayload.userAgent) {
        updatePayload.whatsappOptInUserAgent = auditPayload.userAgent;
      }
      if (auditPayload.metadata) {
        updatePayload.whatsappOptInMetadata = auditPayload.metadata;
      }
    } else if (requestedOptInStatus === 'opted_out' || updatePayload.isBlocked === true) {
      updatePayload.whatsappOptInStatus = 'opted_out';
      updatePayload.whatsappOptOutAt = updatePayload.whatsappOptOutAt || new Date();
      updatePayload.isBlocked = true;
    }

    const contact = await Contact.findOneAndUpdate(
      buildScopedContactFilter(req, { _id: req.params.id }),
      updatePayload,
      { new: true, runValidators: true }
    );

    if (contact) {
      emitCrmRealtimeEvent(req, {
        action: 'contact_updated',
        contactId: contact._id,
        phone: contact.phone
      });
    }

    res.json(contact);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete contact
router.delete('/:id', async (req, res) => {
  try {
    const contact = await Contact.findOneAndDelete({
      _id: req.params.id,
      userId: req.user.id,
      ...(req.companyId ? { companyId: req.companyId } : {})
    });
    
    if (!contact) {
      return res.status(404).json({ error: 'Contact not found' });
    }

    emitCrmRealtimeEvent(req, {
      action: 'contact_deleted',
      contactId: contact._id,
      phone: contact.phone
    });

    res.json({ message: 'Contact deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
