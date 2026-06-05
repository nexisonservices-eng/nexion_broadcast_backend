const express = require('express');
const multer = require('multer');
const mongoose = require('mongoose');
const sharp = require('sharp');
const { createWorker } = require('tesseract.js');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const auth = require('../middleware/auth');
const { normalizeRole, isTenantWideRole } = require('../utils/accessControl');
const {
  applyContactOptIn,
  applyContactOptOut,
  getWhatsAppMessagingPolicy,
  toCleanString
} = require('../services/whatsappOutreach/policy');
const { logConsentEvent } = require('../services/whatsappConsentLogService');
const { invalidateInboxScope } = require('../utils/teamInboxCache');
const { buildContactSearchPlan } = require('../utils/contactSearchPlan');
const { buildPhoneCandidates } = require('../services/whatsappOutreach/conversationResolver');
const {
  buildContactPhoneLookupFilter,
  buildContactIdentityScopeFilter,
  mergeFilters,
  normalizePhoneKey
} = require('../utils/contactIdentity');
const { extractBusinessCardFields } = require('../utils/businessCardParser');

const router = express.Router();
router.use(auth);

const CARD_SCAN_UPLOAD_LIMIT_BYTES = 12 * 1024 * 1024;
const OCR_IMAGE_MAX_WIDTH = 1800;
const OCR_IMAGE_QUALITY_THRESHOLD = 160;
const ocrUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: CARD_SCAN_UPLOAD_LIMIT_BYTES
  },
  fileFilter: (req, file, callback) => {
    if (!file?.mimetype || !String(file.mimetype).startsWith('image/')) {
      return callback(new Error('Please upload an image file for OCR scanning.'));
    }
    callback(null, true);
  }
});

let ocrWorkerPromise = null;

const getOcrWorker = async () => {
  if (!ocrWorkerPromise) {
    ocrWorkerPromise = (async () => {
      const worker = await createWorker('eng');
      if (worker?.setParameters) {
        await worker.setParameters({
          preserve_interword_spaces: '1'
        });
      }
      return worker;
    })().catch((error) => {
      ocrWorkerPromise = null;
      throw error;
    });
  }

  return ocrWorkerPromise;
};

const escapeRegExp = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const normalizeEmailValue = (value = '') => String(value || '').trim().toLowerCase();

const buildContactDuplicateLookup = (phone = '', email = '') => {
  const conditions = [];
  const phoneCandidates = getPhoneLookupCandidates(phone);
  if (phoneCandidates.length) {
    conditions.push({
      $or: [
        { phone: { $in: phoneCandidates } },
        { phoneDigits: { $in: phoneCandidates.map((candidate) => normalizePhoneNumber(candidate)) } }
      ]
    });
  }

  const normalizedEmail = normalizeEmailValue(email);
  if (normalizedEmail) {
    conditions.push({
      email: {
        $regex: `^${escapeRegExp(normalizedEmail)}$`,
        $options: 'i'
      }
    });
  }

  return conditions.length ? { $or: conditions } : null;
};

const preprocessCardImage = async (buffer) => {
  const image = sharp(buffer).rotate();
  const metadata = await image.metadata();
  const width = Math.max(1, Number(metadata.width || OCR_IMAGE_MAX_WIDTH));
  const targetWidth = Math.min(width, OCR_IMAGE_MAX_WIDTH);
  return image
    .resize({
      width: targetWidth,
      withoutEnlargement: true
    })
    .grayscale()
    .normalize()
    .threshold(OCR_IMAGE_QUALITY_THRESHOLD)
    .sharpen()
    .png()
    .toBuffer();
};

const emitCrmRealtimeEvent = (req, payload = {}) => {
  const sendToUser = req?.app?.locals?.sendToUser;
  const userId = toCleanString(req?.user?.id);
  const companyId = toCleanString(req?.companyId);

  if (typeof sendToUser === 'function' && userId) {
    sendToUser(userId, {
      type: 'crm_changed',
      scope: 'crm',
      timestamp: new Date().toISOString(),
      ...payload,
      contactId: toCleanString(payload?.contactId),
      phone: toCleanString(payload?.phone),
      action: toCleanString(payload?.action)
    });
  }

  void invalidateInboxScope({
    companyId,
    userId
  });
};

const CONTACT_LIST_FIELDS = [
  '_id',
  'name',
  'phone',
  'email',
  'companyName',
  'designation',
  'tags',
  'stage',
  'status',
  'leadStatus',
  'source',
  'createdBy',
  'ownerId',
  'assignedTo',
  'assignedAgent',
  'sourceType',
  'lastContact',
  'lastContactAt',
  'nextFollowUpAt',
  'followupDate',
  'notes',
  'internalNotes',
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

const isTenantWideContactRole = (role) => {
  const normalizedRole = normalizeRole(role);
  return ['superadmin', 'admin', 'manager'].includes(normalizedRole);
};

const getContactAgentScope = (req) => {
  const normalizedUserId = toCleanString(req?.user?.id);
  if (!normalizedUserId) return {};

  return {
    $or: [
<<<<<<< Updated upstream
=======
      { ownerId: normalizedUserId },
>>>>>>> Stashed changes
      { assignedTo: normalizedUserId },
      { assignedAgent: normalizedUserId }
    ]
  };
};

const shouldAllowLegacyContactFallback = (req) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  return isTenantWideContactRole(normalizedRole) && Boolean(req?.companyId);
};

const buildLegacyContactFallbackFilter = (req, { searchPlan, tags } = {}) => {
  if (!shouldAllowLegacyContactFallback(req)) return null;

  const legacyConditions = [
    {
      $or: [
        { userId: { $exists: false } },
<<<<<<< Updated upstream
        { userId: null }
=======
        { userId: null },
        { userId: '' }
>>>>>>> Stashed changes
      ]
    }
  ];

  legacyConditions.push({
    $or: [
      { companyId: req.companyId },
      { companyId: { $exists: false } },
      { companyId: null }
    ]
  });

  if (searchPlan?.fallbackClause) {
    legacyConditions.push(searchPlan.fallbackClause);
  }

  if (tags) {
    legacyConditions.push({ tags: { $in: parseTagList(tags) } });
  }

  return legacyConditions.length === 1 ? legacyConditions[0] : { $and: legacyConditions };
};

const buildPhoneMatchFilter = (value) => {
  return buildContactPhoneLookupFilter(value);
};

const buildTenantContactIdentityFilter = (req, extra = {}) =>
  mergeFilters(
    buildContactIdentityScopeFilter({
      companyId: req?.companyId,
      userId: req?.user?.id
    }),
    extra
  );

const canCurrentUserAccessContact = (req, contact = {}) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  if (isTenantWideContactRole(normalizedRole)) return true;

  const currentUserId = toCleanString(req?.user?.id);
  if (!currentUserId) return false;

  return [
    contact?.userId,
    contact?.createdBy,
    contact?.ownerId,
    contact?.assignedTo,
    contact?.assignedAgent
  ].some((value) => toCleanString(value) === currentUserId);
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

const encodeContactCursor = (contact = {}) => {
  const payload = {
    lastContact: contact?.lastContact || contact?.createdAt || null,
    createdAt: contact?.createdAt || null,
    id: String(contact?._id || '').trim()
  };

  if (!payload.id || !payload.lastContact) return '';
  return Buffer.from(JSON.stringify(payload)).toString('base64url');
};

const decodeContactCursor = (cursor = '') => {
  const normalizedCursor = String(cursor || '').trim();
  if (!normalizedCursor) return null;

  try {
    const decoded = JSON.parse(Buffer.from(normalizedCursor, 'base64url').toString('utf8'));
    const lastContact = new Date(decoded?.lastContact || '');
    const createdAt = new Date(decoded?.createdAt || decoded?.lastContact || '');
    const id = String(decoded?.id || '').trim();
    if (!id || Number.isNaN(lastContact.getTime())) return null;
    return {
      lastContact,
      createdAt: Number.isNaN(createdAt.getTime()) ? null : createdAt,
      id
    };
  } catch {
    const fallbackDate = new Date(normalizedCursor);
    if (Number.isNaN(fallbackDate.getTime())) return null;
    return {
      lastContact: fallbackDate,
      createdAt: null,
      id: ''
    };
  }
};

const buildContactCursorFilter = (cursor) => {
  if (!cursor?.lastContact) return {};
  const cursorLastContact = new Date(cursor.lastContact);
  if (Number.isNaN(cursorLastContact.getTime())) return {};
  const cursorCreatedAt =
    cursor.createdAt instanceof Date && !Number.isNaN(cursor.createdAt.getTime())
      ? cursor.createdAt
      : cursor.createdAt
        ? new Date(cursor.createdAt)
        : null;
  const cursorId = String(cursor.id || '').trim();

  return {
    $or: [
      { lastContact: { $lt: cursorLastContact } },
      cursorCreatedAt
        ? {
            lastContact: cursorLastContact,
            $or: [
              { createdAt: { $lt: cursorCreatedAt } },
              {
                createdAt: cursorCreatedAt,
                _id: cursorId ? { $lt: cursorId } : { $exists: true }
              }
            ]
          }
        : {
            lastContact: cursorLastContact,
            _id: cursorId ? { $lt: cursorId } : { $exists: true }
          }
    ]
  };
};

const buildContactListSortClause = (sortOption = '') => {
  const normalizedSort = String(sortOption || '').trim().toLowerCase();
  switch (normalizedSort) {
    case 'name-desc':
      return { nameLower: -1, lastContact: 1, createdAt: 1, _id: 1 };
    case 'last-active-asc':
      return { lastContact: 1, createdAt: 1, _id: 1 };
    case 'last-active-desc':
      return { lastContact: -1, createdAt: -1, _id: -1 };
    case 'name-asc':
    default:
      return { nameLower: 1, lastContact: -1, createdAt: -1, _id: -1 };
  }
};

const buildLastActiveFilterClause = (value = '') => {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized || normalized === 'all') return null;

  const dayMap = {
    '1day': 1,
    '2days': 2,
    '1week': 7,
    '1month': 30
  };

  const days = dayMap[normalized];
  if (!days) return null;

  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  return {
    $or: [
      { lastContact: { $gte: since } },
      { lastContactAt: { $gte: since } },
      { lastInboundMessageAt: { $gte: since } }
    ]
  };
};

const buildScopeCondition = (req) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const tenantWide = isTenantWideContactRole(normalizedRole);

  if (tenantWide) {
    if (req.companyId) {
      return { companyId: req.companyId };
    }
    return {};
  }

  const agentScope = getContactAgentScope(req);
  if (!Object.keys(agentScope).length) return {};

  return req.companyId ? { $and: [{ companyId: req.companyId }, agentScope] } : agentScope;
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
    companyName: toCleanString(
      getImportedFieldValue(contactData, [
        'companyName',
        'company name',
        'company',
        'organization',
        'organisation',
        'business name'
      ])
    ),
    designation: toCleanString(
      getImportedFieldValue(contactData, [
        'designation',
        'jobTitle',
        'job title',
        'title',
        'role',
        'position'
      ])
    ),
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

const resolveContactOwnership = (req, payload = {}) => {
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const currentUserId = toCleanString(req?.user?.id);
  const requestedAssignedTo = toCleanString(payload?.assignedTo || payload?.ownerId || payload?.assignedAgent);
  const requestedCreatedBy = toCleanString(payload?.createdBy);
  const requestedOwnerId = toCleanString(payload?.ownerId);
  const adminCanAssign = isTenantWideContactRole(normalizedRole);

  if (!currentUserId) {
    return {
      createdBy: requestedCreatedBy || null,
      assignedTo: requestedAssignedTo || null,
      ownerId: requestedOwnerId || requestedAssignedTo || null,
      assignedAgent: requestedAssignedTo || requestedOwnerId || null
    };
  }

  if (!adminCanAssign) {
    return {
      createdBy: currentUserId,
      assignedTo: currentUserId,
      ownerId: currentUserId,
      assignedAgent: currentUserId
    };
  }

  const assignedTo = requestedAssignedTo || null;
  return {
    createdBy: requestedCreatedBy || currentUserId,
    assignedTo,
    ownerId: requestedOwnerId || assignedTo || null,
    assignedAgent: assignedTo || requestedOwnerId || null
  };
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
    const requestedRecentlyInteracted =
      String(req.query.recentlyInteractedOnly || req.query.repliedOnly || '')
        .trim()
        .toLowerCase() === 'true';
    const requestedLastActiveFilter = String(
      req.query.activeFilter || req.query.lastActiveFilter || ''
    ).trim().toLowerCase();
    const requestedLeadStatus = String(req.query.leadStatus || '').trim().toLowerCase();
    const requestedSortOption = String(req.query.sort || req.query.sortOption || '')
      .trim()
      .toLowerCase();
    const marketingEligibleOnly =
      String(req.query.marketingEligible || req.query.marketingEligibleOnly || '').trim().toLowerCase() ===
      'true';
    const hasWhatsApp =
      String(req.query.hasWhatsApp || '').trim().toLowerCase() === 'true';
    const cursor = decodeContactCursor(req.query.cursor);
    const parsedPage = Number(req.query.page);
    const parsedPageSize = Number(req.query.pageSize || req.query.limit);
    const page = Number.isFinite(parsedPage) && parsedPage > 0 ? Math.floor(parsedPage) : 1;
    const pageSize = Number.isFinite(parsedPageSize) && parsedPage > 0
      ? Math.min(Math.floor(parsedPageSize), 200)
      : 10;
    const isPageMode = !cursor && (
      req.query.page !== undefined ||
      req.query.pageSize !== undefined ||
      req.query.sort !== undefined ||
      req.query.sortOption !== undefined ||
      req.query.activeFilter !== undefined ||
      req.query.lastActiveFilter !== undefined
    );
    const queryConditions = [];
    const searchPlan = buildContactSearchPlan(search);

    if (searchPlan.summaryClause) {
      queryConditions.push(searchPlan.summaryClause);
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

    if (requestedLastActiveFilter && requestedLastActiveFilter !== 'all') {
      const lastActiveClause = buildLastActiveFilterClause(requestedLastActiveFilter);
      if (lastActiveClause) {
        queryConditions.push(lastActiveClause);
      }
    }

    if (requestedLeadStatus && requestedLeadStatus !== 'all') {
      const normalizedLeadStatuses = new Set([
        'new_lead',
        'interested',
        'follow_up',
        'proposal_sent',
        'converted',
        'closed'
      ]);
      if (!normalizedLeadStatuses.has(requestedLeadStatus)) {
        return res.status(400).json({ error: 'Invalid lead status filter' });
      }
      queryConditions.push({ leadStatus: requestedLeadStatus });
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

    if (requestedRecentlyInteracted) {
      const now = new Date();
      const serviceWindowFallback = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      queryConditions.push({
        $or: [
          { serviceWindowClosesAt: { $gt: now } },
          { lastInboundMessageAt: { $gte: serviceWindowFallback } }
        ]
      });
    }

    const extraFilters =
      queryConditions.length === 0 ? {} : queryConditions.length === 1 ? queryConditions[0] : { $and: queryConditions };
    const filters = buildScopedContactFilter(req, extraFilters);
    const defaultSortClause = { lastContact: -1, createdAt: -1, _id: -1 };

    const fetchContactsPage = async ({
      queryFilters,
      countFilters = queryFilters,
      sortClause,
      requestedPage = 1,
      requestedPageSize = 10
    }) => {
      const totalCount = await Contact.countDocuments(countFilters);
      const totalPages = Math.max(1, Math.ceil(totalCount / requestedPageSize));
      const safePage = Math.min(Math.max(requestedPage, 1), totalPages);
      let contactQuery = Contact.find(queryFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort(sortClause)
        .skip((safePage - 1) * requestedPageSize)
        .limit(requestedPageSize);

      if (searchPlan.hint) {
        contactQuery = contactQuery.hint(searchPlan.hint);
      }

      const pageContacts = await contactQuery.lean();
      return {
        contacts: pageContacts,
        totalCount,
        totalPages,
        page: safePage
      };
    };

    if (isPageMode) {
      const sortClause = buildContactListSortClause(requestedSortOption);
      let pageResult = await fetchContactsPage({
        queryFilters: filters,
        sortClause,
        requestedPage: page,
        requestedPageSize: pageSize
      });

      if (!pageResult.contacts.length) {
        const legacyFilters = buildLegacyContactFallbackFilter(req, { searchPlan, tags });
        if (legacyFilters) {
          pageResult = await fetchContactsPage({
            queryFilters: legacyFilters,
            countFilters: legacyFilters,
            sortClause,
            requestedPage: page,
            requestedPageSize: pageSize
          });
        }
      }

      const contacts = pageResult.contacts.map(normalizeContactConsentForResponse);
      const hasMore = pageResult.page < pageResult.totalPages;

      res.set('X-Total-Count', String(pageResult.totalCount || 0));
      return res.json({
        success: true,
        data: contacts,
        meta: {
          limit: pageSize,
          page: pageResult.page,
          pageSize,
          totalCount: pageResult.totalCount || 0,
          totalPages: pageResult.totalPages,
          hasMore,
          nextCursor: null
        }
      });
    }

    if (cursor) {
      const cursorFilters = { ...filters, ...buildContactCursorFilter(cursor) };
      let contactQuery = Contact.find(cursorFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort(defaultSortClause);

      if (searchPlan.hint) {
        contactQuery = contactQuery.hint(searchPlan.hint);
      }

      contactQuery = contactQuery.limit(pageSize + 1);

      let contacts = await contactQuery.lean();
      let totalCount = await Contact.countDocuments(filters);
      let hasMore = false;
      if (contacts.length > pageSize) {
        hasMore = true;
        contacts = contacts.slice(0, pageSize);
      }

      if (!contacts.length) {
        const legacyFilters = buildLegacyContactFallbackFilter(req, { searchPlan, tags });
        if (legacyFilters) {
          contacts = await Contact.find(legacyFilters)
            .select(CONTACT_LIST_FIELDS)
            .sort(defaultSortClause)
            .limit(pageSize + 1)
            .lean();
          totalCount = await Contact.countDocuments(legacyFilters);
          if (contacts.length > pageSize) {
            hasMore = true;
            contacts = contacts.slice(0, pageSize);
          }
        }
      }

      contacts = contacts.map(normalizeContactConsentForResponse);

      res.set('X-Total-Count', String(totalCount || 0));
      return res.json({
        success: true,
        data: contacts,
        meta: {
          limit: pageSize,
          hasMore,
          nextCursor: hasMore ? encodeContactCursor(contacts[contacts.length - 1]) : null
        }
      });
    }

    let contactQuery = Contact.find(filters)
      .select(CONTACT_LIST_FIELDS)
      .sort(defaultSortClause);

    if (searchPlan.hint) {
      contactQuery = contactQuery.hint(searchPlan.hint);
    }

    let contacts = await contactQuery.lean();
    let totalCount = await Contact.countDocuments(filters);

    if (!contacts.length) {
      const legacyFilters = buildLegacyContactFallbackFilter(req, { searchPlan, tags });
      if (legacyFilters) {
        contacts = await Contact.find(legacyFilters)
          .select(CONTACT_LIST_FIELDS)
          .sort(defaultSortClause)
          .lean();
        totalCount = await Contact.countDocuments(legacyFilters);
      }
    }

    contacts = contacts.map(normalizeContactConsentForResponse);

    res.set('X-Total-Count', String(totalCount || 0));
    res.json({
      success: true,
      data: contacts,
      meta: {
        limit: null,
        hasMore: false,
        nextCursor: null,
        totalCount: totalCount || 0,
        totalPages: 1,
        page: 1,
        pageSize: contacts.length || 0
      }
    });
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
      buildTenantContactIdentityFilter(req, buildPhoneMatchFilter(normalizedPayload.phone) || {})
    );

    if (existingContact) {
      if (!canCurrentUserAccessContact(req, existingContact)) {
        return res.status(409).json({
          error: 'Contact already exists in this workspace and is assigned to another agent'
        });
      }

      const mergedTags = getMergedTags(existingContact.tags, normalizedPayload.tags);
      const nextName = toCleanString(normalizedPayload.name) || existingContact.name;
      const nextEmail = toCleanString(normalizedPayload.email) || existingContact.email;
      const nextCompanyName = toCleanString(normalizedPayload.companyName) || existingContact.companyName;
      const nextDesignation = toCleanString(normalizedPayload.designation) || existingContact.designation;
      const nextSource = toCleanString(existingContact.source) || toCleanString(normalizedPayload.source);

      existingContact.name = nextName;
      existingContact.email = nextEmail;
      existingContact.companyName = nextCompanyName;
      existingContact.designation = nextDesignation;
      existingContact.tags = mergedTags;
      existingContact.source = nextSource;
      existingContact.phone = getPreferredPhoneValue(normalizedPayload) || existingContact.phone;
      existingContact.sourceType = existingContact.sourceType || normalizedPayload.sourceType || 'manual';

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
      ...resolveContactOwnership(req, normalizedPayload),
      leadStatus: toCleanString(normalizedPayload.leadStatus).toLowerCase() || 'new_lead',
      followupDate: normalizedPayload.followupDate ? new Date(normalizedPayload.followupDate) : null,
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

    const batchSize = 250;
    for (let i = 0; i < contacts.length; i += batchSize) {
      const batch = contacts.slice(i, i + batchSize);
      const normalizedBatch = batch
        .map((contactData, index) => ({
          source: contactData,
          normalized: normalizeImportedContactData(contactData),
          lineNumber: contactData?.lineNumber || i + index + 1
        }))
        .filter((entry) => entry.normalized);

      const validRows = [];
      const seenBatchPhoneKeys = new Set();
      for (const entry of normalizedBatch) {
        const normalizedPhone = getPreferredPhoneValue(entry.normalized);
        if (!normalizedPhone) {
          results.failed++;
          results.errors.push({
            line: entry.lineNumber || 'Unknown',
            error: 'Phone number is required',
            data: entry.normalized
          });
          continue;
        }

        if (!isValidPhoneNumber(normalizedPhone)) {
          results.failed++;
          results.errors.push({
            line: entry.lineNumber || 'Unknown',
            error: 'Phone number must contain 10 to 15 digits and be correctly formatted.',
            data: entry.normalized
          });
          continue;
        }

        const phoneKey = normalizePhoneKey(normalizedPhone);
        if (phoneKey && seenBatchPhoneKeys.has(phoneKey)) {
          results.failed++;
          results.errors.push({
            line: entry.lineNumber || 'Unknown',
            error: 'Duplicate phone number in import batch',
            data: entry.normalized
          });
          continue;
        }
        if (phoneKey) {
          seenBatchPhoneKeys.add(phoneKey);
        }

        validRows.push({
          ...entry,
          normalizedPhone,
          phoneDigits: normalizePhoneNumber(normalizedPhone),
          phoneKey
        });
      }

      if (!validRows.length) {
        continue;
      }

      const lookupPhones = Array.from(
        new Set(validRows.flatMap((entry) => buildPhoneCandidates(entry.normalizedPhone)).filter(Boolean))
      );
      const lookupPhoneDigits = Array.from(
        new Set(validRows.map((entry) => entry.phoneDigits).filter(Boolean))
      );
      const lookupPhoneKeys = Array.from(
        new Set(validRows.map((entry) => entry.phoneKey).filter(Boolean))
      );
      const lookupIdentityFilters = validRows
        .map((entry) => buildPhoneMatchFilter(entry.normalizedPhone))
        .filter(Boolean);

      const existingContacts = lookupIdentityFilters.length || lookupPhones.length || lookupPhoneDigits.length || lookupPhoneKeys.length
        ? await Contact.find(
            buildTenantContactIdentityFilter(req, {
              $or: [
                ...lookupIdentityFilters,
                lookupPhones.length ? { phone: { $in: lookupPhones } } : null,
                lookupPhoneDigits.length ? { phoneDigits: { $in: lookupPhoneDigits } } : null,
                lookupPhoneKeys.length ? { phoneKey: { $in: lookupPhoneKeys } } : null
              ].filter(Boolean)
            })
          )
            .select('_id userId companyId createdBy ownerId assignedTo assignedAgent phone phoneDigits phoneKey tags name email source sourceType leadStatus followupDate lastContact whatsappOptInAt whatsappOptInSource whatsappOptInScope whatsappOptInTextSnapshot whatsappOptInProofType whatsappOptInProofId whatsappOptInProofUrl whatsappOptInCapturedBy whatsappOptInPageUrl whatsappOptInIp whatsappOptInUserAgent whatsappOptInMetadata isBlocked')
            .lean()
        : [];

      const existingMap = new Map();
      for (const contact of existingContacts) {
        const contactCandidates = buildPhoneCandidates(contact?.phone || '');
        contactCandidates.forEach((candidate) => {
          if (!existingMap.has(candidate)) {
            existingMap.set(candidate, contact);
          }
        });
        if (contact?.phoneDigits && !existingMap.has(contact.phoneDigits)) {
          existingMap.set(contact.phoneDigits, contact);
        }
        if (contact?.phoneKey && !existingMap.has(contact.phoneKey)) {
          existingMap.set(contact.phoneKey, contact);
        }
      }

      const operations = [];
      for (const entry of validRows) {
        try {
          const importedConsentReferenceId = buildImportedConsentReferenceId(
            entry.normalizedPhone,
            entry.lineNumber
          );
          const existingContact =
            existingMap.get(entry.phoneKey) ||
            existingMap.get(entry.phoneDigits) ||
            buildPhoneCandidates(entry.normalizedPhone).map((candidate) => existingMap.get(candidate)).find(Boolean) ||
            null;

          if (existingContact) {
            if (!canCurrentUserAccessContact(req, existingContact)) {
              results.failed++;
              results.errors.push({
                line: entry.lineNumber || 'Unknown',
                error: 'Contact already exists in this workspace and is assigned to another agent',
                data: entry.normalized
              });
              continue;
            }

            const mergedTags = getMergedTags(existingContact.tags, entry.normalized.tags);
            const updatedName = toCleanString(entry.normalized.name) || toCleanString(existingContact.name);
            const ownership = resolveContactOwnership(req, {
              assignedTo: existingContact.assignedTo || existingContact.ownerId || existingContact.assignedAgent,
              ownerId: existingContact.ownerId,
              createdBy: existingContact.createdBy
            });

            operations.push({
              updateOne: {
                filter: { _id: existingContact._id },
                update: {
                  $set: {
<<<<<<< Updated upstream
                    userId: existingContact.userId || req.user.id,
                    companyId: existingContact.companyId || req.companyId || null,
=======
                    userId: req.user.id,
                    companyId: req.companyId || null,
>>>>>>> Stashed changes
                    ...ownership,
                    name: updatedName,
                    nameLower: updatedName.toLowerCase(),
                    phone: existingContact.phone || entry.normalizedPhone,
                    phoneDigits: existingContact.phoneDigits || entry.phoneDigits,
                    phoneKey: existingContact.phoneKey || entry.phoneKey,
                    email: toCleanString(entry.normalized.email) || toCleanString(existingContact.email),
                    tags: mergedTags,
                    leadStatus: toCleanString(existingContact.leadStatus).toLowerCase() || 'new_lead',
                    followupDate: existingContact.followupDate || null,
<<<<<<< Updated upstream
                    source: toCleanString(existingContact.source),
=======
>>>>>>> Stashed changes
                    sourceType: existingContact.sourceType || 'imported',
                    isBlocked: false,
                    lastContact: existingContact.lastContact || new Date(),
                    updatedAt: new Date(),
                    ...buildImportedLandingPageConsentUpdate(existingContact, {
                      referenceId: importedConsentReferenceId,
                      scope: entry.normalized.scope || 'marketing',
                      lineNumber: entry.lineNumber
                    })
                  }
                },
                upsert: false
              }
            });
          } else {
            const newContactDoc = {
              userId: req.user.id,
              companyId: req.companyId || null,
              ...resolveContactOwnership(req, {
                leadStatus: 'new_lead'
              }),
              name: toCleanString(entry.normalized.name) || '',
              nameLower: toCleanString(entry.normalized.name).toLowerCase(),
              phone: entry.normalizedPhone,
              phoneDigits: entry.phoneDigits,
              phoneKey: entry.phoneKey,
              email: toCleanString(entry.normalized.email) || '',
              tags: Array.isArray(entry.normalized.tags) ? entry.normalized.tags : [],
              leadStatus: 'new_lead',
              isBlocked: false,
              sourceType: 'imported',
              lastContact: new Date(),
              createdAt: new Date(),
              updatedAt: new Date(),
              ...buildImportedLandingPageConsentUpdate(null, {
                referenceId: importedConsentReferenceId,
                scope: entry.normalized.scope || 'marketing',
                lineNumber: entry.lineNumber
              })
            };

            operations.push({
              insertOne: {
                document: newContactDoc
              }
            });
          }
        } catch (error) {
          results.failed++;
          results.errors.push({
            line: entry.lineNumber || 'Unknown',
            error: error.message,
            data: entry.normalized
          });
        }
      }

      if (!operations.length) {
        continue;
      }

      try {
        const bulkResult = await Contact.bulkWrite(operations, { ordered: false });
        results.success += Number(bulkResult?.matchedCount || 0) + Number(bulkResult?.upsertedCount || 0);
      } catch (error) {
        const bulkResult = error?.result?.result || error?.result || null;
        results.success += Number(bulkResult?.nMatched || bulkResult?.matchedCount || 0) +
          Number(bulkResult?.nUpserted || bulkResult?.upsertedCount || 0);
        results.failed += Number(bulkResult?.writeErrors?.length || 1);
        results.errors.push({
          line: 'batch',
          error: error.message,
          data: {
            batchStart: i,
            batchSize: batch.length
          }
        });
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
        failedCount: results.failed,
        batchSize
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

const normalizeScanContactPayload = (fields = {}) => ({
  name: toCleanString(fields.fullName),
  phone: toCleanString(fields.mobileNumber),
  email: toCleanString(fields.email),
  companyName: toCleanString(fields.companyName),
  designation: toCleanString(fields.designation),
  tags: []
});

const findScanDuplicateContacts = async (req, extractedContact = {}) => {
  const lookupFilter = buildContactDuplicateLookup(
    extractedContact.phone,
    extractedContact.email
  );

  if (!lookupFilter) {
    return [];
  }

  const duplicateContacts = await Contact.find(
    buildTenantContactIdentityFilter(req, lookupFilter)
  )
    .select('_id name phone email companyName designation tags stage status sourceType createdAt updatedAt')
    .lean();

  const normalizedPhoneCandidates = getPhoneLookupCandidates(extractedContact.phone);
  const normalizedPhoneDigits = normalizedPhoneCandidates.map((candidate) => normalizePhoneNumber(candidate));
  const normalizedEmail = normalizeEmailValue(extractedContact.email);

  return (Array.isArray(duplicateContacts) ? duplicateContacts : []).map((contact) => {
    const contactPhoneCandidates = getPhoneLookupCandidates(contact?.phone || '');
    const contactPhoneDigits = contactPhoneCandidates.map((candidate) => normalizePhoneNumber(candidate));
    const contactEmail = normalizeEmailValue(contact?.email || '');
    const matchReasons = [];

    if (
      normalizedPhoneCandidates.some((candidate) => contactPhoneCandidates.includes(candidate)) ||
      normalizedPhoneDigits.some((candidate) => contactPhoneDigits.includes(candidate))
    ) {
      matchReasons.push('phone');
    }

    if (normalizedEmail && contactEmail && normalizedEmail === contactEmail) {
      matchReasons.push('email');
    }

    return {
      ...contact,
      matchReasons: Array.from(new Set(matchReasons))
    };
  });
};

router.post('/scan-card', (req, res) => {
  ocrUpload.single('image')(req, res, async (error) => {
    if (error) {
      return res.status(400).json({
        success: false,
        error: error.message || 'Unable to process the uploaded image.'
      });
    }

    const startedAt = Date.now();
    try {
      if (!req.file?.buffer) {
        return res.status(400).json({
          success: false,
          error: 'Please upload a business card image.'
        });
      }

      const preprocessedImage = await preprocessCardImage(req.file.buffer);
      const worker = await getOcrWorker();
      const ocrResult = await worker.recognize(preprocessedImage);
      const rawText = String(ocrResult?.data?.text || '').trim();
      const parsedCard = extractBusinessCardFields(rawText);
      const extractedContact = normalizeScanContactPayload(parsedCard);
      const duplicateContacts = await findScanDuplicateContacts(req, extractedContact);

      return res.json({
        success: true,
        data: {
          contact: extractedContact,
          duplicates: duplicateContacts,
          rawText,
          lines: parsedCard.lines || [],
          inferenceSignals: parsedCard.inferenceSignals || {},
          timings: {
            totalMs: Date.now() - startedAt
          }
        }
      });
    } catch (scanError) {
      return res.status(500).json({
        success: false,
        error: scanError.message || 'Failed to scan business card'
      });
    }
  });
});

// Update contact
router.put('/:id', async (req, res) => {
  try {
    const normalizedPayload = normalizeContactInput(req.body, 'manual');
    const existingContact = await Contact.findOne(
      buildScopedContactFilter(req, { _id: req.params.id })
    );

    if (!existingContact) {
      return res.status(404).json({ error: 'Contact not found' });
    }

    const updatePayload = { ...normalizedPayload };
    if (!isTenantWideContactRole(normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role))) {
      delete updatePayload.ownerId;
      delete updatePayload.assignedTo;
      delete updatePayload.assignedAgent;
      delete updatePayload.createdBy;
    }
<<<<<<< Updated upstream
    if (req.body?.sourceType === undefined || existingContact.sourceType) {
      delete updatePayload.sourceType;
    }
=======
>>>>>>> Stashed changes
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
      const duplicateFilter = buildTenantContactIdentityFilter(req, {
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
      {
        ...updatePayload,
        ...(isTenantWideContactRole(normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role))
          ? {
              ...(req.body?.leadStatus !== undefined
                ? { leadStatus: toCleanString(req.body.leadStatus).toLowerCase() || 'new_lead' }
                : {}),
              ...(req.body?.followupDate !== undefined
                ? { followupDate: req.body.followupDate ? new Date(req.body.followupDate) : null }
                : {}),
              ...(req.body?.assignedTo !== undefined || req.body?.ownerId !== undefined
                ? resolveContactOwnership(req, req.body)
                : {})
            }
          : {})
      },
      { new: true, runValidators: true }
    );

    if (contact) {
      const conversationUpdate = {};
      if (normalizedPayload.name !== undefined) {
        conversationUpdate.contactName = contact.name || '';
      }
      if (normalizedPayload.phone !== undefined) {
        conversationUpdate.contactPhone = contact.phone || '';
      }
      if (Object.keys(conversationUpdate).length > 0) {
        const contactId = String(contact?._id || '').trim();
        if (mongoose.Types.ObjectId.isValid(contactId)) {
          await Conversation.updateMany(
            buildScopedContactFilter(req, { contactId }),
            conversationUpdate
          );
          await ConversationSummary.updateMany(
            buildScopedContactFilter(req, { contactId }),
            conversationUpdate
          );
        }
      }
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
    const contact = await Contact.findOneAndDelete(
      buildScopedContactFilter(req, { _id: req.params.id })
    );
    
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
