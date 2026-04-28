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

const applyOptInAuditPayload = (contact, auditPayload) => {
  if (!contact || !auditPayload) return;
  contact.whatsappOptInSource = auditPayload.source;
  contact.whatsappOptInScope = auditPayload.scope;
  contact.whatsappOptInTextSnapshot = auditPayload.textSnapshot;
  contact.whatsappOptInProofType = auditPayload.proofType;
  contact.whatsappOptInProofId = auditPayload.proofId;
  contact.whatsappOptInProofUrl = auditPayload.proofUrl;
  contact.whatsappOptInCapturedBy = auditPayload.capturedBy;
  contact.whatsappOptInPageUrl = auditPayload.pageUrl;
  contact.whatsappOptInIp = auditPayload.ip;
  contact.whatsappOptInUserAgent = auditPayload.userAgent;
  contact.whatsappOptInMetadata = auditPayload.metadata;
};

// Get all contacts
router.get('/', async (req, res) => {
  try {
    const { search, tags } = req.query;
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
      queryConditions.push({ tags: { $in: tags.split(',') } });
    }

    const extraFilters =
      queryConditions.length === 0 ? {} : queryConditions.length === 1 ? queryConditions[0] : { $and: queryConditions };
    const filters = buildScopedContactFilter(req, extraFilters);
    let contacts = await Contact.find(filters)
      .select(CONTACT_LIST_FIELDS)
      .sort({ lastContact: -1, createdAt: -1 })
      .lean();

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
        legacyConditions.push({ tags: { $in: tags.split(',') } });
      }

      const legacyFilters =
        legacyConditions.length === 1 ? legacyConditions[0] : { $and: legacyConditions };

      contacts = await Contact.find(legacyFilters)
        .select(CONTACT_LIST_FIELDS)
        .sort({ lastContact: -1, createdAt: -1 })
        .lean();
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
        globalConditions.push({ tags: { $in: tags.split(',') } });
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
    }

    res.json(contacts);
  } catch (error) {
    res.status(500).json({ error: error.message });
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
        contact,
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

    return res.json({
      success: true,
      data: {
        contactId: contact._id,
        phone: contact.phone,
        whatsappOptInStatus: contact.whatsappOptInStatus || 'unknown',
        whatsappOptInAt: contact.whatsappOptInAt || null,
        whatsappOptInSource: contact.whatsappOptInSource || null,
        whatsappOptInScope: contact.whatsappOptInScope || null,
        whatsappOptInTextSnapshot: contact.whatsappOptInTextSnapshot || null,
        whatsappOptInProofType: contact.whatsappOptInProofType || null,
        whatsappOptInProofId: contact.whatsappOptInProofId || null,
        whatsappOptInProofUrl: contact.whatsappOptInProofUrl || null,
        whatsappOptInCapturedBy: contact.whatsappOptInCapturedBy || null,
        whatsappOptInPageUrl: contact.whatsappOptInPageUrl || null,
        whatsappOptInIp: contact.whatsappOptInIp || null,
        whatsappOptInUserAgent: contact.whatsappOptInUserAgent || null,
        whatsappOptInMetadata: contact.whatsappOptInMetadata || null,
        whatsappOptOutAt: contact.whatsappOptOutAt || null
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
      return res.status(200).json(toContactResponse(existingContact));
    }

    const contact = await Contact.create({
      ...normalizedPayload,
      userId: req.user.id,
      companyId: req.companyId || null,
      sourceType: normalizedPayload.sourceType || 'manual'
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
      errors: []
    };

    // Process contacts in batches to avoid overwhelming the database
    const batchSize = 50;
    for (let i = 0; i < contacts.length; i += batchSize) {
      const batch = contacts.slice(i, i + batchSize);
      
      for (const contactData of batch) {
        try {
          // Validate required fields
          if (!contactData.phone) {
            results.failed++;
            results.errors.push({
              line: contactData.lineNumber || 'Unknown',
              error: 'Phone number is required',
              data: contactData
            });
            continue;
          }

          const normalizedPhone = getPreferredPhoneValue(contactData);
          if (!normalizedPhone) {
            results.failed++;
            results.errors.push({
              line: contactData.lineNumber || 'Unknown',
              error: 'Phone number is required',
              data: contactData
            });
            continue;
          }

          // Check for duplicate phone numbers
          const existingContact = await Contact.findOne(
            buildScopedContactFilter(req, buildPhoneMatchFilter(normalizedPhone) || {})
          );
          if (existingContact) {
            existingContact.name = toCleanString(contactData.name) || existingContact.name;
            existingContact.email = toCleanString(contactData.email) || existingContact.email;
            existingContact.tags = getMergedTags(existingContact.tags, contactData.tags);
            existingContact.phone = normalizedPhone;
            existingContact.sourceType = existingContact.sourceType || 'imported';
            await existingContact.save();
            results.success++;
            continue;
          }

          // Create contact
          const contact = new Contact({
            userId: req.user.id,
            companyId: req.companyId || null,
            name: contactData.name || '',
            phone: normalizedPhone,
            email: contactData.email || '',
            tags: Array.isArray(contactData.tags) ? contactData.tags : [],
            isBlocked: contactData.status === 'Opted-out',
            sourceType: 'imported',
            lastContact: new Date(),
            createdAt: new Date(),
            updatedAt: new Date()
          });

          const normalizedStatus = String(contactData.status || '').trim().toLowerCase();
          if (normalizedStatus === 'opted-out') {
            applyContactOptOut(contact, { source: 'import' });
          } else if (normalizedStatus === 'opted-in') {
            applyContactOptIn(contact, { source: 'import' });
            applyOptInAuditPayload(
              contact,
              buildOptInAuditPayload(
                {
                  source: 'import',
                  scope: 'marketing',
                  consentText: 'Imported with existing WhatsApp opt-in consent.',
                  proofType: 'import_record',
                  proofId: String(contactData.lineNumber || ''),
                  metadata: {
                    importLineNumber: contactData.lineNumber || null
                  }
                },
                req
              )
            );
          }

          await contact.save();
          results.success++;
          
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

    res.json({
      success: true,
      message: `Import completed: ${results.success} contacts imported successfully${results.failed > 0 ? `, ${results.failed} failed` : ''}`,
      results: {
        imported: results.success,
        failed: results.failed,
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
    
    res.json({ message: 'Contact deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
