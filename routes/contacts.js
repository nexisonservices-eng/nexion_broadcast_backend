const express = require('express');
const Contact = require('../models/Contact');
const auth = require('../middleware/auth');
const {
  applyContactOptIn,
  applyContactOptOut,
  getWhatsAppMessagingPolicy,
  toCleanString
} = require('../services/whatsappOutreach/policy');

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
  'whatsappOptOutAt',
  'lastInboundMessageAt',
  'serviceWindowClosesAt',
  'leadScore',
  'createdAt',
  'updatedAt'
].join(' ');

const buildScopedContactFilter = (req, extra = {}) => {
  const scopedConditions = [{ userId: req.user.id }];
  if (req.companyId) {
    scopedConditions.push({
      $or: [
        { companyId: req.companyId },
        { companyId: null },
        { companyId: { $exists: false } }
      ]
    });
  }

  if (extra && Object.keys(extra).length > 0) {
    scopedConditions.push(extra);
  }

  return scopedConditions.length === 1 ? scopedConditions[0] : { $and: scopedConditions };
};

const toContactResponse = (contact) => {
  if (!contact || typeof contact.toObject !== 'function') return contact;
  return contact.toObject();
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
    const conditions = [{ userId: req.user.id }];
    if (req.companyId) {
      conditions.push({
        $or: [
        { companyId: req.companyId },
        { companyId: null },
        { companyId: { $exists: false } }
      ]
      });
    }
    
    if (search) {
      conditions.push({
        $or: [
        { name: { $regex: search, $options: 'i' } },
        { phone: { $regex: search, $options: 'i' } },
        { email: { $regex: search, $options: 'i' } }
      ]
      });
    }
    
    if (tags) {
      conditions.push({ tags: { $in: tags.split(',') } });
    }

    const filters = conditions.length === 1 ? conditions[0] : { $and: conditions };
    const contacts = await Contact.find(filters)
      .select(CONTACT_LIST_FIELDS)
      .sort({ lastContact: -1, createdAt: -1 })
      .lean();
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
    const contact = await Contact.create({
      ...req.body,
      userId: req.user.id,
      companyId: req.companyId || null,
      sourceType: 'manual'
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

          // Check for duplicate phone numbers
          const existingContact = await Contact.findOne({
            phone: contactData.phone,
            userId: req.user.id,
            ...(req.companyId ? { companyId: req.companyId } : {})
          });
          if (existingContact) {
            results.failed++;
            results.errors.push({
              line: contactData.lineNumber || 'Unknown',
              error: 'Phone number already exists',
              data: contactData
            });
            continue;
          }

          // Create contact
          const contact = new Contact({
            userId: req.user.id,
            companyId: req.companyId || null,
            name: contactData.name || '',
            phone: contactData.phone,
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
    const existingContact = await Contact.findOne({
      _id: req.params.id,
      userId: req.user.id,
      ...(req.companyId ? { $or: [{ companyId: req.companyId }, { companyId: null }, { companyId: { $exists: false } }] } : {})
    });

    if (!existingContact) {
      return res.status(404).json({ error: 'Contact not found' });
    }

    const updatePayload = { ...req.body };
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
      { _id: req.params.id, userId: req.user.id },
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
      ...(req.companyId ? { $or: [{ companyId: req.companyId }, { companyId: null }, { companyId: { $exists: false } }] } : {})
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
