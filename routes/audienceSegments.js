const express = require('express');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');
const AudienceSegment = require('../models/AudienceSegment');
const { toCleanString } = require('../services/whatsappOutreach/policy');

const router = express.Router();

router.use(auth);
router.use(
  requireTenantPolicy({
    requiredFeatures: ['contacts'],
    auditEvent: 'audience_segment_policy'
  })
);

const buildScopedFilter = (req, extra = {}) => {
  const conditions = [{ userId: req.user.id }];
  if (req.companyId) {
    conditions.push({ companyId: req.companyId });
  }
  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }
  return conditions.length === 1 ? conditions[0] : { $and: conditions };
};

const normalizeContacts = (contacts = []) =>
  (Array.isArray(contacts) ? contacts : [])
    .map((contact) => ({
      contactId: toCleanString(contact?._id || contact?.id || contact?.contactId),
      phone: toCleanString(contact?.phone),
      name: toCleanString(contact?.name || contact?.displayName || contact?.contactName),
      sourceType: toCleanString(contact?.sourceType || 'manual') || 'manual',
      whatsappOptInStatus: toCleanString(contact?.whatsappOptInStatus || 'unknown') || 'unknown'
    }))
    .filter((contact) => contact.phone);

const normalizeFilters = (filters = {}) =>
  filters && typeof filters === 'object' && !Array.isArray(filters) ? filters : {};

const buildSegmentPayload = (segment) => ({
  id: String(segment?._id || ''),
  _id: String(segment?._id || ''),
  name: String(segment?.name || ''),
  description: String(segment?.description || ''),
  filters: normalizeFilters(segment?.filters),
  recipientCount: Number(segment?.recipientCount || 0) || 0,
  contacts: Array.isArray(segment?.contacts) ? segment.contacts : [],
  createdAt: segment?.createdAt || null,
  updatedAt: segment?.updatedAt || null
});

router.get('/', async (req, res) => {
  try {
    const { search } = req.query || {};
    const query = buildScopedFilter(req);

    if (String(search || '').trim()) {
      query.name = { $regex: String(search).trim(), $options: 'i' };
    }

    const segments = await AudienceSegment.find(query)
      .sort({ updatedAt: -1, createdAt: -1 })
      .lean();

    return res.json({
      success: true,
      data: segments.map(buildSegmentPayload)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to load audience segments'
    });
  }
});

router.post('/', async (req, res) => {
  try {
    const name = toCleanString(req.body?.name);
    const description = toCleanString(req.body?.description);
    const contacts = normalizeContacts(req.body?.contacts);
    const filters = normalizeFilters(req.body?.filters);
    const segmentId = toCleanString(req.body?.id || req.body?._id);

    if (!name) {
      return res.status(400).json({
        success: false,
        error: 'Segment name is required'
      });
    }

    if (!contacts.length) {
      return res.status(400).json({
        success: false,
        error: 'At least one contact is required to save a segment'
      });
    }

    const payload = {
      name,
      description,
      userId: req.user.id,
      companyId: req.companyId || null,
      filters,
      contacts,
      recipientCount: contacts.length,
      updatedBy: String(req.user.username || req.user.email || req.user.id || '').trim()
    };

    const scopedFilter = buildScopedFilter(req, segmentId ? { _id: segmentId } : { name });
    const updateOptions = {
      upsert: true,
      new: true,
      runValidators: true,
      setDefaultsOnInsert: true
    };

    const saved = await AudienceSegment.findOneAndUpdate(scopedFilter, payload, updateOptions).lean();

    return res.status(segmentId ? 200 : 201).json({
      success: true,
      data: buildSegmentPayload(saved)
    });
  } catch (error) {
    if (error?.code === 11000) {
      return res.status(409).json({
        success: false,
        error: 'An audience segment with this name already exists'
      });
    }

    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to save audience segment'
    });
  }
});

router.delete('/:id', async (req, res) => {
  try {
    const segmentId = toCleanString(req.params.id);
    if (!segmentId) {
      return res.status(400).json({
        success: false,
        error: 'Segment id is required'
      });
    }

    const result = await AudienceSegment.deleteOne(buildScopedFilter(req, { _id: segmentId }));

    if (!result.deletedCount) {
      return res.status(404).json({
        success: false,
        error: 'Audience segment not found'
      });
    }

    return res.json({
      success: true,
      message: 'Audience segment deleted successfully'
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to delete audience segment'
    });
  }
});

module.exports = router;
