const express = require('express');
const mongoose = require('mongoose');
const auth = require('../middleware/auth');
const { requireTenantPolicy } = require('../middleware/tenantPolicy');
const AudienceSegment = require('../models/AudienceSegment');
const Contact = require('../models/Contact');
const { toCleanString } = require('../services/whatsappOutreach/policy');

const router = express.Router();

router.use(auth);
router.use(
  requireTenantPolicy({
    requiredFeatures: ['contacts'],
    auditEvent: 'audience_segment_policy'
  })
);

const toObjectIdOrNull = (value = '') => {
  const normalized = toCleanString(value);
  if (!normalized || !mongoose.Types.ObjectId.isValid(normalized)) {
    return null;
  }
  return new mongoose.Types.ObjectId(normalized);
};

const toScopedMatchValue = (value = '') => toObjectIdOrNull(value) || toCleanString(value);

const buildScopedFilter = (req, extra = {}) => {
  const conditions = [{ userId: toScopedMatchValue(req.user.id) }];
  if (req.companyId) {
    conditions.push({ companyId: toScopedMatchValue(req.companyId) });
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

const normalizeContactIds = (contactIds = []) =>
  Array.from(
    new Set(
      (Array.isArray(contactIds) ? contactIds : [])
        .map((contactId) => toCleanString(contactId))
        .filter(Boolean)
    )
  );

const normalizeFilters = (filters = {}) =>
  filters && typeof filters === 'object' && !Array.isArray(filters) ? filters : {};

const buildSegmentPayload = (segment) => ({
  id: String(segment?._id || ''),
  _id: String(segment?._id || ''),
  name: String(segment?.name || ''),
  description: String(segment?.description || ''),
  sourceType: String(segment?.sourceType || 'manual'),
  filters: normalizeFilters(segment?.filters),
  recipientCount: Number(segment?.recipientCount || 0) || 0,
  contacts: Array.isArray(segment?.contacts) ? segment.contacts : [],
  createdAt: segment?.createdAt || null,
  updatedAt: segment?.updatedAt || null
});

const escapeRegex = (value = '') =>
  String(value || '')
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .trim();

const normalizePagination = (page, pageSize) => {
  const nextPage = Math.max(1, Number.parseInt(String(page || '1'), 10) || 1);
  const nextPageSize = Math.min(
    100,
    Math.max(1, Number.parseInt(String(pageSize || '20'), 10) || 20)
  );
  return { page: nextPage, pageSize: nextPageSize, skip: (nextPage - 1) * nextPageSize };
};

const buildContactsPageExpression = (searchTerm = '', skip = 0, limit = 20) => {
  const hasSearch = Boolean(String(searchTerm || '').trim());
  const regex = escapeRegex(searchTerm);
  const normalizedLimit = Math.max(1, Number(limit) || 20);
  const normalizedSkip = Math.max(0, Number(skip) || 0);

  const filteredContacts = hasSearch
    ? {
        $filter: {
          input: '$contacts',
          as: 'contact',
          cond: {
            $or: [
              { $regexMatch: { input: { $ifNull: ['$$contact.name', ''] }, regex, options: 'i' } },
              { $regexMatch: { input: { $ifNull: ['$$contact.phone', ''] }, regex, options: 'i' } },
              { $regexMatch: { input: { $ifNull: ['$$contact.sourceType', ''] }, regex, options: 'i' } }
            ]
          }
        }
      }
    : '$contacts';

  return {
    filteredContacts,
    totalCount: { $size: filteredContacts },
    pageContacts: { $slice: [filteredContacts, normalizedSkip, normalizedLimit] }
  };
};

const buildDedupContactsExpression = (baseExpression, incomingContacts = []) => ({
  $reduce: {
    input: { $concatArrays: [baseExpression, Array.isArray(incomingContacts) ? incomingContacts : []] },
    initialValue: [],
    in: {
      $let: {
        vars: {
          existingPhones: {
            $map: {
              input: '$$value',
              as: 'existingContact',
              in: '$$existingContact.phone'
            }
          }
        },
        in: {
          $cond: [
            { $in: ['$$this.phone', '$$existingPhones'] },
            '$$value',
            { $concatArrays: ['$$value', ['$$this']] }
          ]
        }
      }
    }
  }
});

const loadContactsByIds = async (req, contactIds = []) => {
  const ids = normalizeContactIds(contactIds);
  if (!ids.length) return [];

  const query = {
    _id: { $in: ids },
    userId: req.user.id
  };

  if (req.companyId) {
    query.companyId = req.companyId;
  }

  const contacts = await Contact.find(query)
    .select('_id phone name sourceType whatsappOptInStatus')
    .lean();

  return normalizeContacts(contacts);
};

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
      error: error?.message || 'Failed to load audience groups'
    });
  }
});

router.get('/:id/contacts', async (req, res) => {
  try {
    const segmentId = toCleanString(req.params.id);
    if (!segmentId) {
      return res.status(400).json({
        success: false,
        error: 'Group id is required'
      });
    }

    const segmentObjectId = toObjectIdOrNull(segmentId);
    if (!segmentObjectId) {
      return res.status(400).json({
        success: false,
        error: 'Invalid group id'
      });
    }

    const { page, pageSize, skip } = normalizePagination(req.query?.page, req.query?.pageSize);
    const search = toCleanString(req.query?.search);
    const pipeline = [
      { $match: buildScopedFilter(req, { _id: segmentObjectId }) },
      { $project: { contacts: 1, recipientCount: 1, name: 1, description: 1, updatedAt: 1, createdAt: 1 } },
      {
        $addFields: buildContactsPageExpression(search, skip, pageSize)
      },
      {
        $project: {
          contacts: '$pageContacts',
          totalCount: 1,
          recipientCount: 1,
          name: 1,
          description: 1,
          updatedAt: 1,
          createdAt: 1
        }
      }
    ];

    const [segment] = await AudienceSegment.aggregate(pipeline);

    if (!segment) {
      return res.status(404).json({
        success: false,
        error: 'Audience group not found'
      });
    }

    return res.json({
      success: true,
      data: {
        id: String(segment?._id || segmentId),
        _id: String(segment?._id || segmentId),
        name: String(segment?.name || ''),
        description: String(segment?.description || ''),
        contacts: Array.isArray(segment?.contacts) ? segment.contacts : [],
        totalCount: Number(segment?.totalCount || 0) || 0,
        recipientCount: Number(segment?.recipientCount || 0) || 0,
        page,
        pageSize
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to load group members'
    });
  }
});

router.post('/', async (req, res) => {
  try {
    const name = toCleanString(req.body?.name);
    const description = toCleanString(req.body?.description);
    const directContacts = normalizeContacts(req.body?.contacts);
    const contactIds = normalizeContactIds(req.body?.contactIds);
    const contacts = directContacts.length ? directContacts : await loadContactsByIds(req, contactIds);
    const filters = normalizeFilters(req.body?.filters);
    const segmentId = toCleanString(req.body?.id || req.body?._id);

    if (!name) {
      return res.status(400).json({
        success: false,
        error: 'Group name is required'
      });
    }

    if (!contacts.length) {
      return res.status(400).json({
        success: false,
        error: 'At least one contact is required to save a group'
      });
    }

    const payload = {
      name,
      description,
      sourceType: contactIds.length ? 'selected_contacts' : 'manual',
      userId: req.user.id,
      companyId: req.companyId || null,
      filters,
      contacts,
      recipientCount: contacts.length,
      createdBy: String(req.user.username || req.user.email || req.user.id || '').trim(),
      updatedBy: String(req.user.username || req.user.email || req.user.id || '').trim()
    };

    const scopedFilter = buildScopedFilter(
      req,
      segmentId ? { _id: toObjectIdOrNull(segmentId) || segmentId } : { name }
    );
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
        error: 'An audience group with this name already exists'
      });
    }

    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to save audience group'
    });
  }
});

router.patch('/:id', async (req, res) => {
  try {
    const segmentId = toCleanString(req.params.id);
    if (!segmentId) {
      return res.status(400).json({
        success: false,
        error: 'Group id is required'
      });
    }

    const name = toCleanString(req.body?.name);
    const description = toCleanString(req.body?.description);

    const update = {
      updatedBy: String(req.user.username || req.user.email || req.user.id || '').trim()
    };

    if (name) {
      update.name = name;
    }
    if (description !== undefined) {
      update.description = description;
    }

    const saved = await AudienceSegment.findOneAndUpdate(
      buildScopedFilter(req, { _id: toObjectIdOrNull(segmentId) || segmentId }),
      { $set: update },
      { new: true, runValidators: true }
    ).lean();

    if (!saved) {
      return res.status(404).json({
        success: false,
        error: 'Audience group not found'
      });
    }

    return res.json({
      success: true,
      data: buildSegmentPayload(saved)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to update audience group'
    });
  }
});

router.post('/:id/contacts', async (req, res) => {
  try {
    const segmentId = toCleanString(req.params.id);
    if (!segmentId) {
      return res.status(400).json({
        success: false,
        error: 'Group id is required'
      });
    }

    const directContacts = normalizeContacts(req.body?.contacts);
    const contactIds = normalizeContactIds(req.body?.contactIds);
    const contacts = directContacts.length ? directContacts : await loadContactsByIds(req, contactIds);

    if (!contacts.length) {
      return res.status(400).json({
        success: false,
        error: 'At least one contact is required to add to a group'
      });
    }

    const normalizedContacts = normalizeContacts(contacts);
    const dedupedContactsExpression = buildDedupContactsExpression('$contacts', normalizedContacts);
    const updated = await AudienceSegment.findOneAndUpdate(
      buildScopedFilter(req, { _id: toObjectIdOrNull(segmentId) || segmentId }),
      [
        {
          $set: {
            contacts: dedupedContactsExpression,
            recipientCount: { $size: dedupedContactsExpression },
            updatedBy: String(req.user.username || req.user.email || req.user.id || '').trim()
          }
        }
      ],
      { new: true, runValidators: true }
    ).lean();

    if (!updated) {
      return res.status(404).json({
        success: false,
        error: 'Audience group not found'
      });
    }

    return res.json({
      success: true,
      data: buildSegmentPayload(updated)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to add contacts to group'
    });
  }
});

router.delete('/:id/contacts', async (req, res) => {
  try {
    const segmentId = toCleanString(req.params.id);
    if (!segmentId) {
      return res.status(400).json({
        success: false,
        error: 'Group id is required'
      });
    }

    const phoneSet = new Set(
      (Array.isArray(req.body?.phones) ? req.body.phones : [])
        .map((phone) => toCleanString(phone))
        .filter(Boolean)
    );

    if (!phoneSet.size) {
      return res.status(400).json({
        success: false,
        error: 'At least one contact phone is required to delete'
      });
    }

    const phones = Array.from(phoneSet);
    const filteredContactsExpression = {
      $filter: {
        input: '$contacts',
        as: 'contact',
        cond: { $not: [{ $in: ['$$contact.phone', phones] }] }
      }
    };

    const updated = await AudienceSegment.findOneAndUpdate(
      buildScopedFilter(req, { _id: toObjectIdOrNull(segmentId) || segmentId }),
      [
        {
          $set: {
            contacts: filteredContactsExpression,
            recipientCount: { $size: filteredContactsExpression },
            updatedBy: String(req.user.username || req.user.email || req.user.id || '').trim()
          }
        }
      ],
      { new: true, runValidators: true }
    ).lean();

    if (!updated) {
      return res.status(404).json({
        success: false,
        error: 'Audience group not found'
      });
    }

    return res.json({
      success: true,
      data: buildSegmentPayload(updated)
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to delete contacts from group'
    });
  }
});

router.delete('/:id', async (req, res) => {
  try {
    const segmentId = toCleanString(req.params.id);
    if (!segmentId) {
      return res.status(400).json({
        success: false,
        error: 'Group id is required'
      });
    }

    const result = await AudienceSegment.deleteOne(
      buildScopedFilter(req, { _id: toObjectIdOrNull(segmentId) || segmentId })
    );

    if (!result.deletedCount) {
      return res.status(404).json({
        success: false,
        error: 'Audience group not found'
      });
    }

    return res.json({
      success: true,
      message: 'Audience group deleted successfully'
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to delete audience group'
    });
  }
});

module.exports = router;
