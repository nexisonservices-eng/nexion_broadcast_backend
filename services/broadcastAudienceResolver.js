const mongoose = require('mongoose');
const Broadcast = require('../models/Broadcast');
const BroadcastDispatch = require('../models/BroadcastDispatch');
const AudienceSegment = require('../models/AudienceSegment');
const Contact = require('../models/Contact');
const {
  buildBroadcastAudienceValidation,
  toCleanString,
} = require('./whatsappOutreach/policy');

const normalizePhoneNumber = (phone = '') => String(phone || '').replace(/\D/g, '');

const normalizePhoneCandidates = (phone = '') => {
  const digits = normalizePhoneNumber(phone);
  if (!digits) return [];
  const values = new Set([digits]);
  if (digits.length > 10) {
    values.add(digits.slice(-10));
  }
  return Array.from(values);
};

const dedupeByPhone = (items = []) => {
  const map = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    const phone = toCleanString(item?.phone);
    const key = normalizePhoneNumber(phone);
    if (!key || map.has(key)) continue;
    map.set(key, { ...item, phone });
  }
  return Array.from(map.values());
};

const normalizeContact = (contact = {}, sourceType = 'manual') => ({
  phone: toCleanString(contact?.phone),
  name: toCleanString(contact?.name || ''),
  contactId: toCleanString(contact?._id || contact?.id || contact?.contactId),
  sourceType: toCleanString(contact?.sourceType || sourceType) || sourceType,
  whatsappOptInStatus: toCleanString(contact?.whatsappOptInStatus || 'unknown') || 'unknown',
  attributes:
    contact && typeof contact === 'object'
      ? contact
      : {},
});

const buildScopedQuery = ({ companyId = null, userId = null, extra = {} } = {}) => ({
  ...(companyId ? { companyId } : {}),
  ...(userId ? { userId } : {}),
  ...(extra && typeof extra === 'object' ? extra : {}),
});

const buildCrmQuery = ({ companyId = null, userId = null, filters = {} } = {}) => {
  const query = buildScopedQuery({ companyId, userId });
  const clauses = [];

  const search = toCleanString(filters?.search);
  if (search) {
    clauses.push({
      $or: [
        { nameLower: { $regex: search.toLowerCase(), $options: 'i' } },
        { phoneDigits: { $regex: normalizePhoneNumber(search), $options: 'i' } },
        { email: { $regex: search, $options: 'i' } },
      ],
    });
  }

  const tags = Array.isArray(filters?.tags)
    ? filters.tags
    : toCleanString(filters?.tags)
        .split(',')
        .map((value) => toCleanString(value))
        .filter(Boolean);
  if (tags.length) clauses.push({ tags: { $in: tags } });
  if (toCleanString(filters?.stage)) clauses.push({ stage: toCleanString(filters.stage) });
  if (toCleanString(filters?.status)) clauses.push({ status: toCleanString(filters.status) });
  if (toCleanString(filters?.leadStatus)) clauses.push({ leadStatus: toCleanString(filters.leadStatus) });
  if (toCleanString(filters?.assignedAgent)) clauses.push({ assignedAgent: toCleanString(filters.assignedAgent) });
  if (toCleanString(filters?.sourceType)) clauses.push({ sourceType: toCleanString(filters.sourceType) });
  if (toCleanString(filters?.whatsappOptInStatus)) {
    clauses.push({ whatsappOptInStatus: toCleanString(filters.whatsappOptInStatus) });
  }

  if (toCleanString(filters?.excludeBlocked).toLowerCase() !== 'false') {
    clauses.push({ isBlocked: { $ne: true } });
  }

  if (!clauses.length) return query;
  return { ...query, $and: clauses };
};

const loadContactsByPhones = async ({ companyId = null, userId = null, phones = [] } = {}) => {
  const uniquePhones = Array.from(
    new Set((Array.isArray(phones) ? phones : []).map((phone) => toCleanString(phone)).filter(Boolean)),
  );
  if (!uniquePhones.length) return [];

  const normalizedDigits = Array.from(
    new Set(uniquePhones.flatMap((phone) => normalizePhoneCandidates(phone))),
  );

  return Contact.find(
    buildScopedQuery({
      companyId,
      userId,
      extra: {
        $or: [
          { phone: { $in: uniquePhones } },
          ...(normalizedDigits.length ? [{ phoneDigits: { $in: normalizedDigits } }] : []),
        ],
      },
    }),
  )
    .select('_id name phone phoneDigits sourceType whatsappOptInStatus isBlocked tags stage leadStatus')
    .lean();
};

const loadSegmentRecipients = async ({ companyId = null, userId = null, segmentId = '' } = {}) => {
  const id = toCleanString(segmentId);
  if (!id) return null;

  const segment = await AudienceSegment.findOne(
    buildScopedQuery({
      companyId,
      userId,
      extra: mongoose.Types.ObjectId.isValid(id) ? { _id: id } : { _id: id },
    }),
  ).lean();

  if (!segment) return null;

  return {
    source: segment,
    recipients: Array.isArray(segment.contacts)
      ? segment.contacts.map((contact) => normalizeContact(contact, 'segment'))
      : [],
  };
};

const loadBroadcastRecipients = async ({ companyId = null, userId = null, broadcastId = '' } = {}) => {
  const id = toCleanString(broadcastId);
  if (!id) return null;

  const broadcast = await Broadcast.findOne(
    buildScopedQuery({
      companyId,
      userId,
      extra: mongoose.Types.ObjectId.isValid(id) ? { _id: id } : { _id: id },
    }),
  )
    .select('_id name companyId createdById recipients audienceSource audienceSnapshot recipientCount messageType templateCategory')
    .lean();

  if (!broadcast) return null;

  const directRecipients = Array.isArray(broadcast.recipients)
    ? broadcast.recipients.map((recipient) => normalizeContact(recipient, 'broadcast'))
    : [];
  if (directRecipients.length) {
    return { source: broadcast, recipients: directRecipients };
  }

  const dispatches = await BroadcastDispatch.find({
    broadcastId: broadcast._id,
    ...(companyId || broadcast.companyId ? { companyId: companyId || broadcast.companyId } : {}),
  })
    .select('recipientPhone recipientIndex status sentAt failedAt whatsappMessageId messageText messageKind templateName templateLanguage')
    .sort({ recipientIndex: 1, _id: 1 })
    .lean();

  return {
    source: broadcast,
    recipients: dispatches.map((dispatch) => ({
      phone: toCleanString(dispatch?.recipientPhone),
      name: '',
      contactId: '',
      sourceType: 'broadcast',
      whatsappOptInStatus: 'unknown',
      attributes: {
        dispatchId: String(dispatch?._id || ''),
        broadcastId: String(dispatch?.broadcastId || ''),
        recipientIndex: Number(dispatch?.recipientIndex || 0),
        status: toCleanString(dispatch?.status || ''),
        sentAt: dispatch?.sentAt || null,
        failedAt: dispatch?.failedAt || null,
        whatsappMessageId: toCleanString(dispatch?.whatsappMessageId || ''),
        messageText: toCleanString(dispatch?.messageText || ''),
        messageKind: toCleanString(dispatch?.messageKind || ''),
        templateName: toCleanString(dispatch?.templateName || ''),
        templateLanguage: toCleanString(dispatch?.templateLanguage || ''),
      },
    })),
  };
};

const loadCsvRecipients = async ({ companyId = null, userId = null, importJobId = '' } = {}) => {
  const id = toCleanString(importJobId);
  if (!id) return [];

  const contacts = await Contact.find(
    buildScopedQuery({
      companyId,
      userId,
      extra: { importJobId: id },
    }),
  )
    .select('_id name phone phoneDigits sourceType whatsappOptInStatus isBlocked tags stage leadStatus importJobId')
    .lean();

  return contacts.map((contact) => normalizeContact(contact, 'csv_import'));
};

const resolveAudienceRecipients = async ({
  companyId = null,
  userId = null,
  mode = 'manual_contacts',
  segmentId = '',
  broadcastId = '',
  importJobId = '',
  contacts = [],
  contactIds = [],
  filters = {},
} = {}) => {
  const normalizedMode = toCleanString(mode || 'manual_contacts').toLowerCase();
  let source = null;
  let rawRecipients = [];

  if (normalizedMode === 'saved_group' || normalizedMode === 'segment') {
    const result = await loadSegmentRecipients({ companyId, userId, segmentId });
    source = result?.source || null;
    rawRecipients = result?.recipients || [];
  } else if (normalizedMode === 'past_broadcast' || normalizedMode === 'broadcast') {
    const result = await loadBroadcastRecipients({ companyId, userId, broadcastId });
    source = result?.source || null;
    rawRecipients = result?.recipients || [];
  } else if (normalizedMode === 'csv_upload' || normalizedMode === 'csv') {
    rawRecipients = await loadCsvRecipients({ companyId, userId, importJobId });
  } else if (normalizedMode === 'crm_filter') {
    const rows = await Contact.find(
      buildCrmQuery({ companyId, userId, filters }),
    )
      .select('_id name phone phoneDigits sourceType whatsappOptInStatus isBlocked tags stage leadStatus')
      .sort({ updatedAt: -1, createdAt: -1, _id: -1 })
      .limit(Math.max(1, Math.min(5000, Number(filters?.limit || 1000))))
      .lean();
    rawRecipients = rows.map((contact) => normalizeContact(contact, 'crm_filter'));
  } else {
    if (Array.isArray(contacts) && contacts.length) {
      rawRecipients = contacts.map((contact) => normalizeContact(contact, 'manual'));
    } else if (Array.isArray(contactIds) && contactIds.length) {
      const rows = await Contact.find(
        buildScopedQuery({
          companyId,
          userId,
          extra: { _id: { $in: contactIds.map((value) => toCleanString(value)).filter(Boolean) } },
        }),
      )
        .select('_id name phone phoneDigits sourceType whatsappOptInStatus isBlocked tags stage leadStatus')
        .lean();
      rawRecipients = rows.map((contact) => normalizeContact(contact, 'manual'));
    }
  }

  const recipients = dedupeByPhone(rawRecipients);
  const contactPhones = recipients.map((recipient) => recipient.phone).filter(Boolean);
  const matchedContacts = await loadContactsByPhones({ companyId, userId, phones: contactPhones });
  const contactsByPhone = new Map();

  for (const contact of matchedContacts) {
    const exactPhone = toCleanString(contact?.phone);
    const digits = normalizePhoneNumber(contact?.phone || '');
    if (exactPhone) contactsByPhone.set(exactPhone, contact);
    if (digits) contactsByPhone.set(digits, contact);
  }

  const messageType = toCleanString(filters?.messageType || 'template').toLowerCase() || 'template';
  const templateCategory = toCleanString(filters?.templateCategory || '').toLowerCase();
  const validation = buildBroadcastAudienceValidation({
    recipients,
    contactsByPhone,
    messageType,
    templateCategory,
  });

  const eligibleRecipients = validation.eligibleRecipients.map((recipient) => {
    const matchedContact =
      contactsByPhone.get(toCleanString(recipient?.phone)) ||
      contactsByPhone.get(normalizePhoneNumber(recipient?.phone || '')) ||
      null;

    return {
      ...recipient,
      phone: toCleanString(recipient?.phone),
      name: toCleanString(recipient?.name || matchedContact?.name || ''),
      contactId: toCleanString(recipient?.contactId || matchedContact?._id || ''),
      sourceType:
        toCleanString(recipient?.sourceType || matchedContact?.sourceType || 'manual') ||
        'manual',
      whatsappOptInStatus:
        toCleanString(recipient?.whatsappOptInStatus || matchedContact?.whatsappOptInStatus || 'unknown') ||
        'unknown',
      attributes:
        recipient?.attributes && typeof recipient.attributes === 'object'
          ? recipient.attributes
          : matchedContact || {},
    };
  });

  return {
    mode: normalizedMode,
    source,
    recipients: eligibleRecipients,
    invalidRecipients: validation.invalidRecipients.map((item) => ({
      ...item,
      phone: toCleanString(item?.phone),
    })),
    summary: {
      selectedContactCount: recipients.length,
      validRecipientCount: eligibleRecipients.length,
      invalidRecipientCount: validation.summary.invalid,
      invalidPhoneCount: validation.summary.invalidPhone,
      missingContactCount: validation.summary.missingContact,
      optedOutCount: validation.summary.optedOut,
      freeformWindowClosedCount: validation.summary.freeformWindowClosed,
      missingMarketingOptInCount: validation.summary.missingMarketingOptIn,
      recentlyInteractedCount: validation.summary.recentlyInteracted,
      marketingRateLimitedCount: validation.summary.marketingRateLimited,
    },
  };
};

module.exports = {
  resolveAudienceRecipients,
  normalizePhoneNumber,
};
