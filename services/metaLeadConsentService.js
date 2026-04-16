const axios = require('axios');
const Contact = require('../models/Contact');
const { getAccessContextForUser, GRAPH_BASE_URL } = require('./metaAuthService');
const { applyContactOptIn, toCleanString } = require('./whatsappOutreach/policy');
const { normalizePhoneDigits } = require('./whatsappOutreach/conversationResolver');
const { logConsentEvent } = require('./whatsappConsentLogService');

const DEFAULT_PHONE_KEYS = ['phone', 'phone number', 'mobile', 'mobile number', 'whatsapp', 'whatsapp number'];
const DEFAULT_NAME_KEYS = ['full name', 'name'];
const DEFAULT_EMAIL_KEYS = ['email', 'email address'];
const DEFAULT_CONSENT_KEYS = [
  'whatsapp consent',
  'whatsapp opt in',
  'whatsapp opt-in',
  'receive whatsapp updates',
  'consent'
];
const DEFAULT_APPROVED_VALUES = ['yes', 'true', 'checked', 'opted in', 'i agree', 'agree'];

const normalizeKey = (value = '') =>
  toCleanString(value)
    .toLowerCase()
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ');

const normalizeValue = (value = '') => normalizeKey(value);

const buildLeadFieldMap = (fieldData = []) => {
  const map = new Map();
  (Array.isArray(fieldData) ? fieldData : []).forEach((entry) => {
    const key = normalizeKey(entry?.name);
    const values = Array.isArray(entry?.values)
      ? entry.values.map((item) => toCleanString(item)).filter(Boolean)
      : [];
    if (key) {
      map.set(key, values);
    }
  });
  return map;
};

const resolveFirstFieldValue = (fieldMap, candidateKeys = []) => {
  for (const rawKey of candidateKeys) {
    const values = fieldMap.get(normalizeKey(rawKey));
    if (values?.length) {
      return values[0];
    }
  }
  return '';
};

const resolveConsentValue = (fieldMap, candidateKeys = [], approvedValues = []) => {
  const rawValue = resolveFirstFieldValue(fieldMap, candidateKeys);
  const normalizedRawValue = normalizeValue(rawValue);
  const normalizedApprovedValues = (approvedValues.length ? approvedValues : DEFAULT_APPROVED_VALUES).map(normalizeValue);

  return {
    rawValue,
    approved: normalizedApprovedValues.includes(normalizedRawValue)
  };
};

const fetchMetaLead = async ({ userId, leadId }) => {
  const accessContext = await getAccessContextForUser(userId);
  if (!accessContext?.accessToken) {
    const error = new Error('Meta access token is not configured for this user.');
    error.status = 400;
    throw error;
  }

  const apiVersion = String(accessContext.apiVersion || 'v22.0').trim();
  const response = await axios.get(`${GRAPH_BASE_URL}/${apiVersion}/${encodeURIComponent(leadId)}`, {
    params: {
      access_token: accessContext.accessToken,
      fields: 'id,created_time,field_data,ad_id,form_id,campaign_id'
    },
    timeout: 15000
  });

  return response.data || null;
};

const buildResolvedLeadPayload = (leadData, mapping = {}) => {
  const fieldMap = buildLeadFieldMap(leadData?.field_data);
  const phone = normalizePhoneDigits(
    resolveFirstFieldValue(fieldMap, mapping.phoneFieldKeys || DEFAULT_PHONE_KEYS)
  );
  const name = toCleanString(
    resolveFirstFieldValue(fieldMap, mapping.nameFieldKeys || DEFAULT_NAME_KEYS)
  );
  const email = toCleanString(
    resolveFirstFieldValue(fieldMap, mapping.emailFieldKeys || DEFAULT_EMAIL_KEYS)
  );
  const consent = resolveConsentValue(
    fieldMap,
    mapping.consentFieldKeys || DEFAULT_CONSENT_KEYS,
    mapping.consentApprovedValues || DEFAULT_APPROVED_VALUES
  );

  return {
    phone,
    name,
    email,
    consentRawValue: consent.rawValue,
    consentApproved: consent.approved,
    availableFields: Array.from(fieldMap.entries()).map(([fieldName, values]) => ({
      fieldName,
      values
    }))
  };
};

const syncMetaLeadConsent = async ({
  userId,
  leadId,
  companyId = null,
  mapping = {},
  capturedBy = 'meta_lead_sync'
}) => {
  const leadData = await fetchMetaLead({ userId, leadId });
  const resolvedLead = buildResolvedLeadPayload(leadData, mapping);

  if (!resolvedLead.phone) {
    const error = new Error('Meta lead is missing a phone number based on the provided field mapping.');
    error.status = 400;
    error.details = { leadId, availableFields: resolvedLead.availableFields };
    throw error;
  }

  if (!resolvedLead.consentApproved) {
    const error = new Error('Meta lead does not have a valid WhatsApp consent answer for the provided mapping.');
    error.status = 400;
    error.details = {
      leadId,
      consentRawValue: resolvedLead.consentRawValue,
      availableFields: resolvedLead.availableFields
    };
    throw error;
  }

  const scopedFilter = {
    userId: String(userId),
    phone: resolvedLead.phone
  };

  if (companyId) {
    scopedFilter.$or = [
      { companyId },
      { companyId: null },
      { companyId: { $exists: false } }
    ];
  }

  let contact = await Contact.findOne(scopedFilter);
  if (!contact) {
    contact = new Contact({
      userId: String(userId),
      companyId: companyId || null,
      phone: resolvedLead.phone,
      name: resolvedLead.name || resolvedLead.phone,
      email: resolvedLead.email || '',
      sourceType: 'meta_lead_ads'
    });
  } else {
    if (resolvedLead.name && !contact.name) {
      contact.name = resolvedLead.name;
    }
    if (resolvedLead.email && !contact.email) {
      contact.email = resolvedLead.email;
    }
  }

  const consentText =
    toCleanString(mapping.consentText) ||
    `Meta Lead Ads consent captured from form ${toCleanString(leadData?.form_id) || 'unknown form'}.`;

  applyContactOptIn(contact, { source: 'meta_lead_ads' });
  contact.isBlocked = false;
  contact.whatsappOptInSource = 'meta_lead_ads';
  contact.whatsappOptInScope = toCleanString(mapping.scope || 'marketing').toLowerCase() || 'marketing';
  contact.whatsappOptInTextSnapshot = consentText;
  contact.whatsappOptInProofType = 'meta_lead_ads';
  contact.whatsappOptInProofId = toCleanString(leadData?.id || leadId);
  contact.whatsappOptInCapturedBy = toCleanString(capturedBy) || 'meta_lead_sync';
  contact.whatsappOptInMetadata = {
    leadId: toCleanString(leadData?.id || leadId),
    formId: toCleanString(leadData?.form_id),
    adId: toCleanString(leadData?.ad_id),
    campaignId: toCleanString(leadData?.campaign_id),
    createdTime: toCleanString(leadData?.created_time),
    consentRawValue: resolvedLead.consentRawValue,
    availableFields: resolvedLead.availableFields
  };

  await contact.save();
  await logConsentEvent({
    contact,
    action: 'opt_in',
    payload: {
      source: 'meta_lead_ads',
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

  return {
    contact,
    leadData,
    resolvedLead
  };
};

module.exports = {
  DEFAULT_PHONE_KEYS,
  DEFAULT_NAME_KEYS,
  DEFAULT_EMAIL_KEYS,
  DEFAULT_CONSENT_KEYS,
  DEFAULT_APPROVED_VALUES,
  fetchMetaLead,
  buildResolvedLeadPayload,
  syncMetaLeadConsent
};
