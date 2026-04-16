const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');
const mongoose = require('mongoose');

const toCleanString = (value = '') => String(value || '').trim();
const toNullableObjectId = (value) => {
  const normalized = toCleanString(value);
  return mongoose.isValidObjectId(normalized) ? normalized : null;
};

const buildLogPayload = ({ contact, action, payload }) => {
  const resolvedContact = contact || {};
  const audit = payload || {};

  return {
    userId: toNullableObjectId(resolvedContact.userId || audit.userId || null),
    companyId: toNullableObjectId(resolvedContact.companyId || audit.companyId || null),
    contactId: toNullableObjectId(resolvedContact._id || audit.contactId || null),
    phone: toCleanString(resolvedContact.phone || audit.phone),
    action,
    source: toCleanString(audit.source || resolvedContact.whatsappOptInSource),
    scope: toCleanString(audit.scope || resolvedContact.whatsappOptInScope),
    consentText: toCleanString(audit.consentText || resolvedContact.whatsappOptInTextSnapshot),
    proofType: toCleanString(audit.proofType || resolvedContact.whatsappOptInProofType),
    proofId: toCleanString(audit.proofId || resolvedContact.whatsappOptInProofId),
    proofUrl: toCleanString(audit.proofUrl || resolvedContact.whatsappOptInProofUrl),
    capturedBy: toCleanString(audit.capturedBy || resolvedContact.whatsappOptInCapturedBy),
    pageUrl: toCleanString(audit.pageUrl || resolvedContact.whatsappOptInPageUrl),
    ip: toCleanString(audit.ip || resolvedContact.whatsappOptInIp),
    userAgent: toCleanString(audit.userAgent || resolvedContact.whatsappOptInUserAgent),
    metadata: audit.metadata || resolvedContact.whatsappOptInMetadata || null
  };
};

const logConsentEvent = async ({ contact, action, payload = {} }) => {
  if (!action || !contact) return null;
  if (mongoose.connection.readyState !== 1) return null;

  try {
    const logPayload = buildLogPayload({ contact, action, payload });
    return await WhatsAppConsentLog.create(logPayload);
  } catch (error) {
    console.error('Failed to write WhatsApp consent log:', error?.message || error);
    return null;
  }
};

module.exports = {
  logConsentEvent
};
