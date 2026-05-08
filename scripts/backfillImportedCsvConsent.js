require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Contact = require('../models/Contact');
const { logConsentEvent } = require('../services/whatsappConsentLogService');

const toCleanString = (value = '') => String(value || '').trim();

const isUnset = (value) =>
  value === undefined || value === null || String(value || '').trim() === '';

const buildBackfillReferenceId = (contact = {}) => {
  const phoneDigits = String(contact?.phone || '').replace(/\D/g, '').slice(-4) || 'contact';
  const contactToken = String(contact?._id || '').slice(-6) || 'row';
  const timeToken = Date.now().toString(36);
  const randomToken = Math.random().toString(36).slice(2, 8);
  return `csv-backfill-${contactToken}-${phoneDigits}-${timeToken}-${randomToken}`;
};

const shouldBackfillContact = (contact = {}) => {
  const status = toCleanString(contact?.whatsappOptInStatus).toLowerCase();
  const sourceType = toCleanString(contact?.sourceType).toLowerCase();
  const source = toCleanString(contact?.whatsappOptInSource).toLowerCase();
  const isBlocked = contact?.isBlocked === true;
  const hasOptOutEvidence = Boolean(contact?.whatsappOptOutAt);

  return (
    ['','imported'].includes(sourceType) &&
    !toCleanString(contact?.source).toLowerCase() &&
    (status === 'unknown' || isUnset(status)) &&
    !isBlocked &&
    !hasOptOutEvidence &&
    ['','unknown','csv_import','import'].includes(source)
  );
};

const backfillImportedContacts = async () => {
  await connectDB();

  const contacts = await Contact.find({
    $and: [
      {
        $or: [
          { whatsappOptInStatus: { $exists: false } },
          { whatsappOptInStatus: null },
          { whatsappOptInStatus: '' },
          { whatsappOptInStatus: 'unknown' }
        ]
      },
      {
        $or: [
          { sourceType: { $exists: false } },
          { sourceType: null },
          { sourceType: '' },
          { sourceType: 'imported' }
        ]
      },
      {
        $or: [
          { source: { $exists: false } },
          { source: null },
          { source: '' }
        ]
      }
    ]
  }).lean(false);

  let scanned = 0;
  let updated = 0;
  let skipped = 0;

  for (const contact of contacts) {
    scanned += 1;
    if (!shouldBackfillContact(contact)) {
      skipped += 1;
      continue;
    }

    const now = new Date();
    const referenceId = buildBackfillReferenceId(contact);
    const updatePayload = {
      whatsappOptInStatus: 'opted_in',
      whatsappOptInAt: now,
      whatsappOptInSource: 'landing_page',
      whatsappOptInScope: 'marketing',
      whatsappOptInTextSnapshot:
        contact.whatsappOptInTextSnapshot ||
        'Consent captured via website landing page during CSV import.',
      whatsappOptInProofType: 'import_record',
      whatsappOptInProofId: referenceId,
      whatsappOptInProofUrl: contact.whatsappOptInProofUrl || '',
      whatsappOptInCapturedBy: contact.whatsappOptInCapturedBy || 'csv_import_backfill',
      whatsappOptInPageUrl: contact.whatsappOptInPageUrl || '',
      whatsappOptInIp: contact.whatsappOptInIp || '',
      whatsappOptInUserAgent: contact.whatsappOptInUserAgent || '',
      whatsappOptInMetadata: {
        ...(contact.whatsappOptInMetadata && typeof contact.whatsappOptInMetadata === 'object'
          ? contact.whatsappOptInMetadata
          : {}),
        backfilledAt: now.toISOString(),
        backfillSource: 'csv_import_backfill',
        consentSource: 'landing_page'
      },
      whatsappOptOutAt: null,
      isBlocked: false
    };

    await Contact.collection.updateOne(
      { _id: contact._id },
      { $set: updatePayload }
    );

    const updatedContact = {
      ...contact.toObject(),
      ...updatePayload
    };

    updatedContact.whatsappOptInMetadata = {
      ...(contact.whatsappOptInMetadata && typeof contact.whatsappOptInMetadata === 'object'
        ? contact.whatsappOptInMetadata
        : {}),
      backfilledAt: now.toISOString(),
      backfillSource: 'csv_import_backfill',
      consentSource: 'landing_page'
    };
    updated += 1;

    await logConsentEvent({
      contact: updatedContact,
      action: 'opt_in',
      payload: {
        source: 'landing_page',
        scope: 'marketing',
        consentText: updatedContact.whatsappOptInTextSnapshot,
        proofType: updatedContact.whatsappOptInProofType,
        proofId: updatedContact.whatsappOptInProofId,
        capturedBy: updatedContact.whatsappOptInCapturedBy,
        metadata: updatedContact.whatsappOptInMetadata
      }
    });
  }

  console.log(
    `Backfill complete. Scanned: ${scanned}, updated: ${updated}, skipped: ${skipped}`
  );

  await mongoose.connection.close();
};

backfillImportedContacts().catch(async (error) => {
  console.error('Backfill failed:', error);
  try {
    await mongoose.connection.close();
  } catch (_closeError) {
    // ignore close failures
  }
  process.exitCode = 1;
});
