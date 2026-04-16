const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');

const getRetentionDays = () =>
  Number(process.env.CONSENT_LOG_RETENTION_DAYS || 365);

const isRetentionEnabled = () =>
  String(process.env.CONSENT_LOG_ARCHIVE_ENABLED || '').toLowerCase() === 'true';

const archiveOldConsentLogs = async () => {
  if (!isRetentionEnabled()) return { archived: 0 };

  const days = getRetentionDays();
  if (!Number.isFinite(days) || days <= 0) {
    return { archived: 0 };
  }

  const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const result = await WhatsAppConsentLog.updateMany(
    { createdAt: { $lt: cutoff }, isArchived: { $ne: true } },
    { $set: { isArchived: true, archivedAt: new Date() } }
  );

  return { archived: result?.modifiedCount || 0 };
};

module.exports = {
  archiveOldConsentLogs
};
