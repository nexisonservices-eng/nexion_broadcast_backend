const ConsentExportJob = require('../models/ConsentExportJob');
const { generateConsentExport } = require('./consentExportService');
const { sendConsentExportEmail } = require('./emailService');

const toCleanString = (value = '') => String(value || '').trim();

const processConsentExportJobs = async () => {
  const job = await ConsentExportJob.findOne({ status: 'queued' }).sort({ createdAt: 1 });
  if (!job) return null;

  job.status = 'processing';
  job.error = '';
  await job.save();

  try {
    const { csv, checksum, count } = await generateConsentExport({
      filters: job.filters || {},
      limit: Number(process.env.CONSENT_EXPORT_MAX_ROWS || 5000)
    });

    const subject = 'WhatsApp Consent Logs Export';
    const text = `Consent export generated. Rows: ${count}. Checksum: ${checksum}`;

    await sendConsentExportEmail({
      to: job.email,
      subject,
      text,
      csvBuffer: Buffer.from(csv, 'utf8'),
      fileName: `whatsapp-consent-logs_${new Date().toISOString().slice(0, 10)}.csv`
    });

    job.status = 'completed';
    job.checksum = checksum;
    job.completedAt = new Date();
    await job.save();
    return job;
  } catch (error) {
    job.status = 'failed';
    job.error = toCleanString(error?.message || 'Export failed');
    await job.save();
    return job;
  }
};

module.exports = {
  processConsentExportJobs
};
