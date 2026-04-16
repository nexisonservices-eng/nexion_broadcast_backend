const nodemailer = require('nodemailer');

const toCleanString = (value = '') => String(value || '').trim();

const isEmailEnabled = () =>
  String(process.env.CONSENT_EXPORT_EMAIL_ENABLED || '').toLowerCase() === 'true';

const buildTransport = () => {
  const host = toCleanString(process.env.SMTP_HOST);
  const port = Number(process.env.SMTP_PORT || 587);
  const user = toCleanString(process.env.SMTP_USER);
  const pass = toCleanString(process.env.SMTP_PASS);
  const secure = String(process.env.SMTP_SECURE || '').toLowerCase() === 'true';

  if (!host || !user || !pass) {
    return null;
  }

  return nodemailer.createTransport({
    host,
    port,
    secure,
    auth: { user, pass }
  });
};

const sendConsentExportEmail = async ({ to, subject, text, csvBuffer, fileName }) => {
  if (!isEmailEnabled()) {
    const error = new Error('Consent export email is disabled.');
    error.code = 'EMAIL_DISABLED';
    throw error;
  }

  const transport = buildTransport();
  if (!transport) {
    const error = new Error('SMTP transport is not configured.');
    error.code = 'SMTP_NOT_CONFIGURED';
    throw error;
  }

  const from =
    toCleanString(process.env.CONSENT_EXPORT_EMAIL_FROM) ||
    toCleanString(process.env.SMTP_FROM) ||
    toCleanString(process.env.SMTP_USER);

  await transport.sendMail({
    from,
    to,
    subject,
    text,
    attachments: [
      {
        filename: fileName,
        content: csvBuffer
      }
    ]
  });
};

module.exports = {
  sendConsentExportEmail,
  isEmailEnabled
};
