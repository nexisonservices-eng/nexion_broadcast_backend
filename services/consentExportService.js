const crypto = require('crypto');
const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');

const escapeCsvValue = (value = '') => {
  const text = String(value ?? '').replace(/"/g, '""');
  if (/[",\n]/.test(text)) return `"${text}"`;
  return text;
};

const CSV_HEADERS = [
  'phone',
  'action',
  'source',
  'scope',
  'consentText',
  'proofType',
  'proofId',
  'proofUrl',
  'capturedBy',
  'pageUrl',
  'ip',
  'userAgent',
  'metadata',
  'createdAt'
];

const buildSummaryRows = (items) => {
  const optInCount = items.filter((item) => item.action === 'opt_in').length;
  const optOutCount = items.filter((item) => item.action === 'opt_out').length;
  const totalCount = items.length;

  const summaryRows = [
    { action: 'opt_in', count: optInCount },
    { action: 'opt_out', count: optOutCount },
    { action: 'total', count: totalCount }
  ];

  return summaryRows.map((row) =>
    CSV_HEADERS.map((key) => {
      if (key === 'phone') return escapeCsvValue('SUMMARY');
      if (key === 'action') return escapeCsvValue(row.action);
      if (key === 'metadata') return escapeCsvValue(JSON.stringify({ count: row.count }));
      return '';
    }).join(',')
  );
};

const buildCsvFromLogs = (items = []) => {
  const rows = items.map((item) =>
    CSV_HEADERS
      .map((key) => {
        if (key === 'metadata') {
          return escapeCsvValue(item?.metadata ? JSON.stringify(item.metadata) : '');
        }
        return escapeCsvValue(item?.[key] ?? '');
      })
      .join(',')
  );

  return [CSV_HEADERS.join(','), ...buildSummaryRows(items), ...rows].join('\n');
};

const generateConsentExport = async ({ filters = {}, limit = 5000 }) => {
  const items = await WhatsAppConsentLog.find(filters)
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();

  const csv = buildCsvFromLogs(items);
  const checksum = crypto.createHash('sha256').update(csv).digest('hex');
  return { csv, checksum, count: items.length };
};

module.exports = {
  generateConsentExport
};
