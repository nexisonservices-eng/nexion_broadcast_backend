const { randomUUID } = require('node:crypto');

const startTemplateCatalogDiagnostics = (req, res, next) => {
  req.templateCatalogRequestId = randomUUID();
  res.set('X-Request-ID', req.templateCatalogRequestId);
  res.append('Access-Control-Expose-Headers', 'X-Request-ID');
  return next();
};

const redactCatalogText = (value, req, credentials = req.whatsappCredentials) => {
  if (typeof value !== 'string') return undefined;
  const authorization = req.headers?.authorization;
  const sessionToken = typeof authorization === 'string' ? authorization.match(/^Bearer\s+(\S+)/i)?.[1] : null;
  const secrets = [
    sessionToken, credentials?.accessToken, credentials?.whatsappToken, credentials?.metaUserAccessToken,
    credentials?.appSecret, credentials?.metaAppSecret
  ];
  let text = value;
  for (const secret of secrets) {
    if (typeof secret !== 'string' || !secret) continue;
    text = text.split(secret).join('[redacted]');
    if (/^[\x20-\x7e]+$/.test(secret)) {
      const encoded = encodeURIComponent(secret);
      if (encoded !== secret) text = text.split(encoded).join('[redacted]');
    }
  }
  return text
    .replace(/\b(?:authorization|access[_-]?token|app[_-]?secret(?:[_-]?proof)?|client[_-]?secret|api[_-]?key)\b["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:Bearer|OAuth)\s+[^\s,;&]+|[^\s,;&]+)/gi, '[redacted]')
    .replace(/\b(?:Bearer|OAuth)\s+[\w.~+/=-]+/gi, '[redacted]')
    .slice(0, 1000);
};

const numericField = (value) =>
  (typeof value === 'number' || (typeof value === 'string' && /^\d+$/.test(value))) &&
    Number.isSafeInteger(Number(value)) ? Number(value) : undefined;

const logTemplateCatalogFailure = (req, {
  source, stage, error, message, upstreamStatus,
  credentials = req.whatsappCredentials, wabaId = credentials?.businessAccountId
}) => {
  const clean = (value) => redactCatalogText(value, req, credentials);
  const id = (value) => value == null ? null : clean(String(value));
  console.error('[WHATSAPP_TEMPLATE_CATALOG]', JSON.stringify({
    outcome: 'failure',
    source,
    stage,
    requestId: req.templateCatalogRequestId,
    userId: id(req.user?.id),
    companyId: id(req.companyId || req.user?.companyId || credentials?.companyId),
    credentialOwnerId: id(credentials?.credentialOwnerUserId),
    wabaId: id(wabaId),
    upstreamStatus: numericField(upstreamStatus),
    message: clean(typeof error === 'string' ? error : error?.message) || clean(message),
    type: clean(error?.type),
    code: numericField(error?.code),
    error_subcode: numericField(error?.error_subcode),
    fbtrace_id: clean(error?.fbtrace_id)
  }));
};

module.exports = { startTemplateCatalogDiagnostics, redactCatalogText, logTemplateCatalogFailure };
