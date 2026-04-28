const toShortValue = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw.length > 120 ? `${raw.slice(0, 117)}...` : raw;
};

const emitAuthAuditLog = ({
  event,
  allowed,
  reason,
  req,
  extra = {}
} = {}) => {
  const payload = {
    ts: new Date().toISOString(),
    event: String(event || 'auth_policy').trim() || 'auth_policy',
    allowed: Boolean(allowed),
    reason: String(reason || '').trim() || null,
    path: toShortValue(req?.originalUrl || req?.url),
    method: toShortValue(req?.method),
    userId: toShortValue(req?.user?.id || req?.user?.userId),
    role: toShortValue(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role),
    companyId: toShortValue(req?.companyId || req?.user?.companyId),
    ip: toShortValue(req?.headers?.['x-forwarded-for'] || req?.socket?.remoteAddress),
    userAgent: toShortValue(req?.headers?.['user-agent']),
    ...extra
  };

  const serialized = JSON.stringify(payload);
  if (payload.allowed) {
    console.info(`[AUTH_AUDIT] ${serialized}`);
    return;
  }

  console.warn(`[AUTH_AUDIT] ${serialized}`);
};

module.exports = {
  emitAuthAuditLog
};
