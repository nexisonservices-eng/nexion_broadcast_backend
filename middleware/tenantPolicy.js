const {
  DEFAULT_ALLOWED_ROLES,
  normalizeRole,
  resolveRequestedCompanyId,
  isValidCompanyId,
  hasAnyFeatureAccess
} = require('../utils/accessControl');
const { emitAuthAuditLog } = require('../utils/authAuditLogger');

const requireTenantPolicy = (options = {}) => {
  const allowedRoles = new Set(options.allowedRoles || Array.from(DEFAULT_ALLOWED_ROLES));
  const requiredFeatures = Array.isArray(options.requiredFeatures) ? options.requiredFeatures : [];
  const auditEvent = options.auditEvent || 'tenant_policy';

  return (req, res, next) => {
    const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
    const allowMissingCompanyId =
      String(process.env.ALLOW_MISSING_COMPANY_ID || '').trim().toLowerCase() === 'true' ||
      process.env.NODE_ENV !== 'production' ||
      ['superadmin', 'admin', 'manager'].includes(normalizedRole);
    req.user = {
      ...(req.user || {}),
      normalizedRole
    };

    if (!allowedRoles.has(normalizedRole)) {
      emitAuthAuditLog({
        event: auditEvent,
        allowed: false,
        reason: 'role_forbidden',
        req,
        extra: { allowedRoles: Array.from(allowedRoles) }
      });
      return res.status(403).json({
        success: false,
        error: `Forbidden: role "${normalizedRole}" cannot access this resource`
      });
    }

    const requestedCompanyId = resolveRequestedCompanyId(req);
    if (!isValidCompanyId(requestedCompanyId)) {
      if (allowMissingCompanyId) {
        req.companyId = req?.user?.companyId || null;
        req.authContext = {
          tenant: req.companyId || null,
          role: normalizedRole,
          feature: requiredFeatures
        };
        return next();
      }

      emitAuthAuditLog({
        event: auditEvent,
        allowed: false,
        reason: 'invalid_company_id',
        req
      });
      return res.status(403).json({
        success: false,
        error: 'Forbidden: valid tenant companyId is required'
      });
    }

    req.companyId = requestedCompanyId;

    if (
      normalizedRole !== 'superadmin' &&
      !hasAnyFeatureAccess(req?.user?.featureFlags || req?.planFeatures || {}, requiredFeatures)
    ) {
      emitAuthAuditLog({
        event: auditEvent,
        allowed: false,
        reason: 'feature_forbidden',
        req,
        extra: { requiredFeatures }
      });
      return res.status(403).json({
        success: false,
        error: 'Forbidden: feature is not enabled for this workspace plan'
      });
    }

    req.authContext = {
      tenant: req.companyId,
      role: normalizedRole,
      feature: requiredFeatures
    };

    next();
  };
};

module.exports = {
  requireTenantPolicy
};
