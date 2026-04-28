const { requireTenantPolicy } = require('./tenantPolicy');
const { normalizeRole } = require('../utils/accessControl');

const CRM_ALLOWED_ROLES = ['superadmin', 'admin', 'manager', 'agent'];

const requireCrmPolicy = (options = {}) => {
  const allowedRoles = options.allowedRoles || CRM_ALLOWED_ROLES;
  const requiredFeatures = Array.isArray(options.requiredFeatures)
    ? options.requiredFeatures
    : ['contacts', 'teamInbox', 'broadcastMessaging', 'broadcastDashboard'];

  const tenantPolicy = requireTenantPolicy({
    allowedRoles,
    requiredFeatures,
    auditEvent: 'crm_policy'
  });

  return (req, res, next) =>
    tenantPolicy(req, res, () => {
      req.crmAccess = {
        role: req?.user?.normalizedRole || normalizeRole(req?.user?.role),
        companyId: req.companyId
      };
      next();
    });
};

module.exports = {
  normalizeRole,
  requireCrmPolicy
};
