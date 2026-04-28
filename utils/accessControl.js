const mongoose = require('mongoose');

const ROLE_ALIASES = {
  super_admin: 'superadmin',
  superadmin: 'superadmin',
  admin: 'admin',
  manager: 'manager',
  user: 'agent',
  agent: 'agent'
};

const TENANT_WIDE_ROLES = new Set(['superadmin', 'admin', 'manager']);
const DEFAULT_ALLOWED_ROLES = new Set(['superadmin', 'admin', 'manager', 'agent']);

const toCleanString = (value) => String(value || '').trim();

const normalizeRole = (role) => {
  const normalized = toCleanString(role).toLowerCase();
  return ROLE_ALIASES[normalized] || 'agent';
};

const resolveRequestedCompanyId = (req) =>
  toCleanString(
    req?.headers?.['x-company-id'] ||
      req?.query?.companyId ||
      req?.body?.companyId ||
      req?.companyId ||
      req?.user?.companyId
  );

const isValidCompanyId = (companyId) =>
  Boolean(companyId && mongoose.Types.ObjectId.isValid(String(companyId)));

const hasAnyFeatureAccess = (features = {}, keys = []) => {
  if (!Array.isArray(keys) || keys.length === 0) return true;
  const featureFlags = features && typeof features === 'object' ? features : {};
  const hasKnownFeatureKeys = keys.some((key) => Object.prototype.hasOwnProperty.call(featureFlags, key));
  if (!hasKnownFeatureKeys) return true;
  return keys.some((key) => Boolean(featureFlags[key]));
};

const isTenantWideRole = (role) => TENANT_WIDE_ROLES.has(normalizeRole(role));

const canAccessOwnedResource = ({ role, ownerId, userId }) => {
  if (isTenantWideRole(role)) return true;
  return String(ownerId || '') === String(userId || '');
};

const buildTenantResourceFilter = ({
  req,
  base = {},
  ownerField = 'createdBy'
} = {}) => {
  const role = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const filter = {
    ...base,
    companyId: req?.companyId
  };

  if (!isTenantWideRole(role)) {
    filter[ownerField] = req?.user?.id;
  }

  return filter;
};

module.exports = {
  DEFAULT_ALLOWED_ROLES,
  normalizeRole,
  resolveRequestedCompanyId,
  isValidCompanyId,
  hasAnyFeatureAccess,
  isTenantWideRole,
  canAccessOwnedResource,
  buildTenantResourceFilter
};
