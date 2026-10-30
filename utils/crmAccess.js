const mongoose = require('mongoose');
const { normalizeRole } = require('./accessControl');

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) => (mongoose.Types.ObjectId.isValid(value) ? value : null);

// Shared by CRM REST access and realtime contact subscriptions. Keep this
// predicate aligned with the CRM API's tenant and ownership policy.
const buildScopedFilter = (req, extra = {}) => {
  const conditions = [];
  const normalizedRole = normalizeRole(req?.user?.normalizedRole || req?.user?.companyRole || req?.user?.role);
  const isAdminLikeRole = ['superadmin', 'admin', 'manager'].includes(normalizedRole);
  const normalizedCompanyId = toObjectIdIfValid(req?.companyId);
  const normalizedUserId = toCleanString(req?.user?.id);

  if (isAdminLikeRole) {
    if (normalizedCompanyId) conditions.push({ companyId: normalizedCompanyId });
    else if (req?.companyId) conditions.push({ companyId: req.companyId });
  } else {
    const agentScope = normalizedUserId
      ? { $or: [
          { userId: normalizedUserId },
          { ownerId: normalizedUserId },
          { assignedTo: normalizedUserId },
          { assignedAgent: normalizedUserId },
          { createdBy: normalizedUserId }
        ] }
      : null;
    if (req?.companyId) conditions.push({ companyId: req.companyId });
    if (agentScope) conditions.push(agentScope);
  }

  if (extra && Object.keys(extra).length > 0) conditions.push(extra);
  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

module.exports = { buildScopedFilter };
