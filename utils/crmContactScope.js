const TENANT_WIDE_ROLES = new Set(['superadmin', 'admin', 'manager']);

const buildLegacyCrmContactScope = ({ companyId, role }, extra = {}) => {
  const normalizedCompanyId = String(companyId || '').trim();
  const normalizedRole = String(role || '').trim().toLowerCase();

  if (!normalizedCompanyId || !TENANT_WIDE_ROLES.has(normalizedRole)) {
    return null;
  }

  return {
    $and: [
      {
        $or: [
          { companyId: normalizedCompanyId },
          { companyId: { $exists: false } },
          { companyId: null }
        ]
      },
      {
        $or: [
          { userId: { $exists: false } },
          { userId: null }
        ]
      },
      ...(extra && Object.keys(extra).length ? [extra] : [])
    ]
  };
};

const buildLegacyCrmContactFilter = (scope, contactId) => {
  const normalizedContactId = String(contactId || '').trim();
  if (!normalizedContactId) return null;
  return buildLegacyCrmContactScope(scope, { _id: normalizedContactId });
};

module.exports = { buildLegacyCrmContactFilter, buildLegacyCrmContactScope };
