const TENANT_WIDE_ROLES = new Set(['superadmin', 'admin', 'manager']);

const buildLegacyCrmContactFilter = ({ companyId, role }, contactId) => {
  const normalizedCompanyId = String(companyId || '').trim();
  const normalizedRole = String(role || '').trim().toLowerCase();
  const normalizedContactId = String(contactId || '').trim();

  if (!normalizedCompanyId || !normalizedContactId || !TENANT_WIDE_ROLES.has(normalizedRole)) {
    return null;
  }

  return {
    $and: [
      { _id: normalizedContactId },
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
      }
    ]
  };
};

module.exports = { buildLegacyCrmContactFilter };
