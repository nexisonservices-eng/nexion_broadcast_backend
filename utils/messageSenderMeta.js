const { normalizeRole } = require('./accessControl');

const toCleanString = (value = '') => String(value || '').trim();

const resolveSenderName = (user = {}) => {
  const candidates = [
    user?.senderName,
    user?.displayName,
    user?.fullName,
    user?.name,
    user?.username,
    user?.email
  ];

  for (const candidate of candidates) {
    const normalized = toCleanString(candidate);
    if (normalized) return normalized;
  }

  return '';
};

const resolveInternalSenderRole = (roleLike = '') => {
  const normalizedRole = normalizeRole(roleLike);
  if (normalizedRole === 'agent') return 'agent';
  return 'admin';
};

const resolveWorkspaceSenderRole = (user = {}) => {
  const workspaceState = toCleanString(user?.workspaceAccessState || '').toLowerCase();
  if (workspaceState.includes('admin')) {
    return 'admin';
  }

  const normalizedCompanyRole = normalizeRole(user?.normalizedRole || user?.companyRole || user?.role);
  if (['superadmin', 'admin', 'manager'].includes(normalizedCompanyRole)) {
    return 'admin';
  }

  if (Boolean(user?.canAccessAgentManagement) || Boolean(user?.canAccessUserManagement)) {
    return 'admin';
  }

  return 'agent';
};

const resolveOutboundSenderMeta = (user = {}) => {
  const senderRole = resolveWorkspaceSenderRole(user);
  const senderId = toCleanString(user?.id || user?._id || user?.userId) || null;
  const senderName =
    resolveSenderName(user) ||
    (senderRole === 'admin' ? 'Admin' : 'Agent');

  return {
    senderId,
    senderRole,
    senderName
  };
};

module.exports = {
  resolveOutboundSenderMeta,
  resolveInternalSenderRole,
  resolveSenderName
};
