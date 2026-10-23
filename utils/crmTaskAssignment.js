const toCleanString = (value = '') => String(value || '').trim();

const isWorkspaceAssignee = (users = [], assigneeId) => {
  const targetId = toCleanString(assigneeId);
  if (!targetId) return false;

  return (Array.isArray(users) ? users : []).some((user) => {
    const userId = toCleanString(user?._id || user?.id || user?.userId);
    const isEnabled = user?.isEnabled !== false && user?.isActive !== false;
    return userId === targetId && isEnabled;
  });
};

module.exports = { isWorkspaceAssignee };
