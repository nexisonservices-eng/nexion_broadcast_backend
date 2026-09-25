const buildAgentCreatorFilter = (user = {}) => {
  const ids = (user.workspaceCreators || [])
    .filter((creator) => ['agent', 'user'].includes(String(creator.role || '').toLowerCase()))
    .map((creator) => String(creator.id || '')).filter(Boolean);
  if (['agent', 'user'].includes(String(user.companyRole || user.role || '').toLowerCase()) && user.id) ids.push(String(user.id));
  return { $or: [
    { createdByWorkspaceRole: { $in: ['agent', 'user'] } },
    { createdById: { $in: [...new Set(ids)] } }
  ] };
};
module.exports = { buildAgentCreatorFilter };
