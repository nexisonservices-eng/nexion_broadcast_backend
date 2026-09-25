const resolveBroadcastCreator = (broadcast, user = {}) => {
  if (!broadcast) return broadcast;
  const row = typeof broadcast.toObject === 'function' ? broadcast.toObject() : broadcast;
  const id = String(row.createdById?._id || row.createdById || '');
  const creators = [...(user.workspaceCreators || []), { id: user.id, name: user.username, email: user.email, role: user.companyRole || user.role }];
  const creator = creators.find((entry) => id && String(entry.id) === id);
  if (!creator) return row;
  const name = String(creator.name || creator.email || '').trim();
  return { ...row, createdById: id, ...(name ? { createdBy: name, createdByName: name } : {}),
    createdByEmail: creator.email || row.createdByEmail,
    createdByWorkspaceRole: creator.role || row.createdByWorkspaceRole };
};

const resolveBroadcastCreators = (result, user) => {
  if (!result?.success) return result;
  const data = result.data;
  return { ...result, data: Array.isArray(data) ? data.map((row) => resolveBroadcastCreator(row, user))
    : Array.isArray(data?.items) ? { ...data, items: data.items.map((row) => resolveBroadcastCreator(row, user)) }
    : resolveBroadcastCreator(data, user) };
};
module.exports = { resolveBroadcastCreator, resolveBroadcastCreators };
