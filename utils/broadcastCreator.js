const preserveBroadcastCreator = (existing, updates = {}) => {
  const next = { ...updates };
  for (const field of ['createdById', 'createdBy', 'createdByName', 'createdByEmail', 'createdByWorkspaceRole']) {
    next[field] = existing[field];
  }
  return next;
};

const buildCreatorLookup = (user = {}) => {
  const lookup = new Map();
  const creators = [
    ...(Array.isArray(user.workspaceCreators) ? user.workspaceCreators : []),
    { id: user.id, name: user.username, email: user.email, role: user.companyRole || user.role },
  ];

  for (const creator of creators) {
    const id = String(creator?.id || '').trim();
    if (id && !lookup.has(id)) lookup.set(id, creator);
  }

  return lookup;
};

const resolveBroadcastCreatorWithLookup = (broadcast, creatorLookup) => {
  if (!broadcast) return broadcast;
  const row = typeof broadcast.toObject === 'function' ? broadcast.toObject() : broadcast;
  const id = String(row.createdById?._id || row.createdById || '');
  const creator = creatorLookup.get(id);
  if (!creator) return row;
  const name = String(creator.name || creator.email || '').trim();
  return { ...row, createdById: id, ...(name ? { createdBy: name, createdByName: name } : {}),
    createdByEmail: creator.email || row.createdByEmail,
    createdByWorkspaceRole: creator.role || row.createdByWorkspaceRole };
};

const resolveBroadcastCreator = (broadcast, user = {}) =>
  resolveBroadcastCreatorWithLookup(broadcast, buildCreatorLookup(user));

const resolveBroadcastCreators = (result, user) => {
  if (!result?.success) return result;
  const data = result.data;
  const creatorLookup = buildCreatorLookup(user);
  return { ...result, data: Array.isArray(data) ? data.map((row) => resolveBroadcastCreatorWithLookup(row, creatorLookup))
    : Array.isArray(data?.items) ? { ...data, items: data.items.map((row) => resolveBroadcastCreatorWithLookup(row, creatorLookup)) }
    : resolveBroadcastCreatorWithLookup(data, creatorLookup) };
};
module.exports = { preserveBroadcastCreator, resolveBroadcastCreator, resolveBroadcastCreators };
