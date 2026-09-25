// The registry and membership are supplied by the server, not by query parameters.
const createAgentActivityHandler = (registry, resolveUser = async (req) => req.user) => async (req, res) => {
  try {
    const user = await resolveUser(req);
    const role = String(user?.companyRole || user?.role || '').toLowerCase();
    if (!['admin', 'manager'].includes(role)) return res.status(403).json({ message: 'Admin workspace only.' });
    const config = registry[req.params.kind];
    if (!config) return res.status(404).json({ message: 'Unknown activity type.' });
    const ownId = String(user.id || user.userId || user._id || '');
    let ids = [...new Set((user.workspaceReadUserIds || []).map(String))].filter((id) => id && id !== ownId);
    if (req.query.createdById) ids = ids.filter((id) => id === String(req.query.createdById));
    const page = Math.max(1, Math.min(100000, Number.parseInt(req.query.page, 10) || 1));
    const limit = 25;
    if (!ids.length) return res.json({ success: true, data: [], pagination: { page, limit, total: 0, pages: 0 } });
    const { model, owner, fallback, title, where = {} } = config;
    const scope = fallback ? { $or: [
      { [owner]: { $in: ids } },
      { $and: [{ $or: [{ [owner]: null }, ...(model.schema.path(owner)?.instance === 'String' ? [{ [owner]: '' }] : [])] }, { [fallback]: { $in: ids } }] }
    ] } : { [owner]: { $in: ids } };
    const clauses = [scope, where];
    if (user.companyId && model.schema.path('companyId')) clauses.push({ companyId: user.companyId });
    const search = String(req.query.search || '').trim().slice(0, 150);
    if (search) clauses.push({ $or: title.map((field) => ({ [field]: { $regex: search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' } })) });
    const filter = { $and: clauses };
    const projection = [...new Set([...title, owner, fallback, 'status', 'stage', 'createdAt', 'updatedAt'])].filter(Boolean).join(' ');
    const [rows, total] = await Promise.all([
      model.find(filter).select(projection).sort({ createdAt: -1, _id: -1 }).skip((page - 1) * limit).limit(limit).lean(),
      model.countDocuments(filter)
    ]);
    return res.json({ success: true, data: rows.map((row) => ({
      id: String(row._id),
      name: title.map((field) => field.split('.').reduce((value, key) => value?.[key], row)).find((value) => typeof value === 'string' && value.trim()) || config.label,
      createdById: String(row[owner] || row[fallback] || ''),
      status: row.status || row.stage || '',
      createdAt: row.createdAt || null
    })), pagination: { page, limit, total, pages: Math.ceil(total / limit) } });
  } catch (error) {
    return res.status(500).json({ message: 'Unable to load agent activity.' });
  }
};

module.exports = { createAgentActivityHandler };
