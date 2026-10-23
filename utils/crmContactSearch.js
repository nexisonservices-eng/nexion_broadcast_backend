const toCleanString = (value = '') => String(value || '').trim();
const escapeRegExp = (value = '') => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const buildCrmContactSearchFilter = (search) => {
  const normalizedSearch = toCleanString(search);
  if (!normalizedSearch) return {};

  const searchPattern = escapeRegExp(normalizedSearch);
  const clauses = [
    { name: { $regex: searchPattern, $options: 'i' } },
    { phone: { $regex: searchPattern, $options: 'i' } },
    { email: { $regex: searchPattern, $options: 'i' } },
    { source: { $regex: searchPattern, $options: 'i' } },
    { ownerId: { $regex: searchPattern, $options: 'i' } },
    { stage: { $regex: searchPattern, $options: 'i' } },
    { notes: { $regex: searchPattern, $options: 'i' } },
    { tags: { $in: [new RegExp(searchPattern, 'i')] } }
  ];

  const digits = normalizedSearch.replace(/\D/g, '');
  if (digits) {
    clauses.push({ phoneDigits: { $regex: escapeRegExp(digits) } });
    clauses.push({ phoneKey: { $regex: escapeRegExp(digits) } });
  }

  return { $or: clauses };
};

module.exports = { buildCrmContactSearchFilter };
