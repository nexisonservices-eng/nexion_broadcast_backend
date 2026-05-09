const {
  buildPrefixRange,
  escapeRegex,
  isDigitsOnlySearch,
  isSimpleNameSearch
} = require('./inboxSearchPlan');

const buildContactSearchPlan = (search = '') => {
  const normalizedSearch = String(search || '').trim();
  if (!normalizedSearch) {
    return {
      mode: null,
      summaryClause: null,
      fallbackClause: null,
      hint: null
    };
  }

  const normalizedSearchLower = normalizedSearch.toLowerCase();
  const normalizedSearchPhone = normalizedSearch.replace(/[\s().+-]/g, '');

  if (isDigitsOnlySearch(normalizedSearchPhone)) {
    return {
      mode: 'digits',
      summaryClause: {
        phoneDigits: buildPrefixRange(normalizedSearchPhone)
      },
      fallbackClause: {
        phone: {
          $regex: `^${escapeRegex(normalizedSearchPhone)}`,
          $options: 'i'
        }
      },
      hint: { companyId: 1, userId: 1, phoneDigits: 1, lastContact: -1, createdAt: -1, _id: -1 }
    };
  }

  if (isSimpleNameSearch(normalizedSearch)) {
    return {
      mode: 'name',
      summaryClause: {
        nameLower: buildPrefixRange(normalizedSearchLower)
      },
      fallbackClause: {
        name: {
          $regex: `^${escapeRegex(normalizedSearch)}`,
          $options: 'i'
        }
      },
      hint: { companyId: 1, userId: 1, nameLower: 1, lastContact: -1, createdAt: -1, _id: -1 }
    };
  }

  const regexClause = {
    $or: [
      { name: { $regex: normalizedSearch, $options: 'i' } },
      { phone: { $regex: normalizedSearch, $options: 'i' } },
      { email: { $regex: normalizedSearch, $options: 'i' } }
    ]
  };

  return {
    mode: 'generic',
    summaryClause: regexClause,
    fallbackClause: regexClause,
    hint: null
  };
};

module.exports = {
  buildContactSearchPlan
};
