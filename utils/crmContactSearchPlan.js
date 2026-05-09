const {
  buildPrefixRange,
  escapeRegex,
  isDigitsOnlySearch,
  isSimpleNameSearch
} = require('./inboxSearchPlan');

const buildCrmContactSearchPlan = (search = '') => {
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

  const fullRegexClause = {
    $or: [
      { name: { $regex: normalizedSearch, $options: 'i' } },
      { phone: { $regex: normalizedSearch, $options: 'i' } },
      { email: { $regex: normalizedSearch, $options: 'i' } },
      { notes: { $regex: normalizedSearch, $options: 'i' } },
      { tags: { $in: [new RegExp(normalizedSearch, 'i')] } }
    ]
  };

  if (isDigitsOnlySearch(normalizedSearchPhone)) {
    return {
      mode: 'digits',
      summaryClause: {
        phoneDigits: buildPrefixRange(normalizedSearchPhone)
      },
      fallbackClause: fullRegexClause,
      hint: {
        companyId: 1,
        userId: 1,
        phoneDigits: 1,
        nextFollowUpAt: 1,
        leadScore: -1,
        lastContact: -1,
        createdAt: -1,
        _id: -1
      }
    };
  }

  if (isSimpleNameSearch(normalizedSearch)) {
    return {
      mode: 'name',
      summaryClause: {
        nameLower: buildPrefixRange(normalizedSearchLower)
      },
      fallbackClause: fullRegexClause,
      hint: {
        companyId: 1,
        userId: 1,
        nameLower: 1,
        nextFollowUpAt: 1,
        leadScore: -1,
        lastContact: -1,
        createdAt: -1,
        _id: -1
      }
    };
  }

  return {
    mode: 'generic',
    summaryClause: fullRegexClause,
    fallbackClause: fullRegexClause,
    hint: null
  };
};

module.exports = {
  buildCrmContactSearchPlan
};
