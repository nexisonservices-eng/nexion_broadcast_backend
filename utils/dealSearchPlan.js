const {
  buildPrefixRange,
  escapeRegex,
  isSimpleNameSearch
} = require('./inboxSearchPlan');

const buildDealSearchPlan = (search = '') => {
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
  const fullRegexClause = {
    $or: [
      { title: { $regex: normalizedSearch, $options: 'i' } },
      { productName: { $regex: normalizedSearch, $options: 'i' } },
      { source: { $regex: normalizedSearch, $options: 'i' } },
      { notes: { $regex: normalizedSearch, $options: 'i' } },
      { lostReason: { $regex: normalizedSearch, $options: 'i' } }
    ]
  };

  if (isSimpleNameSearch(normalizedSearch)) {
    return {
      mode: 'simple',
      summaryClause: {
        $or: [
          { titleLower: buildPrefixRange(normalizedSearchLower) },
          { productNameLower: buildPrefixRange(normalizedSearchLower) },
          { sourceLower: buildPrefixRange(normalizedSearchLower) },
          { lostReasonLower: buildPrefixRange(normalizedSearchLower) }
        ]
      },
      fallbackClause: fullRegexClause,
      hint: null
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
  buildDealSearchPlan
};
