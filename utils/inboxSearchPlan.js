const escapeRegex = (value = '') =>
  String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const isDigitsOnlySearch = (value = '') => /^\d+$/.test(String(value || '').trim());

const isSimpleNameSearch = (value = '') => /^[a-zA-Z\s.'-]+$/.test(String(value || '').trim());

const buildPrefixRange = (prefix = '') => {
  const normalized = String(prefix || '').trim();
  if (!normalized) return null;
  return {
    $gte: normalized,
    $lt: `${normalized}\uffff`
  };
};

const buildInboxSearchPlan = (search = '') => {
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
        contactPhoneDigits: buildPrefixRange(normalizedSearchPhone)
      },
      fallbackClause: {
        contactPhone: {
          $regex: `^${escapeRegex(normalizedSearchPhone)}`,
          $options: 'i'
        }
      },
      hint: { companyId: 1, userId: 1, contactPhoneDigits: 1, lastMessageTime: -1, _id: -1 }
    };
  }

  if (isSimpleNameSearch(normalizedSearch)) {
    return {
      mode: 'name',
      summaryClause: {
        contactNameLower: buildPrefixRange(normalizedSearchLower)
      },
      fallbackClause: {
        contactName: {
          $regex: `^${escapeRegex(normalizedSearch)}`,
          $options: 'i'
        }
      },
      hint: { companyId: 1, userId: 1, contactNameLower: 1, lastMessageTime: -1, _id: -1 }
    };
  }

  const indexedSummaryClause = {
    $or: [
      { contactNameLower: buildPrefixRange(normalizedSearchLower) },
      ...(isDigitsOnlySearch(normalizedSearchPhone)
        ? [{ contactPhoneDigits: buildPrefixRange(normalizedSearchPhone) }]
        : [])
    ]
  };

  const regexClause = {
    $or: [
      { contactName: { $regex: normalizedSearch, $options: 'i' } },
      { contactPhone: { $regex: normalizedSearch, $options: 'i' } },
      { lastMessage: { $regex: normalizedSearch, $options: 'i' } }
    ]
  };

  return {
    mode: 'generic',
    summaryClause: indexedSummaryClause,
    fallbackClause: regexClause,
    hint: { companyId: 1, userId: 1, contactNameLower: 1, lastMessageTime: -1, _id: -1 }
  };
};

module.exports = {
  buildInboxSearchPlan,
  buildPrefixRange,
  escapeRegex,
  isDigitsOnlySearch,
  isSimpleNameSearch
};
