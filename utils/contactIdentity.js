const toCleanString = (value = '') => String(value || '').trim();

const normalizePhoneDigits = (value = '') => toCleanString(value).replace(/\D/g, '');

const normalizePhoneKey = (value = '') => {
  const digits = normalizePhoneDigits(value);
  if (!digits) return '';
  return digits.length > 10 ? digits.slice(-10) : digits;
};

const escapeRegExp = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const buildContactPhoneCandidates = (value = '') => {
  const rawValue = toCleanString(value);
  const normalizedPhone = normalizePhoneDigits(rawValue);

  return Array.from(
    new Set(
      [
        rawValue,
        normalizedPhone,
        normalizedPhone ? `+${normalizedPhone}` : '',
        normalizedPhone.length > 10 ? normalizedPhone.slice(-10) : ''
      ].filter(Boolean)
    )
  );
};

const buildContactPhoneSuffixCandidates = (value = '') => {
  const normalizedPhone = normalizePhoneDigits(value);
  return Array.from(
    new Set(
      [
        normalizedPhone.length >= 10 ? normalizedPhone.slice(-10) : '',
        normalizedPhone.length >= 11 ? normalizedPhone.slice(-11) : '',
        normalizedPhone.length >= 12 ? normalizedPhone.slice(-12) : ''
      ].filter(Boolean)
    )
  );
};

const buildContactPhoneLookupFilter = (value = '') => {
  const rawValue = toCleanString(value);
  const normalizedPhone = normalizePhoneDigits(rawValue);
  const phoneKey = normalizePhoneKey(rawValue);
  if (!rawValue && !normalizedPhone) return null;

  const exactCandidates = buildContactPhoneCandidates(rawValue);
  const suffixCandidates = buildContactPhoneSuffixCandidates(rawValue);
  const phoneFieldNames = ['phone', 'phoneDigits', 'phoneNumber', 'mobile', 'whatsappNumber'];
  const filters = [];

  if (phoneKey) {
    filters.push({ phoneKey });
  }

  if (exactCandidates.length > 0) {
    phoneFieldNames.forEach((fieldName) => {
      filters.push({ [fieldName]: { $in: exactCandidates } });
    });
  }

  suffixCandidates.forEach((suffix) => {
    const escaped = escapeRegExp(suffix);
    phoneFieldNames.forEach((fieldName) => {
      filters.push({ [fieldName]: new RegExp(`${escaped}$`) });
    });
  });

  return filters.length > 0 ? { $or: filters } : null;
};

const buildContactIdentityScopeFilter = ({ companyId = '', userId = '' } = {}) => {
  const normalizedCompanyId = toCleanString(companyId);
  const normalizedUserId = toCleanString(userId);

  if (normalizedCompanyId) {
    return { companyId: normalizedCompanyId };
  }
  if (normalizedUserId) {
    return { userId: normalizedUserId };
  }
  return {};
};

const mergeFilters = (...filters) => {
  const parts = filters.filter((filter) => filter && Object.keys(filter).length > 0);
  if (!parts.length) return {};
  return parts.length === 1 ? parts[0] : { $and: parts };
};

module.exports = {
  normalizePhoneDigits,
  normalizePhoneKey,
  buildContactPhoneCandidates,
  buildContactPhoneLookupFilter,
  buildContactIdentityScopeFilter,
  mergeFilters
};
