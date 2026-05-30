const mongoose = require('mongoose');

const toCleanString = (value = '') => String(value || '').trim();

const normalizePhoneDigits = (value = '') => toCleanString(value).replace(/\D/g, '');

const escapeRegExp = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const buildPhoneCandidates = (value = '') => {
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

const buildPhoneSuffixCandidates = (value = '') => {
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

const buildConversationPhoneLookupFilter = (value = '') => {
  const rawValue = toCleanString(value);
  const normalizedPhone = normalizePhoneDigits(rawValue);
  if (!rawValue && !normalizedPhone) return null;

  const exactCandidates = buildPhoneCandidates(rawValue);
  const suffixCandidates = buildPhoneSuffixCandidates(rawValue);

  const filters = [];
  const phoneFieldNames = ['contactPhone', 'contactPhoneDigits', 'phone', 'phoneNumber', 'contactId.phone', 'contactId.mobile'];
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

const getConversationIdentityTokens = (conversation = {}) => {
  const contactId = toCleanString(conversation?.contactId?._id || conversation?.contactId);
  const contactPhoneDigits = normalizePhoneDigits(
    conversation?.contactPhoneDigits ||
      conversation?.contactPhone ||
      conversation?.contactId?.phone ||
      conversation?.contactId?.mobile ||
      conversation?.phone ||
      conversation?.phoneNumber ||
      ''
  );
  const phoneSuffix = contactPhoneDigits.length > 10 ? contactPhoneDigits.slice(-10) : '';

  const tokens = [];
  if (contactId && mongoose.Types.ObjectId.isValid(contactId)) {
    tokens.push(`contact:${contactId}`);
  }
  if (contactPhoneDigits) {
    tokens.push(`phone:${contactPhoneDigits}`);
  }
  if (phoneSuffix && phoneSuffix !== contactPhoneDigits) {
    tokens.push(`phone:${phoneSuffix}`);
  }

  return Array.from(new Set(tokens));
};

const conversationsShareIdentity = (left = {}, right = {}) => {
  const leftTokens = new Set(getConversationIdentityTokens(left));
  if (!leftTokens.size) return false;
  return getConversationIdentityTokens(right).some((token) => leftTokens.has(token));
};

const mergeConversationRecords = (existing = {}, incoming = {}) => {
  const merged = { ...existing };

  const chooseLatestDate = (...values) => {
    const dates = values
      .map((value) => (value ? new Date(value) : null))
      .filter((value) => value instanceof Date && !Number.isNaN(value.getTime()));
    if (!dates.length) return null;
    return new Date(Math.max(...dates.map((value) => value.getTime())));
  };

  const chooseEarliestDate = (...values) => {
    const dates = values
      .map((value) => (value ? new Date(value) : null))
      .filter((value) => value instanceof Date && !Number.isNaN(value.getTime()));
    if (!dates.length) return null;
    return new Date(Math.min(...dates.map((value) => value.getTime())));
  };

  if (
    existing?.contactId &&
    typeof existing.contactId === 'object' &&
    !Array.isArray(existing.contactId)
  ) {
    merged.contactId = existing.contactId;
  }
  if (
    incoming?.contactId &&
    typeof incoming.contactId === 'object' &&
    !Array.isArray(incoming.contactId)
  ) {
    merged.contactId = incoming.contactId;
  }

  for (const [key, value] of Object.entries(incoming || {})) {
    if (value === undefined || value === null || value === '') {
      continue;
    }

    if (merged[key] === undefined || merged[key] === null || merged[key] === '') {
      merged[key] = value;
    }
  }

  merged.lastMessageTime = chooseLatestDate(
    existing?.lastMessageTime,
    incoming?.lastMessageTime
  ) || incoming?.lastMessageTime || existing?.lastMessageTime;
  const getTime = (value) => {
    const date = value ? new Date(value) : null;
    return date instanceof Date && !Number.isNaN(date.getTime()) ? date.getTime() : 0;
  };
  const latestMessageSource =
    getTime(incoming?.lastMessageTime || incoming?.updatedAt || incoming?.createdAt) >=
    getTime(existing?.lastMessageTime || existing?.updatedAt || existing?.createdAt)
      ? incoming
      : existing;
  [
    'lastMessage',
    'lastMessageMediaType',
    'lastMessageAttachmentName',
    'lastMessageAttachmentPages',
    'lastMessageFrom',
    'lastMessageWhatsappMessageId',
    'lastMessageStatus'
  ].forEach((key) => {
    if (latestMessageSource?.[key] !== undefined && latestMessageSource?.[key] !== null) {
      merged[key] = latestMessageSource[key];
    }
  });
  merged.updatedAt = chooseLatestDate(existing?.updatedAt, incoming?.updatedAt) || merged.lastMessageTime;
  merged.createdAt = chooseEarliestDate(existing?.createdAt, incoming?.createdAt) || existing?.createdAt || incoming?.createdAt;

  if (!String(merged.contactPhoneDigits || '').trim()) {
    merged.contactPhoneDigits = normalizePhoneDigits(
      incoming?.contactPhoneDigits || existing?.contactPhoneDigits || merged.contactPhone || ''
    );
  }

  if (!String(merged.contactPhone || '').trim() && String(merged.contactPhoneDigits || '').trim()) {
    merged.contactPhone = merged.contactPhoneDigits;
  }

  return merged;
};

const dedupeConversationsByIdentity = (conversations = []) => {
  const deduped = [];
  const tokenIndex = new Map();

  for (const conversation of Array.isArray(conversations) ? conversations : []) {
    const tokens = getConversationIdentityTokens(conversation);
    const matchIndex = tokens
      .map((token) => tokenIndex.get(token))
      .find((value) => Number.isInteger(value) && value >= 0);

    if (matchIndex === undefined) {
      const index = deduped.length;
      deduped.push(conversation);
      tokens.forEach((token) => tokenIndex.set(token, index));
      continue;
    }

    const merged = mergeConversationRecords(deduped[matchIndex], conversation);
    deduped[matchIndex] = merged;
    tokens.forEach((token) => tokenIndex.set(token, matchIndex));
    getConversationIdentityTokens(merged).forEach((token) => tokenIndex.set(token, matchIndex));
  }

  return deduped;
};

module.exports = {
  buildConversationPhoneLookupFilter,
  buildPhoneCandidates,
  conversationsShareIdentity,
  dedupeConversationsByIdentity,
  getConversationIdentityTokens,
  mergeConversationRecords,
  normalizePhoneDigits
};
