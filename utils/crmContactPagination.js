const mongoose = require('mongoose');

const CONTACT_CURSOR_SORT = Object.freeze({
  nextFollowUpAt: 1,
  leadScore: -1,
  lastContact: -1,
  createdAt: -1,
  _id: -1
});

const toCleanString = (value = '') => String(value || '').trim();

const toDateValue = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const toNumberValue = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const encodeCrmContactCursor = (contact = {}) => {
  const nextFollowUpAt = toDateValue(contact?.nextFollowUpAt);
  const leadScore = toNumberValue(contact?.leadScore);
  const lastContact = toDateValue(contact?.lastContact);
  const createdAt = toDateValue(contact?.createdAt);
  const id = toCleanString(contact?._id);

  if (!id || !createdAt) {
    return '';
  }

  return Buffer.from(
    JSON.stringify({
      nextFollowUpAt: nextFollowUpAt ? nextFollowUpAt.toISOString() : null,
      leadScore,
      lastContact: lastContact ? lastContact.toISOString() : null,
      createdAt: createdAt.toISOString(),
      id
    })
  ).toString('base64url');
};

const decodeCrmContactCursor = (cursor = '') => {
  const normalized = toCleanString(cursor);
  if (!normalized) return null;

  try {
    const decoded = JSON.parse(Buffer.from(normalized, 'base64url').toString('utf8'));
    const createdAt = toDateValue(decoded?.createdAt);
    const id = toCleanString(decoded?.id);

    if (!createdAt || !id || !mongoose.Types.ObjectId.isValid(id)) {
      return null;
    }

    return {
      nextFollowUpAt: toDateValue(decoded?.nextFollowUpAt),
      leadScore: toNumberValue(decoded?.leadScore),
      lastContact: toDateValue(decoded?.lastContact),
      createdAt,
      id
    };
  } catch {
    return null;
  }
};

const buildCrmContactCursorFilter = (cursor = {}) => {
  const createdAt = toDateValue(cursor?.createdAt);
  const id = toCleanString(cursor?.id);
  if (!createdAt || !id || !mongoose.Types.ObjectId.isValid(id)) {
    return {};
  }

  const cursorObjectId = new mongoose.Types.ObjectId(id);
  const sortValues = {
    nextFollowUpAt: cursor?.nextFollowUpAt === undefined ? null : toDateValue(cursor.nextFollowUpAt),
    leadScore: cursor?.leadScore === undefined ? null : toNumberValue(cursor.leadScore),
    lastContact: cursor?.lastContact === undefined ? null : toDateValue(cursor.lastContact),
    createdAt,
    _id: cursorObjectId
  };

  const sortFields = [
    { field: 'nextFollowUpAt', direction: 1 },
    { field: 'leadScore', direction: -1 },
    { field: 'lastContact', direction: -1 },
    { field: 'createdAt', direction: -1 },
    { field: '_id', direction: -1 }
  ];

  const clauses = [];
  sortFields.forEach((spec, index) => {
    const value = sortValues[spec.field];
    if (value === undefined) return;

    const clause = {};
    for (let i = 0; i < index; i += 1) {
      const prefixField = sortFields[i].field;
      clause[prefixField] = sortValues[prefixField];
    }

    clause[spec.field] = spec.direction === 1 ? { $gt: value } : { $lt: value };
    clauses.push(clause);
  });

  return clauses.length ? { $or: clauses } : {};
};

module.exports = {
  CONTACT_CURSOR_SORT,
  buildCrmContactCursorFilter,
  decodeCrmContactCursor,
  encodeCrmContactCursor
};
