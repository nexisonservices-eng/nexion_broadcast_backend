const mongoose = require('mongoose');

const MIN_SORT_DATE = new Date(-8640000000000000);

const toCleanString = (value = '') => String(value || '').trim();

const toDateValue = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const normalizeDealSortDate = (value) => {
  const parsed = toDateValue(value);
  return parsed || new Date(MIN_SORT_DATE.getTime());
};

const encodeDealCursor = (deal = {}) => {
  const expectedCloseAtSort = normalizeDealSortDate(
    deal?.expectedCloseAtSort || deal?.expectedCloseAt || null
  );
  const updatedAt = toDateValue(deal?.updatedAt);
  const createdAt = toDateValue(deal?.createdAt);
  const id = toCleanString(deal?._id);

  if (!id || !updatedAt || !createdAt) {
    return '';
  }

  return Buffer.from(
    JSON.stringify({
      expectedCloseAtSort: expectedCloseAtSort.toISOString(),
      updatedAt: updatedAt.toISOString(),
      createdAt: createdAt.toISOString(),
      id
    })
  ).toString('base64url');
};

const decodeDealCursor = (cursor = '') => {
  const normalized = toCleanString(cursor);
  if (!normalized) return null;

  try {
    const decoded = JSON.parse(Buffer.from(normalized, 'base64url').toString('utf8'));
    const expectedCloseAtSort = toDateValue(decoded?.expectedCloseAtSort);
    const updatedAt = toDateValue(decoded?.updatedAt);
    const createdAt = toDateValue(decoded?.createdAt);
    const id = toCleanString(decoded?.id);

    if (!expectedCloseAtSort || !updatedAt || !createdAt || !id) {
      return null;
    }

    return {
      expectedCloseAtSort,
      updatedAt,
      createdAt,
      id
    };
  } catch {
    return null;
  }
};

const buildDealCursorFilter = (cursor) => {
  if (!cursor?.expectedCloseAtSort || !cursor?.updatedAt || !cursor?.createdAt) {
    return {};
  }

  const expectedCloseAtSort = toDateValue(cursor.expectedCloseAtSort);
  const updatedAt = toDateValue(cursor.updatedAt);
  const createdAt = toDateValue(cursor.createdAt);
  if (!expectedCloseAtSort || !updatedAt || !createdAt) {
    return {};
  }

  const cursorId = toCleanString(cursor.id);
  const cursorObjectId = cursorId && mongoose.Types.ObjectId.isValid(cursorId)
    ? new mongoose.Types.ObjectId(cursorId)
    : null;

  return {
    $or: [
      { expectedCloseAtSort: { $gt: expectedCloseAtSort } },
      {
        expectedCloseAtSort,
        updatedAt: { $lt: updatedAt }
      },
      {
        expectedCloseAtSort,
        updatedAt,
        createdAt: { $lt: createdAt }
      },
      {
        expectedCloseAtSort,
        updatedAt,
        createdAt,
        _id: cursorObjectId ? { $lt: cursorObjectId } : { $exists: true }
      }
    ]
  };
};

module.exports = {
  MIN_SORT_DATE,
  buildDealCursorFilter,
  decodeDealCursor,
  encodeDealCursor,
  normalizeDealSortDate
};
