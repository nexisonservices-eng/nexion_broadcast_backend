const mongoose = require('mongoose');

const DEFAULT_PAGE_LIMIT = 30;
const MAX_PAGE_LIMIT = 80;

const toCleanString = (value) => String(value || '').trim();

const normalizePageLimit = (rawLimit, { fallback = DEFAULT_PAGE_LIMIT, max = MAX_PAGE_LIMIT } = {}) => {
  const parsedLimit = Number(rawLimit);
  if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
    return Math.max(1, Math.min(fallback, max));
  }

  return Math.max(1, Math.min(parsedLimit, max));
};

const encodeCursorPayload = (payload) => {
  try {
    return Buffer.from(JSON.stringify(payload)).toString('base64url');
  } catch {
    return '';
  }
};

const decodeCursorPayload = (cursor = '', { timestampKeys = ['timestamp', 'createdAt'] } = {}) => {
  const normalizedCursor = toCleanString(cursor);
  if (!normalizedCursor) return null;

  try {
    const decoded = JSON.parse(Buffer.from(normalizedCursor, 'base64url').toString('utf8'));
    const id = toCleanString(decoded?.id);
    const timestampSource = timestampKeys
      .map((key) => decoded?.[key])
      .find((value) => value);
    const timestamp = new Date(timestampSource || '');

    if (!id || Number.isNaN(timestamp.getTime())) {
      return null;
    }

    return {
      timestamp,
      id
    };
  } catch {
    const fallbackTimestamp = new Date(normalizedCursor);
    if (Number.isNaN(fallbackTimestamp.getTime())) return null;
    return {
      timestamp: fallbackTimestamp,
      id: ''
    };
  }
};

const buildDescendingTimestampCursorFilter = (cursor) => {
  if (!cursor?.timestamp) return {};

  const cursorTimestamp = new Date(cursor.timestamp);
  if (Number.isNaN(cursorTimestamp.getTime())) return {};

  const cursorId = toCleanString(cursor.id);
  const cursorObjectId = cursorId && mongoose.Types.ObjectId.isValid(cursorId)
    ? new mongoose.Types.ObjectId(cursorId)
    : null;
  return {
    $or: [
      { timestamp: { $lt: cursorTimestamp } },
      {
        timestamp: cursorTimestamp,
        _id: cursorObjectId ? { $lt: cursorObjectId } : { $exists: true }
      }
    ]
  };
};

const buildChronologicalPage = ({ documents = [], limit, encodeCursor }) => {
  const hasMore = documents.length > limit;
  const trimmedDocuments = hasMore ? documents.slice(0, limit) : documents;
  const chronologicalDocuments = [...trimmedDocuments].reverse();
  const nextCursor =
    hasMore && trimmedDocuments.length
      ? encodeCursor(trimmedDocuments[trimmedDocuments.length - 1])
      : null;

  return {
    items: chronologicalDocuments,
    hasMore,
    nextCursor
  };
};

const encodeMessageCursor = (message = {}) => {
  const timestamp = message?.timestamp || message?.whatsappTimestamp || message?.createdAt || null;
  const id = toCleanString(message?._id);
  if (!timestamp || !id) {
    return '';
  }

  return encodeCursorPayload({
    timestamp,
    id
  });
};

const decodeMessageCursor = (cursor = '') => decodeCursorPayload(cursor);

const buildMessageCursorFilter = (cursor) => buildDescendingTimestampCursorFilter(cursor);

const encodeAttachmentCursor = (message = {}) => encodeMessageCursor(message);

const decodeAttachmentCursor = (cursor = '') => decodeMessageCursor(cursor);

module.exports = {
  DEFAULT_PAGE_LIMIT,
  MAX_PAGE_LIMIT,
  normalizePageLimit,
  buildChronologicalPage,
  buildDescendingTimestampCursorFilter,
  buildMessageCursorFilter,
  decodeMessageCursor,
  encodeMessageCursor,
  decodeAttachmentCursor,
  encodeAttachmentCursor
};
