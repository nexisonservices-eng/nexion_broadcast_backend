const mongoose = require('mongoose');
const ConversationSummary = require('../models/ConversationSummary');

const toObjectIdValue = (value) => {
  const rawValue = value && typeof value === 'object' && value._id ? value._id : value;
  const normalized = String(rawValue || '').trim();
  return normalized || null;
};

const toCleanString = (value = '') => String(value || '').trim();

const toLowerCleanString = (value = '') => toCleanString(value).toLowerCase();

const toDigitsString = (value = '') => toCleanString(value).replace(/\D/g, '');

const toDateValue = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const toNumberValue = (value, fallback = null) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const isValidObjectIdValue = (value) => {
  const normalized = toObjectIdValue(value);
  return Boolean(normalized) && mongoose.Types.ObjectId.isValid(normalized);
};

const normalizeSummaryPayload = (payload = {}) => {
  const hasStatus = payload?.status !== undefined;
  const normalizedStatus = hasStatus ? toCleanString(payload?.status) : undefined;
  const hasLastMessageTime = payload?.lastMessageTime !== undefined;
  const hasResolvedAt = payload?.resolvedAt !== undefined;
  const resolvedResolvedAt =
    hasResolvedAt
      ? toDateValue(payload?.resolvedAt)
      : hasStatus && normalizedStatus === 'resolved'
        ? new Date()
        : undefined;

  return {
    conversationId: toObjectIdValue(payload?.conversationId),
    userId: payload?.userId === undefined ? undefined : toObjectIdValue(payload?.userId),
    companyId: payload?.companyId === undefined ? undefined : toObjectIdValue(payload?.companyId),
    contactId: payload?.contactId === undefined ? undefined : toObjectIdValue(payload?.contactId),
    contactPhone:
      payload?.contactPhone === undefined ? undefined : toCleanString(payload?.contactPhone),
    contactPhoneDigits:
      payload?.contactPhone === undefined ? undefined : toDigitsString(payload?.contactPhone),
    contactName:
      payload?.contactName === undefined ? undefined : toCleanString(payload?.contactName),
    contactNameLower:
      payload?.contactName === undefined ? undefined : toLowerCleanString(payload?.contactName),
    status: hasStatus ? (normalizedStatus || 'active') : undefined,
    assignedTo: payload?.assignedTo === undefined ? undefined : (toCleanString(payload?.assignedTo) || null),
    assignedToId:
      payload?.assignedToId === undefined ? undefined : toObjectIdValue(payload?.assignedToId),
    tags: Array.isArray(payload?.tags)
      ? Array.from(new Set(payload.tags.map((tag) => toCleanString(tag)).filter(Boolean)))
      : undefined,
    priority: payload?.priority === undefined ? undefined : (toCleanString(payload?.priority) || undefined),
    lastMessageTime: hasLastMessageTime ? toDateValue(payload?.lastMessageTime) : undefined,
    lastMessage:
      payload?.lastMessage === undefined ? undefined : toCleanString(payload?.lastMessage),
    lastMessageMediaType:
      payload?.lastMessageMediaType === undefined
        ? undefined
        : toCleanString(payload?.lastMessageMediaType),
    lastMessageAttachmentName:
      payload?.lastMessageAttachmentName === undefined
        ? undefined
        : toCleanString(payload?.lastMessageAttachmentName),
    lastMessageAttachmentPages:
      payload?.lastMessageAttachmentPages === undefined
        ? undefined
        : toNumberValue(payload?.lastMessageAttachmentPages, null),
    lastMessageFrom:
      payload?.lastMessageFrom === undefined ? undefined : toCleanString(payload?.lastMessageFrom) || undefined,
    lastMessageWhatsappMessageId:
      payload?.lastMessageWhatsappMessageId === undefined
        ? undefined
        : toCleanString(payload?.lastMessageWhatsappMessageId),
    lastMessageStatus:
      payload?.lastMessageStatus === undefined
        ? undefined
        : toCleanString(payload?.lastMessageStatus),
    unreadCount:
      payload?.unreadCount === undefined ? undefined : Math.max(0, toNumberValue(payload?.unreadCount, 0)),
    notes: payload?.notes === undefined ? undefined : toCleanString(payload?.notes),
    resolvedAt: resolvedResolvedAt
  };
};

const buildSummaryUpdate = (payload = {}) => {
  const normalized = normalizeSummaryPayload(payload);
  const conversationId = normalized.conversationId || toObjectIdValue(payload?._id);
  const userId = normalized.userId;
  const companyId = normalized.companyId;

  if (!conversationId || !userId) {
    return null;
  }

  if (!isValidObjectIdValue(conversationId) || !isValidObjectIdValue(userId)) {
    return null;
  }

  const now = new Date();
  const baseSet = {
    conversationId,
    userId,
    updatedAt: now
  };
  if (companyId !== undefined) {
    baseSet.companyId = companyId;
  }

  const update = {
    $set: baseSet,
    $setOnInsert: {
      createdAt: now
    }
  };

  const optionalKeys = [
    'contactId',
    'contactPhone',
    'contactName',
    'contactPhoneDigits',
    'contactNameLower',
    'status',
    'assignedTo',
    'assignedToId',
    'tags',
    'priority',
    'lastMessageTime',
    'lastMessage',
    'lastMessageMediaType',
    'lastMessageAttachmentName',
    'lastMessageAttachmentPages',
    'lastMessageFrom',
    'lastMessageWhatsappMessageId',
    'lastMessageStatus',
    'unreadCount',
    'notes',
    'resolvedAt'
  ];

  optionalKeys.forEach((key) => {
    if (normalized[key] !== undefined) {
      update.$set[key] = normalized[key];
    }
  });

  return {
    filter: { conversationId },
    update
  };
};

const upsertConversationSummary = async (payload = {}) => {
  const request = buildSummaryUpdate(payload);
  if (!request) return null;

  return ConversationSummary.findOneAndUpdate(request.filter, request.update, {
    upsert: true,
    new: true,
    setDefaultsOnInsert: true
  }).lean();
};

const upsertConversationSummaries = async (items = []) => {
  const safeItems = Array.isArray(items) ? items.filter(Boolean) : [];
  if (!safeItems.length) return [];

  const ops = [];
  for (const item of safeItems) {
    const request = buildSummaryUpdate(item);
    if (!request) continue;
    ops.push({
      updateOne: {
        filter: request.filter,
        update: request.update,
        upsert: true
      }
    });
  }

  if (!ops.length) return [];
  return ConversationSummary.bulkWrite(ops, { ordered: false });
};

const bulkUpsertConversationSummaries = upsertConversationSummaries;

const syncConversationSummaryFromConversation = async (conversation = {}) => {
  if (!conversation) return null;
  const source =
    typeof conversation.toObject === 'function' ? conversation.toObject() : conversation;
  return upsertConversationSummary(source);
};

const deleteConversationSummary = async (conversationId = '') => {
  const normalized = toObjectIdValue(conversationId);
  if (!normalized) return null;

  return ConversationSummary.deleteOne({ conversationId: normalized });
};

module.exports = {
  deleteConversationSummary,
  bulkUpsertConversationSummaries,
  syncConversationSummaryFromConversation,
  upsertConversationSummaries,
  upsertConversationSummary
};
