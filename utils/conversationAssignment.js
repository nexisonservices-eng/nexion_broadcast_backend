const mongoose = require('mongoose');

const toCleanString = (value = '') => String(value || '').trim();

const toObjectIdString = (value = '') => {
  const rawValue = value && typeof value === 'object' && value._id ? value._id : value;
  const normalized = toCleanString(rawValue);
  return normalized && mongoose.Types.ObjectId.isValid(normalized) ? normalized : null;
};

const firstClean = (...values) =>
  values
    .map((value) => toCleanString(value && typeof value === 'object' && value._id ? value._id : value))
    .find(Boolean) || '';

const getConversationAssignee = (conversation = {}) => {
  const source = conversation || {};
  return firstClean(source.assignedTo, source.assignedAgent, source.assignedToId);
};

const getContactAssignee = (contact = {}) => {
  const source = contact || {};
  return firstClean(source.assignedTo, source.assignedAgent, source.assignedToId, source.ownerId);
};

const getOwnerLikeIds = ({ conversation = {}, contact = {} } = {}) => {
  const conversationSource = conversation || {};
  const contactSource = contact || {};
  return new Set(
    [
      conversationSource.userId,
      conversationSource.createdBy,
      contactSource.userId,
      contactSource.createdBy
    ]
      .map((value) => firstClean(value))
      .filter(Boolean)
  );
};

const isConversationAssignedToDifferentAgent = ({
  conversation = {},
  actorUserId = '',
  contact = {}
} = {}) => {
  const actor = firstClean(actorUserId);
  const currentAssignee = getConversationAssignee(conversation);
  if (!actor || !currentAssignee || currentAssignee === actor) {
    return false;
  }

  const ownerLikeIds = getOwnerLikeIds({ conversation, contact });
  return !ownerLikeIds.has(currentAssignee);
};

const buildConversationAssignmentPatch = ({
  conversation = {},
  contact = {},
  actorUserId = '',
  preferActorForOwnerless = false,
  allowActorFallback = true
} = {}) => {
  const actor = firstClean(actorUserId);
  const currentAssignee = getConversationAssignee(conversation);
  const contactAssignee = getContactAssignee(contact);
  const ownerLikeIds = getOwnerLikeIds({ conversation, contact });
  const currentAssigneeIsOwnerLike = currentAssignee && ownerLikeIds.has(currentAssignee);
  const actorIsOwnerLike = actor && ownerLikeIds.has(actor);
  const actorOwnsConversation = actor && firstClean((conversation || {}).userId) === actor;

  let nextAssignee = currentAssignee || contactAssignee || (allowActorFallback ? actor : '') || '';

  if (
    allowActorFallback &&
    preferActorForOwnerless &&
    actor &&
    (actorOwnsConversation || !actorIsOwnerLike) &&
    (!currentAssignee || currentAssigneeIsOwnerLike)
  ) {
    nextAssignee = actor;
  }

  if (!nextAssignee) {
    return {};
  }

  const patch = {};
  if (toCleanString(conversation.assignedTo) !== nextAssignee) {
    patch.assignedTo = nextAssignee;
  }
  if (toCleanString(conversation.assignedAgent) !== nextAssignee) {
    patch.assignedAgent = nextAssignee;
  }

  const objectIdValue = toObjectIdString(nextAssignee);
  const currentAssignedToId = toCleanString(conversation.assignedToId);
  if (objectIdValue && currentAssignedToId !== objectIdValue) {
    patch.assignedToId = objectIdValue;
  } else if (!objectIdValue && currentAssignedToId) {
    patch.assignedToId = null;
  }

  return patch;
};

const collectConversationParticipantUserIds = (conversation = {}, ...extraUserIds) =>
  Array.from(
    new Set(
      [
        conversation.userId,
        conversation.createdBy,
        conversation.assignedTo,
        conversation.assignedAgent,
        conversation.assignedToId,
        ...extraUserIds
      ]
        .map((value) => firstClean(value))
        .filter(Boolean)
    )
  );

module.exports = {
  buildConversationAssignmentPatch,
  collectConversationParticipantUserIds,
  getConversationAssignee,
  isConversationAssignedToDifferentAgent
};
