const Contact = require('../../models/Contact');
const Conversation = require('../../models/Conversation');
const { normalizeRole, isTenantWideRole } = require('../../utils/accessControl');
const {
  buildConversationPhoneLookupFilter,
  buildPhoneCandidates,
  normalizePhoneDigits
} = require('../../utils/conversationIdentity');
const buildPhoneLookupFilters = buildConversationPhoneLookupFilter;
const {
  buildContactPhoneLookupFilter,
  buildContactIdentityScopeFilter,
  mergeFilters
} = require('../../utils/contactIdentity');
const {
  buildConversationAssignmentPatch,
  isConversationAssignedToDifferentAgent
} = require('../../utils/conversationAssignment');
const {
  syncConversationSummaryFromConversation
} = require('../../services/conversationSummaryService');

const buildCompanyScopeFilter = (companyId) => (companyId ? { companyId } : {});

const buildActorConversationAccessFilter = ({ userId = '', companyId = '' } = {}) => ({
  ...buildCompanyScopeFilter(companyId),
  $or: [
    { userId },
    { assignedTo: userId },
    { assignedToId: userId },
    { assignedAgent: userId }
  ]
});

const saveAssignmentForActor = async ({ conversation, contact = null, actorUserId = '' } = {}) => {
  if (!conversation?._id) return conversation;
  const resolvedContact =
    contact ||
    (conversation.contactId
      ? await Contact.findOne({
          _id: conversation.contactId,
          ...(conversation.companyId ? { companyId: conversation.companyId } : {})
        }).lean()
      : null);
  const patch = buildConversationAssignmentPatch({
    conversation,
    contact: resolvedContact,
    actorUserId,
    preferActorForOwnerless: false,
    allowActorFallback: false
  });
  if (!Object.keys(patch).length) {
    return conversation;
  }
  Object.assign(conversation, patch);
  await conversation.save();
  await syncConversationSummaryFromConversation(conversation);
  return conversation;
};

const maybeApplySort = async (queryLike, sortSpec) => {
  if (!queryLike) return null;
  if (typeof queryLike.sort === 'function') {
    return queryLike.sort(sortSpec);
  }
  return queryLike;
};

const resolveConversationForOutboundSend = async ({
  userId,
  companyId,
  conversationId,
  to,
  isTenantWide = false
}) => {
  if (!userId || !conversationId) return null;

  const baseIdQuery = {
    _id: conversationId,
    ...buildCompanyScopeFilter(companyId)
  };
  const byScope = await Conversation.findOne(
<<<<<<< Updated upstream
    isTenantWide ? baseIdQuery : { ...baseIdQuery, ...buildActorConversationAccessFilter({ userId, companyId }) }
  );
  if (byScope) {
    return saveAssignmentForActor({ conversation: byScope, actorUserId: userId });
  }
=======
    isTenantWide ? baseIdQuery : { ...baseIdQuery, userId }
  );
  if (byScope) return byScope;
>>>>>>> Stashed changes

  const phoneCandidates = buildPhoneCandidates(to);
  const phoneLookupFilter = buildConversationPhoneLookupFilter(to);
  if (!phoneCandidates.length && !phoneLookupFilter) return null;

<<<<<<< Updated upstream
  const byPhone = await maybeApplySort(
    Conversation.findOne(
      {
        ...buildCompanyScopeFilter(companyId),
        ...(phoneLookupFilter || { contactPhone: { $in: phoneCandidates } })
      }
    ),
    { createdAt: 1, updatedAt: 1, lastMessageTime: 1, _id: 1 }
=======
  return maybeApplySort(
    Conversation.findOne(
      isTenantWide
        ? {
            ...buildCompanyScopeFilter(companyId),
            ...(phoneLookupFilter || { contactPhone: { $in: phoneCandidates } })
          }
        : {
            userId,
            ...buildCompanyScopeFilter(companyId),
            ...(phoneLookupFilter || { contactPhone: { $in: phoneCandidates } })
          }
    ),
    { lastMessageTime: -1, updatedAt: -1, createdAt: -1 }
>>>>>>> Stashed changes
  );

  if (
    byPhone &&
    !isTenantWide &&
    isConversationAssignedToDifferentAgent({ conversation: byPhone, actorUserId: userId })
  ) {
    return null;
  }

  return saveAssignmentForActor({ conversation: byPhone, actorUserId: userId });
};

const resolveContactForTemplateSend = async ({
  userId,
  companyId,
  contactId,
  to,
  contactName = ''
}) => {
  if (!userId) {
    return { contact: null, createdContact: false };
  }

  const trimmedContactName = String(contactName || '').trim();
  const phoneCandidates = buildPhoneCandidates(to);
  const phoneLookupFilter = buildConversationPhoneLookupFilter(to);
  const rawPhoneValue = String(to || '').trim();

  let contact = null;

  if (contactId) {
    contact = await Contact.findOne(
      mergeFilters(
        buildContactIdentityScopeFilter({ companyId, userId }),
        { _id: contactId }
      )
    );
  }

  if (!contact && phoneLookupFilter) {
    contact = await Contact.findOne(
      mergeFilters(
        buildContactIdentityScopeFilter({ companyId, userId }),
        buildContactPhoneLookupFilter(to) || phoneLookupFilter
      )
    ).sort({ createdAt: 1, updatedAt: 1 });
  }

  if (contact) {
    return { contact, createdContact: false };
  }

  const now = new Date();
  const storedPhone = rawPhoneValue || phoneCandidates[0];
  const createdContact = await Contact.create({
    userId,
    companyId: companyId || null,
    name: trimmedContactName || storedPhone,
    phone: storedPhone,
    source: 'whatsapp_template',
    sourceType: 'manual',
    lastContact: now,
    lastContactAt: now
  });

  return { contact: createdContact, createdContact: true };
};

const resolveOrCreateConversationForTemplateSend = async ({
  userId,
  companyId,
  conversationId,
  contactId,
  to,
  contactName = '',
  isTenantWide = false
}) => {
  const existingConversation = await resolveConversationForOutboundSend({
    userId,
    companyId,
    conversationId,
    to,
    isTenantWide
  });

  if (existingConversation) {
    const contactLookup = await resolveContactForTemplateSend({
      userId,
      companyId: existingConversation.companyId || companyId || null,
      contactId: existingConversation.contactId || contactId,
      to: existingConversation.contactPhone || to,
      contactName: existingConversation.contactName || contactName
    });

    const assignedConversation = await saveAssignmentForActor({
      conversation: existingConversation,
      contact: contactLookup.contact,
      actorUserId: userId
    });

    return {
      conversation: assignedConversation,
      contact: contactLookup.contact,
      createdContact: false,
      createdConversation: false
    };
  }

  const { contact, createdContact } = await resolveContactForTemplateSend({
    userId,
    companyId,
    contactId,
    to,
    contactName
  });

  const phoneCandidates = buildPhoneCandidates(contact?.phone || to);
  const conversationPhoneLookupFilter = buildConversationPhoneLookupFilter(contact?.phone || to);
  const conversationMatchConditions = [
    buildCompanyScopeFilter(companyId),
    contact?._id
      ? {
          $or: [
            { contactId: contact._id },
            conversationPhoneLookupFilter || { contactPhone: { $in: phoneCandidates } }
          ]
        }
      : null
  ].filter(Boolean);

  let conversation = await maybeApplySort(
    Conversation.findOne(
      conversationMatchConditions.length === 1
        ? conversationMatchConditions[0]
        : { $and: conversationMatchConditions }
    ),
    { createdAt: 1, updatedAt: 1, lastMessageTime: 1, _id: 1 }
  );

  let createdConversation = false;
  if (
    conversation &&
    !isTenantWide &&
    isConversationAssignedToDifferentAgent({ conversation, contact, actorUserId: userId })
  ) {
    return {
      conversation: null,
      contact,
      createdContact,
      createdConversation: false
    };
  }

  if (!conversation) {
    const assignmentPatch = buildConversationAssignmentPatch({
      contact,
      actorUserId: userId,
      preferActorForOwnerless: false,
      allowActorFallback: false
    });
    conversation = await Conversation.create({
      userId,
      companyId: companyId || null,
      contactId: contact._id,
      contactPhone: String(contact?.phone || to || '').trim(),
      contactName: String(contact?.name || contactName || to || '').trim(),
      channel: 'whatsapp',
      status: 'active',
      ...assignmentPatch,
      lastMessageTime: new Date(),
      lastMessage: '',
      lastMessageMediaType: '',
      lastMessageAttachmentName: '',
      lastMessageAttachmentPages: null,
      lastMessageFrom: 'agent',
      lastMessageWhatsappMessageId: '',
      unreadCount: 0
    });
    createdConversation = true;
    await syncConversationSummaryFromConversation(conversation);
  } else if (
    !isTenantWide ||
    Object.keys(
      buildConversationAssignmentPatch({
        conversation,
        contact,
        actorUserId: userId,
        allowActorFallback: false
      })
    ).length
  ) {
    conversation = await saveAssignmentForActor({
      conversation,
      contact,
      actorUserId: userId
    });
  }

  return {
    conversation,
    contact,
    createdContact,
    createdConversation
  };
};

const markOutboundTemplateContactActivity = async ({ contact, contactName = '' }) => {
  if (!contact || typeof contact.save !== 'function') return contact;

  const now = new Date();
  const trimmedContactName = String(contactName || '').trim();

  contact.lastContact = now;
  contact.lastContactAt = now;
  if (trimmedContactName && !String(contact.name || '').trim()) {
    contact.name = trimmedContactName;
  }
  if (!String(contact.source || '').trim()) {
    contact.source = 'whatsapp_template';
  }

  await contact.save();
  return contact;
};

const cleanupCreatedTemplateOutreachTarget = async ({
  conversation,
  contact,
  createdConversation = false,
  createdContact = false
}) => {
  if (createdConversation && conversation?._id) {
    await Conversation.deleteOne({ _id: conversation._id });
  }

  if (createdContact && contact?._id) {
    await Contact.deleteOne({ _id: contact._id });
  }
};

module.exports = {
  normalizePhoneDigits,
  buildPhoneCandidates,
  buildPhoneLookupFilters,
  resolveConversationForOutboundSend,
  resolveOrCreateConversationForTemplateSend,
  markOutboundTemplateContactActivity,
  cleanupCreatedTemplateOutreachTarget
};
