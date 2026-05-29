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
  syncConversationSummaryFromConversation
} = require('../../services/conversationSummaryService');

const buildCompanyScopeFilter = (companyId) => (companyId ? { companyId } : {});

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
    isTenantWide ? baseIdQuery : { ...baseIdQuery, userId }
  );
  if (byScope) return byScope;

  const phoneCandidates = buildPhoneCandidates(to);
  const phoneLookupFilter = buildConversationPhoneLookupFilter(to);
  if (!phoneCandidates.length && !phoneLookupFilter) return null;

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
  );
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
    contact = await Contact.findOne({
      _id: contactId,
      userId,
      ...buildCompanyScopeFilter(companyId)
    });
  }

  if (!contact && phoneLookupFilter) {
    contact = await Contact.findOne({
      userId,
      ...buildCompanyScopeFilter(companyId),
      ...phoneLookupFilter
    });
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

    return {
      conversation: existingConversation,
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
    { userId },
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
    { lastMessageTime: -1, updatedAt: -1, createdAt: -1 }
  );

  let createdConversation = false;
  if (!conversation) {
    conversation = await Conversation.create({
      userId,
      companyId: companyId || null,
      contactId: contact._id,
      contactPhone: String(contact?.phone || to || '').trim(),
      contactName: String(contact?.name || contactName || to || '').trim(),
      channel: 'whatsapp',
      status: 'active',
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
