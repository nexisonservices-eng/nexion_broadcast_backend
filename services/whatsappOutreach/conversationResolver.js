const Contact = require('../../models/Contact');
const Conversation = require('../../models/Conversation');

const normalizePhoneDigits = (value = '') => String(value || '').replace(/\D/g, '');

const buildPhoneCandidates = (value = '') => {
  const rawValue = String(value || '').trim();
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

const buildCompanyFallbackFilter = (companyId) =>
  companyId
    ? {
        $or: [{ companyId }, { companyId: null }, { companyId: { $exists: false } }]
      }
    : {};

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
  to
}) => {
  if (!userId || !conversationId) return null;

  const baseIdQuery = { _id: conversationId, userId };

  if (companyId) {
    const strict = await Conversation.findOne({ ...baseIdQuery, companyId });
    if (strict) return strict;
  }

  const byUserOnly = await Conversation.findOne(baseIdQuery);
  if (byUserOnly) return byUserOnly;

  const phoneCandidates = buildPhoneCandidates(to);
  if (!phoneCandidates.length) return null;

  return maybeApplySort(
    Conversation.findOne({
      userId,
      ...buildCompanyFallbackFilter(companyId),
      contactPhone: { $in: phoneCandidates }
    }),
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
  const rawPhoneValue = String(to || '').trim();

  let contact = null;

  if (contactId) {
    if (companyId) {
      contact = await Contact.findOne({ _id: contactId, userId, companyId });
    }
    if (!contact) {
      contact = await Contact.findOne({
        _id: contactId,
        userId,
        ...buildCompanyFallbackFilter(companyId)
      });
    }
  }

  if (!contact && phoneCandidates.length > 0) {
    if (companyId) {
      contact = await Contact.findOne({
        userId,
        companyId,
        phone: { $in: phoneCandidates }
      });
    }

    if (!contact) {
      contact = await Contact.findOne({
        userId,
        ...buildCompanyFallbackFilter(companyId),
        phone: { $in: phoneCandidates }
      });
    }
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
  contactName = ''
}) => {
  const existingConversation = await resolveConversationForOutboundSend({
    userId,
    companyId,
    conversationId,
    to
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
  const companyFallbackFilter = buildCompanyFallbackFilter(companyId);
  const conversationMatchConditions = [
    { userId },
    contact?._id ? { $or: [{ contactId: contact._id }, { contactPhone: { $in: phoneCandidates } }] } : null
  ].filter(Boolean);

  if (companyId) {
    conversationMatchConditions.splice(1, 0, companyFallbackFilter);
  }

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
      status: 'active',
      lastMessageTime: new Date(),
      lastMessage: '',
      lastMessageMediaType: '',
      lastMessageAttachmentName: '',
      lastMessageAttachmentPages: null,
      lastMessageFrom: 'agent',
      unreadCount: 0
    });
    createdConversation = true;
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
  resolveConversationForOutboundSend,
  resolveOrCreateConversationForTemplateSend,
  markOutboundTemplateContactActivity,
  cleanupCreatedTemplateOutreachTarget
};
