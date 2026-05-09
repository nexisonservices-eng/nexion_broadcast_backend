const Contact = require('../../models/Contact');
const Conversation = require('../../models/Conversation');
const {
  syncConversationSummaryFromConversation
} = require('../../services/conversationSummaryService');

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

const escapeRegExp = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const buildPhoneLookupFilters = (value = '') => {
  const rawValue = String(value || '').trim();
  const normalizedPhone = normalizePhoneDigits(rawValue);
  if (!rawValue && !normalizedPhone) return null;

  const exactCandidates = buildPhoneCandidates(rawValue);
  const digitCandidates = Array.from(
    new Set(
      [
        normalizedPhone,
        normalizedPhone.length > 10 ? normalizedPhone.slice(-10) : '',
        normalizedPhone.length > 11 ? normalizedPhone.slice(-11) : '',
        normalizedPhone.length > 12 ? normalizedPhone.slice(-12) : ''
      ].filter(Boolean)
    )
  );
  const suffixCandidates = Array.from(
    new Set(
      [
        normalizedPhone.length >= 10 ? normalizedPhone.slice(-10) : '',
        normalizedPhone.length >= 11 ? normalizedPhone.slice(-11) : '',
        normalizedPhone.length >= 12 ? normalizedPhone.slice(-12) : ''
      ].filter(Boolean)
    )
  );

  const filters = [];
  if (exactCandidates.length > 0) {
    filters.push({ phone: { $in: exactCandidates } });
  }
  if (digitCandidates.length > 0) {
    filters.push({ phoneDigits: { $in: digitCandidates } });
  }
  suffixCandidates.forEach((suffix) => {
    const escaped = escapeRegExp(suffix);
    filters.push({ phone: new RegExp(`${escaped}$`) });
    filters.push({ phoneDigits: new RegExp(`${escaped}$`) });
  });

  return filters.length > 0 ? { $or: filters } : null;
};

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
  to
}) => {
  if (!userId || !conversationId) return null;

  const baseIdQuery = { _id: conversationId, userId };
  const byScope = await Conversation.findOne({
    ...baseIdQuery,
    ...buildCompanyScopeFilter(companyId)
  });
  if (byScope) return byScope;

  const phoneCandidates = buildPhoneCandidates(to);
  const phoneLookupFilter = buildPhoneLookupFilters(to);
  if (!phoneCandidates.length && !phoneLookupFilter) return null;

  return maybeApplySort(
    Conversation.findOne({
      userId,
      ...buildCompanyScopeFilter(companyId),
      ...(phoneLookupFilter || { contactPhone: { $in: phoneCandidates } })
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
  const phoneLookupFilter = buildPhoneLookupFilters(to);
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
  const conversationMatchConditions = [
    { userId },
    buildCompanyScopeFilter(companyId),
    contact?._id ? { $or: [{ contactId: contact._id }, { contactPhone: { $in: phoneCandidates } }] } : null
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
