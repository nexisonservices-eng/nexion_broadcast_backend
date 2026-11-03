const normalizeConversationId = (value) => {
  if (value == null) return '';
  if (typeof value === 'object' && typeof value.toString === 'function') {
    return String(value.toString() || '').trim();
  }
  return String(value).trim();
};

const filterMessagesForConversation = (messages, conversationId) => {
  const requestedId = normalizeConversationId(conversationId);
  if (!requestedId || !Array.isArray(messages)) return [];
  return messages.filter(
    (message) => normalizeConversationId(message?.conversationId) === requestedId
  );
};

module.exports = { filterMessagesForConversation };
