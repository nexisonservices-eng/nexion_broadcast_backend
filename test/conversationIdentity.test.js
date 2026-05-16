const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildConversationPhoneLookupFilter,
  dedupeConversationsByIdentity,
  getConversationIdentityTokens
} = require('../utils/conversationIdentity');

test('buildConversationPhoneLookupFilter matches country-code and local phone variants', () => {
  const filter = buildConversationPhoneLookupFilter('9677973676');
  assert.ok(filter);
  const regexFilters = (filter.$or || []).filter((entry) => entry.contactPhone instanceof RegExp);
  assert.ok(regexFilters.length > 0);
  assert.equal(regexFilters.some((entry) => entry.contactPhone.test('919677973676')), true);
});

test('dedupeConversationsByIdentity collapses the same lead across phone formats', () => {
  const conversations = dedupeConversationsByIdentity([
    {
      _id: 'conv-2',
      contactPhone: '9677973676',
      contactName: 'Lead A duplicate',
      lastMessageTime: '2026-05-15T11:00:00.000Z'
    },
    {
      _id: 'conv-1',
      contactPhone: '919677973676',
      contactName: 'Lead A',
      lastMessageTime: '2026-05-15T10:00:00.000Z'
    }
  ]);

  assert.equal(conversations.length, 1);
  assert.equal(conversations[0]._id, 'conv-2');
  assert.equal(conversations[0].contactName, 'Lead A duplicate');
});

test('getConversationIdentityTokens prefers phone digits and stable contact id tokens', () => {
  const tokens = getConversationIdentityTokens({
    contactId: '507f1f77bcf86cd799439011',
    contactPhone: '+91 96779 73676'
  });

  assert.ok(tokens.includes('contact:507f1f77bcf86cd799439011'));
  assert.ok(tokens.includes('phone:919677973676'));
  assert.ok(tokens.includes('phone:9677973676'));
});
