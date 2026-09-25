const { test } = require('node:test');
const assert = require('node:assert/strict');
const { resolveBroadcastCreator, resolveBroadcastCreators } = require('../utils/broadcastCreator');

const user = { id: 'admin', username: 'Company admin', workspaceCreators: [
  { id: 'agent', name: 'Priya', email: 'priya@example.com', role: 'user' }
] };

test('agent ID resolves to agent name even if the stored label is the company name', () => {
  const source = { createdById: 'agent', createdBy: 'Company admin' };
  const row = resolveBroadcastCreator(source, user);
  assert.equal(row.createdBy, 'Priya');
  assert.equal(row.createdByName, 'Priya');
  assert.equal(row.createdById, 'agent');
  assert.equal(source.createdBy, 'Company admin');
});
test('does not guess an agent when a broadcast belongs to admin or an unknown creator', () => {
  assert.equal(resolveBroadcastCreator({ createdById: 'admin', createdBy: 'Old name' }, user).createdByName, 'Company admin');
  const unknown = { createdById: 'unknown', createdBy: 'Saved creator' };
  assert.deepEqual(resolveBroadcastCreator(unknown, user), unknown);
});
test('list, paginated list and detail share creator names and preserve pagination', () => {
  const row = { createdById: 'agent' };
  assert.equal(resolveBroadcastCreators({ success: true, data: [row] }, user).data[0].createdByName, 'Priya');
  assert.equal(resolveBroadcastCreators({ success: true, data: row }, user).data.createdByName, 'Priya');
  const page = resolveBroadcastCreators({ success: true, data: { items: [row], meta: { nextCursor: 'next' } } }, user);
  assert.equal(page.data.items[0].createdByName, 'Priya');
  assert.equal(page.data.meta.nextCursor, 'next');
});
