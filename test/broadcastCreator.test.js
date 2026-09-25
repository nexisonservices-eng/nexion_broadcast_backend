const { test } = require('node:test');
const assert = require('node:assert/strict');
const { resolveBroadcastCreator, resolveBroadcastCreators } = require('../utils/broadcastCreator');
const { preserveBroadcastCreator } = require('../utils/broadcastCreator');
const Broadcast = require('../models/Broadcast');

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

test('creator name, email and role survive storage and an admin editing the draft', () => {
  const agent = new Broadcast({
    createdById: '507f1f77bcf86cd799439011', createdBy: 'Priya',
    createdByName: 'Priya', createdByEmail: 'priya@example.com', createdByWorkspaceRole: 'agent'
  }).toObject();
  assert.equal(agent.createdByName, 'Priya');
  assert.equal(agent.createdByEmail, 'priya@example.com');
  const edited = new Broadcast({ ...agent, ...preserveBroadcastCreator(agent, {
    name: 'Edited campaign', createdById: '507f1f77bcf86cd799439012',
    createdBy: 'Admin', createdByName: 'Admin', createdByEmail: 'admin@example.com',
    createdByWorkspaceRole: 'admin'
  }) }).toObject();
  assert.equal(edited.name, 'Edited campaign');
  assert.equal(String(edited.createdById), String(agent.createdById));
  assert.equal(edited.createdByName, 'Priya');
  assert.equal(edited.createdByEmail, 'priya@example.com');
  assert.equal(edited.createdByWorkspaceRole, 'agent');
});

test('create uses the logged-in agent even with admin-owned WhatsApp credentials and creator fields in the body', async () => {
  const fs = require('node:fs');
  const vm = require('node:vm');
  let saved;
  const sandbox = { module: { exports: {} }, require: (path) => {
    if (path === '../services/broadcastService') return {
      createBroadcast: async (payload) => { saved = payload; return { success: true, data: payload }; }
    };
    return {};
  } };
  vm.runInNewContext(fs.readFileSync(require.resolve('../controllers/broadcastController'), 'utf8'), sandbox);
  let status;
  await sandbox.module.exports.createBroadcast({
    user: { id: 'agent', username: 'Priya', email: 'priya@example.com', normalizedRole: 'agent' },
    companyId: 'company', headers: {},
    body: { name: 'Campaign', createdById: 'admin', createdBy: 'Company admin', createdByWorkspaceRole: 'admin' },
    whatsappCredentials: { credentialOwnerUserId: 'admin', phoneNumberId: 'shared-phone' }
  }, { status(code) { status = code; return this; }, json() {} });
  assert.equal(status, 201);
  assert.equal(saved.createdById, 'agent');
  assert.equal(saved.createdBy, 'Priya');
  assert.equal(saved.createdByWorkspaceRole, 'agent');
  assert.equal(saved.credentialsSnapshot.phoneNumberId, 'shared-phone');
});
