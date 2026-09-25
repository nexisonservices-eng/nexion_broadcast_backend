const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildTenantResourceFilter } = require('../utils/accessControl');

test('admin template reads cover the company, agent reads retain ownership', () => {
  const req = { companyId: 'company', user: { id: 'admin', companyRole: 'admin' } };
  assert.deepEqual(buildTenantResourceFilter({ req, ownerField: 'userId' }), { companyId: 'company' });
  req.user = { id: 'agent', companyRole: 'user' };
  assert.deepEqual(buildTenantResourceFilter({ req, ownerField: 'userId' }), { companyId: 'company', userId: 'agent' });
});

test('legacy admin without company only reads verified members', () => {
  const req = { user: { id: 'admin', companyRole: 'admin', workspaceReadUserIds: ['agent'] } };
  const filter = buildTenantResourceFilter({ req });
  assert.deepEqual(filter.createdBy.$in, ['admin', 'agent']);
  assert.ok(!filter.createdBy.$in.includes('other-admin'));
});
