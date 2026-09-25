const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildAgentCreatorFilter } = require('../utils/broadcastCreatorFilter');

test('agent filter includes stored roles and verified agent IDs but excludes admin IDs', () => {
  const scope = buildAgentCreatorFilter({ id: 'admin', role: 'admin', workspaceCreators: [
    { id: 'admin', role: 'admin' }, { id: 'agent-one', role: 'user' }, { id: 'agent-two', role: 'agent' }
  ] });
  assert.deepEqual(scope.$or[0], { createdByWorkspaceRole: { $in: ['agent', 'user'] } });
  assert.deepEqual(scope.$or[1], { createdById: { $in: ['agent-one', 'agent-two'] } });
});

test('an agent can filter their own legacy broadcasts without a directory', () => {
  assert.deepEqual(buildAgentCreatorFilter({ id: 'agent', companyRole: 'user' }).$or[1], { createdById: { $in: ['agent'] } });
  assert.deepEqual(buildAgentCreatorFilter({ id: 'admin', role: 'admin' }).$or[1], { createdById: { $in: [] } });
});
