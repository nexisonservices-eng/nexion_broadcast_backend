const { test } = require('node:test');
const assert = require('node:assert/strict');
const { pathToFileURL } = require('node:url');
const path = require('node:path');

const handlers = async () => [require('../utils/agentActivity').createAgentActivityHandler];
const makeModel = () => {
  const calls = {};
  const query = {
    select(value) { calls.projection = value; return this; },
    sort() { return this; }, skip(value) { calls.skip = value; return this; }, limit(value) { calls.limit = value; return this; },
    async lean() { return [{ _id: 'record', creator: 'agent', meta: { summary: 'Team meeting' }, status: 'active' }]; }
  };
  return { calls, model: {
    schema: { path: (field) => field === 'companyId' || field === 'creator' ? { instance: 'ObjectId' } : null },
    find(filter) { calls.filter = filter; return query; },
    async countDocuments(filter) { calls.countFilter = filter; return 26; }
  } };
};
const response = () => ({ code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } });
const request = (query = {}, role = 'admin') => ({
  params: { kind: 'meetings' }, query,
  user: { id: 'admin', companyRole: role, companyId: 'company', workspaceReadUserIds: ['admin', 'agent'] }
});

test('all services scope, count and paginate agent records using trusted membership', async () => {
  for (const createHandler of await handlers()) {
    const { model, calls } = makeModel();
    const handler = createHandler({ meetings: { model, owner: 'creator', fallback: 'userId', title: ['meta.summary'], label: 'Meeting' } });
    const res = response();
    await handler(request({ page: 2, companyId: 'other-company', search: 'a.*' }), res);
    assert.equal(res.code, 200);
    assert.deepEqual(calls.filter.$and[0].$or[0], { creator: { $in: ['agent'] } });
    assert.deepEqual(calls.filter.$and[2], { companyId: 'company' });
    assert.equal(JSON.stringify(calls.filter).includes('other-company'), false);
    assert.deepEqual(calls.filter, calls.countFilter);
    assert.equal(calls.skip, 25);
    assert.equal(calls.limit, 25);
    assert.equal(res.body.pagination.pages, 2);
    assert.equal(res.body.data[0].name, 'Team meeting');
    assert.equal(res.body.data[0].createdById, 'agent');
    assert.equal(JSON.stringify(calls.filter).includes('"creator":""'), false);
  }
});

test('all services reject agents and unknown kinds and ignore outside creator IDs', async () => {
  for (const createHandler of await handlers()) {
    const { model, calls } = makeModel();
    const handler = createHandler({ meetings: { model, owner: 'creator', title: ['title'], label: 'Meeting' } });
    const forbidden = response();
    await handler(request({}, 'user'), forbidden);
    assert.equal(forbidden.code, 403);
    const unknown = response();
    await handler({ ...request(), params: { kind: 'credentials' } }, unknown);
    assert.equal(unknown.code, 404);
    const outside = response();
    await handler(request({ createdById: 'other-agent' }), outside);
    assert.deepEqual(outside.body.data, []);
    assert.equal(calls.filter, undefined);
  }
});
