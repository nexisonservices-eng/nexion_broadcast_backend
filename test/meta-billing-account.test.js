const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../services/metaAdsService.js'), 'utf8');
const credentialSource = fs.readFileSync(path.join(__dirname, '../services/userMetaCredentialsService.js'), 'utf8');

// Run the complete service and its real Graph cache with isolated HTTP/auth dependencies.
// Any attempt to access a database fails the test.
function billingFixture() {
  const users = {
    affected: { saved: 'act_222', configured: '111', token: 'existing-user-token' },
    other: { saved: 'act_333', configured: '', token: 'other-user-token' }
  };
  const requests = [];
  const lookups = [];
  const inaccessible = new Set();
  let funds = 25;
  let fundingEdge = false;
  let wrongValidationId = false;
  let lookupError;
  const dependencies = {
    axios: async (request) => {
      requests.push(request);
      const graphPath = new URL(request.url).pathname.split('/').slice(2).join('/');
      if (inaccessible.has(graphPath)) {
        throw Object.assign(new Error('Meta access denied'), {
          response: { status: 400, data: { error: { code: 100 } } }
        });
      }
      const id = graphPath.split('/')[0];
      let data;
      if (graphPath.endsWith('/insights')) {
        data = { data: [{ campaign_id: 'campaign-1', spend: '12', impressions: '50' }] };
      } else if (graphPath.endsWith('/funding_source_details')) {
        data = { data: [{ type: 'STORED_BALANCE', amount: funds }] };
      } else if (request.params.fields === 'id') {
        data = { id: wrongValidationId ? '999' : id };
      } else {
        data = { id, currency: 'INR', is_prepay_account: true,
          funding_source_details: fundingEdge ? undefined : { type: 'STORED_BALANCE', amount: funds } };
      }
      return { status: 200, data };
    },
    '../config/metaAdsConfig': { getMetaAdsConfig: () => ({ apiVersion: 'v23.0', forceMock: false }) },
    './metaAuthService': {
      GRAPH_BASE_URL: 'https://graph.example.invalid',
      getAccessContextForUser: async (userId) => ({
        source: 'user', accessToken: users[userId].token, apiVersion: 'v22.0',
        connection: { selectedAdAccountId: users[userId].saved }
      })
    },
    './userMetaCredentialsService': {
      getMetaConfigByUserId: async (userId, options) => {
        lookups.push({ userId, options });
        if (lookupError) throw lookupError;
        return { adAccountId: users[userId].configured };
      }
    },
    './metaCreativeService': {},
    './metaAdCreation': { createAdCreation: () => ({}) }
  };
  const context = {
    module: { exports: {} }, structuredClone, URL,
    console: { log() {}, info() {}, warn() {}, error() {} },
    require: (name) => {
      if (name.startsWith('../models/')) return new Proxy({}, {
        get() { throw new Error('Billing must not access database models'); }
      });
      assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency: ${name}`);
      return dependencies[name];
    }
  };
  vm.runInNewContext(source, context);
  return {
    service: context.module.exports, users, requests, lookups, inaccessible,
    setFunds(value) { funds = value; },
    useFundingEdge() { fundingEdge = true; },
    returnWrongValidationId() { wrongValidationId = true; },
    failLookup() { lookupError = Object.assign(new Error('Admin configuration unavailable'), { status: 503 }); }
  };
}

test('billing prefers the accessible Admin account while retaining the existing token and selection', async () => {
  const f = billingFixture();
  const result = await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  assert.equal(result.adAccount.id, 'act_111');
  assert.equal(result.billing.availableFunds, 25);
  assert.equal(f.users.affected.saved, 'act_222');
  assert.equal(f.requests.length, 2);
  assert.equal(f.requests[0].params.fields, 'id');
  for (const request of f.requests) {
    assert.equal(request.params.access_token, 'existing-user-token');
    assert.equal(new URL(request.url).pathname, '/v22.0/act_111');
  }
  assert.equal(f.lookups[0].userId, 'affected');
  assert.equal(f.lookups[0].options.throwOnError, true);
});

test('inaccessible configured account returns a clear error without querying the saved account', async () => {
  const f = billingFixture();
  f.inaccessible.add('act_111');
  await assert.rejects(f.service.getAdAccountBillingSummary({ userId: 'affected' }), (error) => {
    assert.match(error.message, /Admin-configured Meta ad account/);
    assert.equal(error.details.adAccountId, 'act_111');
    return true;
  });
  assert.equal(f.requests.length, 1);
});

test('an unexpected account returned during validation is rejected', async () => {
  const f = billingFixture();
  f.returnWrongValidationId();
  await assert.rejects(f.service.getAdAccountBillingSummary({ userId: 'affected' }), /Admin-configured/);
  assert.equal(f.requests.length, 1);
});

test('missing Admin account retains saved-account billing without extra Graph validation', async () => {
  const f = billingFixture();
  f.users.affected.configured = '';
  const result = await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  assert.equal(result.adAccount.id, 'act_222');
  assert.equal(f.requests.length, 1);
});

test('another user retains their account and token after the affected user requests billing', async () => {
  const f = billingFixture();
  await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  const result = await f.service.getAdAccountBillingSummary({ userId: 'other' });
  assert.equal(result.adAccount.id, 'act_333');
  assert.equal(f.requests.at(-1).params.access_token, 'other-user-token');
  assert.equal(f.users.other.saved, 'act_333');
});

test('configured account access is revalidated even when billing data is cached', async () => {
  const f = billingFixture();
  await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  f.inaccessible.add('act_111');
  await assert.rejects(f.service.getAdAccountBillingSummary({ userId: 'affected' }), /Admin-configured/);
});

test('billing refresh bypasses cached account and funding data using the correct account/version', async () => {
  const f = billingFixture();
  f.useFundingEdge();
  await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  f.setFunds(75);
  const cached = await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  assert.equal(cached.billing.availableFunds, 25);
  const refreshed = await f.service.getAdAccountBillingSummary({ userId: 'affected', forceRefresh: true });
  assert.equal(refreshed.adAccount.id, 'act_111');
  assert.equal(refreshed.billing.availableFunds, 75);
  assert.ok(f.requests.every(request => new URL(request.url).pathname.startsWith('/v22.0/act_111')));
});

test('changing Admin account cannot reuse another account billing cache', async () => {
  const f = billingFixture();
  await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  f.users.affected.configured = '444';
  f.setFunds(80);
  const result = await f.service.getAdAccountBillingSummary({ userId: 'affected' });
  assert.equal(result.adAccount.id, 'act_444');
  assert.equal(result.billing.availableFunds, 80);
});

test('failed Admin lookup does not silently use the saved account', async () => {
  const f = billingFixture();
  f.failLookup();
  await assert.rejects(f.service.getAdAccountBillingSummary({ userId: 'affected' }), { status: 503 });
  assert.equal(f.requests.length, 0);
});

test('campaign analytics retain campaign account mappings and do not perform the billing Admin lookup', async () => {
  const f = billingFixture();
  const result = await f.service.fetchCrudCampaignInsights({
    userId: 'affected', range: 'last7days',
    campaign: { metaCampaignId: 'campaign-1', metaResponse: { adAccountId: 'act_222' } }
  });
  assert.equal(result.spend, 12);
  assert.equal(result.impressions, 50);
  assert.equal(f.lookups.length, 0);
  assert.equal(new URL(f.requests[0].url).pathname, '/v23.0/act_222/insights');
  assert.equal(f.requests[0].params.date_preset, 'last_7d');
});

function credentialsFixture({ fail = false, data = {}, bases = ['https://admin.example.invalid'] } = {}) {
  const context = {
    module: { exports: {} },
    process: { env: { ADMIN_API_BASE_URL: bases[0], ADMIN_BACKEND_URL: bases[1] } },
    require: () => ({ get: async () => {
      if (fail) throw new Error('HTTP error containing private connection details');
      return { data: { data } };
    } })
  };
  vm.runInNewContext(credentialSource, context);
  return context.module.exports;
}

test('strict credential lookup distinguishes absent account configuration from lookup failure', async () => {
  const empty = await credentialsFixture().getMetaConfigByUserId('affected', { throwOnError: true });
  assert.equal(empty.adAccountId, '');
  for (const options of [{ fail: true }, { data: null }, { bases: [] }]) {
    const service = credentialsFixture(options);
    await assert.rejects(service.getMetaConfigByUserId('affected', { throwOnError: true }), (error) => {
      assert.equal(error.status, 503);
      assert.match(error.message, /current Admin Meta configuration/);
      assert.equal(error.message.includes('private'), false);
      return true;
    });
    assert.equal(await service.getMetaConfigByUserId('affected'), null);
  }
});

test('credential lookup still normalizes the Admin account without changing legacy callers', async () => {
  const service = credentialsFixture({ data: { metaAdAccountId: ' act_111 ' } });
  assert.equal((await service.getMetaConfigByUserId('affected', { throwOnError: true })).adAccountId, 'act_111');
  assert.equal((await service.getMetaConfigByUserId('affected')).adAccountId, 'act_111');
});
