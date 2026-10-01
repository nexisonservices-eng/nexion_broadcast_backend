const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const load = (file, dependencies) => {
  const context = {
    module: { exports: {} },
    console: { log() {}, warn() {}, error() {} },
    require: (name) => dependencies[name] || {}
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'), context);
  return context.module.exports;
};

const service = (request) => load('services/metaAdsService.js', {
  axios: request,
  '../config/metaAdsConfig': { getMetaAdsConfig: () => ({ apiVersion: 'v23.0' }) },
  './metaAuthService': {
    GRAPH_BASE_URL: 'https://graph.facebook.com',
    decryptMetaToken: (value) => value,
    getAccessContextForUser: async () => ({
      apiVersion: 'v24.0',
      connection: { selectedPageId: 'page', selectedPageAccessToken: 'test-page-token' }
    })
  }
});

test('form discovery preserves Meta permission errors', async () => {
  const error = Object.assign(new Error('Request failed'), {
    response: { status: 403, data: { error: { code: 200, message: 'Missing leads permission' } } }
  });
  await assert.rejects(service(async () => { throw error; }).getPageLeads({ userId: 'user' }),
    (actual) => actual === error);
});

test('a configured form loads without requiring form discovery', async () => {
  const calls = [];
  const result = await service(async (request) => {
    calls.push(request);
    assert.equal(request.url, 'https://graph.facebook.com/v24.0/form/leads');
    return { data: { data: [{ id: 'lead' }] } };
  }).getPageLeads({ userId: 'user', formId: 'form' });
  assert.equal(calls.length, 1);
  assert.equal(result.leads[0].id, 'lead');
});

test('an invalid configured form falls back to discovered forms', async () => {
  const result = await service(async ({ url }) => {
    if (url.endsWith('/old/leads')) throw Object.assign(new Error('Invalid form'), {
      response: { data: { error: { code: 100, error_subcode: 33 } } }
    });
    if (url.endsWith('/page/leadgen_forms')) return { data: { data: [{ id: 'new' }] } };
    assert.ok(url.endsWith('/new/leads'));
    return { data: { data: [{ id: 'lead' }] } };
  }).getPageLeads({ userId: 'user', formId: 'old' });
  assert.equal(result.resolvedFormId, 'new');
});

test('the controller formats Graph lead fields and preserves campaign attribution', async () => {
  const model = { find: () => ({ select() { return this; }, lean: async () => [{
    metaCampaignId: 'campaign', name: 'Lead campaign'
  }] }) };
  const controller = load('controllers/metaLeadController.js', {
    '../models/campaign': model,
    '../models/MetaAdCampaign': model,
    '../services/userMetaCredentialsService': { getMetaConfigByUserId: async () => ({}) },
    '../services/metaAdsService': { getPageLeads: async () => ({ leads: [{
      id: 'lead', ad_id: 'ad', campaign_id: 'campaign', created_time: '2026-10-01T10:00:00Z',
      field_data: [
        { name: 'full_name', values: ['Test Person'] },
        { name: 'email', values: ['test@example.com'] },
        { name: 'phone_number', values: ['123456789'] }
      ]
    }] }) }
  });
  const res = { status(code) { this.code = code; return this; }, json(data) { this.data = data; } };
  await controller.getMetaLeads({ query: { userId: 'user' } }, res);
  assert.equal(res.data.success, true);
  const lead = res.data.leads[0];
  assert.equal(lead.fullName, 'Test Person');
  assert.equal(lead.email, 'test@example.com');
  assert.equal(lead.phoneNumber, '123456789');
  assert.equal(lead.adId, 'ad');
  assert.equal(lead.campaignId, 'campaign');
  assert.equal(res.data.campaigns[0].campaignName, 'Lead campaign');
});
