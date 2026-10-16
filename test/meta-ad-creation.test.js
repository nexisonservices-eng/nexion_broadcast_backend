const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createAdCreation, extractAdId } = require('../services/metaAdCreation');

const source = fs.readFileSync(path.join(__dirname, '../services/metaAdsService.js'), 'utf8');
const stageError = (stage, message, details, status = 400) => Object.assign(new Error(message), { stage, details, status });
const buildStageError = (stage, error) => stageError(stage, error.message, error.response?.data, error.response?.status);
const buildAdAccountPath = (account, edge) => `${account}/${edge}`;
const partialData = { metaCampaignId: 'campaign', metaAdSetId: 'adset', metaCreativeId: 'creative' };
const args = { partialData, effectiveAdAccountId: 'act_1', resolvedAccessToken: 'private-token', payload: { name: 'private-name' } };
const correctAd = { id: '123456789', adset_id: 'adset', creative: { id: 'creative' } };
const quiet = { info() {}, warn() {}, error() {}, log() {} };
const creation = (graphRequest, logger = quiet) => createAdCreation({ graphRequest, logger,
  wait: async () => {}, buildAdAccountPath, buildStageError, buildStageErrorWithDetails: stageError });

// Exercise the actual Graph boundary, including its cache and promise sharing.
const graph = (http, { stale, blocked = false } = {}) => {
  const cached = new Map();
  const logs = [];
  const context = {
    getEnvConfig: () => ({ apiVersion: 'test' }),
    buildMetaGraphRequestKey: ({ method, path, params }) => JSON.stringify({ method, path, params }),
    getMetaTokenKey: () => 'private-token',
    getCachedMetaGraphResponse: (key) => cached.get(key) || stale || null,
    setCachedMetaGraphResponse: (key, response) => cached.set(key, response),
    getMetaRateLimitState: () => blocked ? { blockedUntil: new Date(Date.now() + 60000) } : null,
    clearMetaRateLimitState() {}, setMetaRateLimitState: () => ({}), logMetaGraphEvent: (...values) => logs.push(values),
    metaGraphRequestInflight: new Map(), metaGraphRequestStats: {}, metaGraphRequestSequence: 0,
    GRAPH_BASE_URL: 'https://example.invalid', enqueueMetaGraphRequest: (fn) => Promise.resolve().then(fn),
    cloneMetaResponse: (value) => structuredClone(value), cloneMetaValue: (value) => structuredClone(value),
    isMetaRateLimitError: (error) => error?.response?.data?.error?.code === 17,
    normalizeMetaRateLimitError: (error) => ({ code: error?.response?.data?.error?.code }),
    buildStageErrorWithDetails: stageError,
    console: { info: (...values) => logs.push(values), error: (...values) => logs.push(values) }, axios: http
  };
  vm.runInNewContext(source.slice(source.indexOf('const graphRequest = async'),
    source.indexOf('const normalizePreviewPlacements =')) + '\nthis.request = graphRequest;', context);
  return { request: context.request, logs };
};

for (const [label, response] of [
  ['body', { id: '123456789' }], ['HTTP wrapper', { status: 200, data: { id: '123456789' } }],
  ['legacy result wrapper', { result: { id: '123456789' } }],
  ['nested data wrapper', { data: { data: { id: '123456789' } } }],
  ['legacy ad_id', { ad_id: '123456789' }]
]) test(`extracts a confirmed ID from ${label}`, () => assert.equal(extractAdId(response), '123456789'));

test('rejects empty and non-string IDs', () => {
  for (const id of ['', '   ', 123, {}, [], null]) assert.equal(extractAdId({ id }), '');
});

test('successful POST confirms the ID without lookup and persists pending state first', async () => {
  let saved = false;
  const service = creation(async (request) => {
    assert.equal(saved, true);
    assert.equal(request.method, 'POST');
    assert.equal(request.returnResponse, true);
    return { status: 200, data: { id: '123456789' } };
  });
  assert.equal(await service.create({ ...args, onPending: async (pending) => {
    assert.equal(pending.metaCampaignId, 'campaign');
    assert.equal(pending.reconciliationRequired, true);
    saved = true;
  } }), '123456789');
});

test('a failed pending-state save prevents the POST', async () => {
  let calls = 0;
  await assert.rejects(creation(async () => { calls++; }).create({ ...args,
    onPending: async () => { throw new Error('Mongo unavailable'); } }), /Mongo unavailable/);
  assert.equal(calls, 0);
});

test('concurrent body and HTTP-wrapper callers retain their own response contract', async () => {
  for (const firstWrapped of [false, true]) {
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    let calls = 0;
    const { request } = graph(async () => { calls++; await gate; return { status: 200, data: { id: '123456789' } }; });
    const base = { method: 'POST', path: 'act_1/ads', accessToken: 'private-token' };
    const first = request({ ...base, returnResponse: firstWrapped });
    const second = request({ ...base, returnResponse: !firstWrapped });
    release();
    const responses = await Promise.all([first, second]);
    assert.equal(calls, 1);
    assert.equal(firstWrapped ? responses[0].data.id : responses[0].id, '123456789');
    assert.equal(firstWrapped ? responses[1].id : responses[1].data.id, '123456789');
  }
});

test('fallback bypasses a stale GET cache and any pre-existing in-flight read', async () => {
  let calls = 0;
  const { request } = graph(async () => { calls++; return { status: 200, data: { data: [correctAd] } }; },
    { stale: { status: 200, data: { data: [] } } });
  assert.equal((await request({ path: 'adset/ads', accessToken: 'private-token' })).data.length, 0);
  assert.equal(await creation(request).create({ ...args, reconciliationOnly: true }), '123456789');
  assert.equal(calls, 1);

  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  calls = 0;
  const fresh = graph(async () => {
    const index = ++calls;
    if (index === 1) await gate;
    return { status: 200, data: { data: index === 1 ? [] : [correctAd] } };
  }).request;
  const oldRead = fresh({ path: 'adset/ads', params: { fields: 'id,adset_id,creative{id}', limit: 100 }, accessToken: 'private-token' });
  await Promise.resolve();
  assert.equal(await creation(fresh).create({ ...args, reconciliationOnly: true }), '123456789');
  release();
  await oldRead;
  assert.equal(calls, 2);
});

test('fallback follows pagination using a fresh cursor on the original endpoint', async () => {
  const requests = [];
  const service = creation(async (request) => {
    requests.push(request);
    if (request.method === 'POST') return { status: 200, data: {} };
    assert.equal(request.bypassCache, true);
    if (!request.params.after) return { data: { data: [
      { ...correctAd, id: 'wrong-creative', creative: { id: 'other' } },
      { ...correctAd, id: 'wrong-adset', adset_id: 'other' }
    ], paging: { next: 'https://example.invalid/?access_token=secret', cursors: { after: 'page2' } } } };
    return { data: { data: [correctAd] } };
  });
  assert.equal(await service.create(args), '123456789');
  assert.equal(requests.length, 3);
  assert.equal(requests[2].path, 'adset/ads');
  assert.equal(requests[2].params.after, 'page2');
});

test('account lookup runs when ad-set results contain only unrelated ads', async () => {
  const service = creation(async ({ path }) => ({ data: { data: path === 'act_1/ads' ? [correctAd] : [
    { id: 'wrong', name: 'private-name', adset_id: 'adset', creative: { id: 'other' } }
  ] } }));
  assert.equal(await service.create({ ...args, reconciliationOnly: true }), '123456789');
});

test('a confirmed Graph rejection fails without fallback or a second POST', async () => {
  let calls = 0;
  const service = creation(async () => {
    calls++;
    throw Object.assign(new Error('Invalid parameter'), { response: { status: 400, data: { error: { code: 100 } } } });
  });
  await assert.rejects(service.create(args), (error) => {
    assert.equal(error.stage, 'Ad creation');
    assert.equal(error.details.error.code, 100);
    assert.equal(error.reconciliationRequired, undefined);
    assert.equal(error.partialData.metaCreativeId, 'creative');
    return true;
  });
  assert.equal(calls, 1);
});

test('a timeout after possible creation reconciles successfully without retrying POST', async () => {
  let posts = 0;
  const service = creation(async ({ method }) => {
    if (method === 'POST') { posts++; throw Object.assign(new Error('timeout'), { code: 'ECONNABORTED' }); }
    return { data: { data: [correctAd] } };
  });
  assert.equal(await service.create(args), '123456789');
  assert.equal(posts, 1);
});

test('unresolved creation preserves IDs; a later retry is read-only and recovers the same ad', async () => {
  let posts = 0;
  let visible = false;
  const service = creation(async ({ method }) => {
    if (method === 'POST') { posts++; return { status: 200, data: {} }; }
    return { data: { data: visible ? [correctAd] : [] } };
  });
  let saved;
  await assert.rejects(service.create(args), (error) => {
    assert.equal(error.code, 'META_AD_RECONCILIATION_REQUIRED');
    assert.equal(error.status, 409);
    assert.equal(error.partialData.metaCampaignId, 'campaign');
    saved = error.partialData;
    return true;
  });
  await assert.rejects(service.create({ ...args, partialData: saved, reconciliationOnly: true }), /Reconciliation/);
  visible = true;
  assert.equal(await service.create({ ...args, partialData: saved, reconciliationOnly: true }), '123456789');
  assert.equal(posts, 1);
});

test('multiple exact matches and merely similar names never confirm an ID', async () => {
  for (const ads of [[correctAd, { ...correctAd, id: 'duplicate' }], [
    { id: 'wrong', name: 'private-name', adset_id: 'adset', creative: { id: 'other' } }
  ]]) {
    await assert.rejects(creation(async () => ({ data: { data: ads } })).create({ ...args, reconciliationOnly: true }), /Reconciliation/);
  }
});

test('rate-limited fresh lookups fail closed instead of returning cached ads', async () => {
  const { request } = graph(async () => { throw Error('must not call HTTP'); },
    { blocked: true, stale: { data: { data: [correctAd] } } });
  await assert.rejects(creation(request).create({ ...args, reconciliationOnly: true }), /Reconciliation/);
});

test('diagnostics never include response data, credentials, or request payloads', async () => {
  const logs = [];
  const logger = { info: (...values) => logs.push(values), warn: (...values) => logs.push(values) };
  const service = creation(async () => ({ status: 200, config: { access_token: 'private-token' },
    data: { id: '123456789', private: 'sensitive-response' } }), logger);
  await service.create(args);
  const output = JSON.stringify(logs);
  for (const secret of ['private-token', 'private-name', 'sensitive-response']) assert.equal(output.includes(secret), false);
  assert.equal(logs[0][1].httpStatus, 200);
  assert.equal(logs[0][1].endpoint, 'act_1/ads');
});

test('existing full-stack mock mode returns IDs without any Graph request', async () => {
  const context = { getEnvConfig: () => ({}), shouldUseMockMode: () => true,
    ensureConnectedMetaUser: async () => ({ connection: {} }) };
  vm.runInNewContext(source.slice(source.indexOf('const createFullAdStack ='),
    source.indexOf('const fetchCampaignInsights =')) + '\nthis.create = createMetaAdStack;', context);
  const result = await context.create({ campaign: { status: 'PAUSED' }, userId: 'user' });
  assert.equal(result.apiMode, 'mock');
  assert.match(result.adId, /^mock-ad-/);
  assert.match(result.campaignId, /^mock-campaign-/);
});

test('CRUD creation uses the resolver and resumes saved IDs without recreating the stack', async () => {
  const requests = [];
  let visible = false;
  const graphRequest = async (request) => {
    requests.push(request);
    if (request.method === 'POST') return { status: 200, data: {
      id: request.path.endsWith('/campaigns') ? 'campaign' : 'adset'
    } };
    return { status: 200, data: { data: visible ? [correctAd] : [] } };
  };
  // Only the /ads POST lacks an ID; the real CRUD function must reconcile it.
  const request = async (options) => options.method === 'POST' && options.path.endsWith('/ads')
    ? (requests.push(options), { status: 200, data: {} }) : graphRequest(options);
  const context = { console: quiet, getEnvConfig: () => ({}),
    ensureConnectedMetaUser: async () => ({ accessToken: 'private-token', connection: { selectedAdAccountId: 'act_1', selectedPageId: 'page' } }),
    toCanonicalAdAccountId: (value) => value, decryptMetaToken: (value) => value,
    normalizeOptimizationGoalForCrudObjective: () => 'REACH', parseDelimitedTerms: () => [],
    uploadCreativeAsset: async () => ({ mediaHash: 'hash' }), mapCrudObjectiveToMetaObjective: () => 'OUTCOME_AWARENESS',
    parseTargetingCountriesFromCrud: () => ['IN'], buildAdAccountPath, graphRequest: request,
    metaCreativeService: { createCreative: async () => ({ id: 'creative' }) },
    buildStageError, buildStageErrorWithDetails: stageError, extractApiErrorMessage: (error) => error.message,
    invalidateMetaCampaignCache: async () => {}, adCreation: creation(request) };
  vm.runInNewContext(source.slice(source.indexOf('const createMetaAdStackFromCrud ='),
    source.indexOf('const updateMetaCrudDeliveryStatus =')) + '\nthis.create = createMetaAdStackFromCrud;', context);
  let pending;
  await assert.rejects(context.create({ userId: 'user', campaignName: 'Test', objective: 'awareness',
    onAdCreationPending: async (value) => { pending = value; } }), /Reconciliation/);
  assert.equal(pending.metaCampaignId, 'campaign');
  visible = true;
  const before = requests.length;
  const result = await context.create({ userId: 'user', resumePartialData: pending });
  assert.equal(result.adId, '123456789');
  assert.equal(result.campaignId, 'campaign');
  assert.equal(requests.slice(before).some((request) => request.method === 'POST'), false);
  assert.equal(requests.filter((request) => request.method === 'POST' && request.path.endsWith('/ads')).length, 1);
});

const controllerHarness = (initial = {}) => {
  const campaign = { _id: 'local', companyId: 'tenant', createdBy: 'user', name: 'Test',
    lifecycleStatus: 'draft', ...initial, save: async () => campaign };
  const calls = [];
  let visible = false;
  const adCreation = creation(async (request) => {
    calls.push(request);
    return { status: 200, data: { data: visible ? [correctAd] : [] } };
  });
  const dependencies = {
    '../models/campaign': {
      findById: async () => campaign,
      findByIdAndUpdate: async (id, update) => { Object.assign(campaign, update.$set); return campaign; },
      findOneAndUpdate: async () => {
        if (campaign.lifecycleStatus === 'publishing') return null;
        campaign.lifecycleStatus = 'publishing';
        return campaign;
      }
    },
    '../utils/accessControl': { normalizeRole: (role) => role, canAccessOwnedResource: () => true },
    '../services/metaAdsService': {
      getSetupBundle: async () => ({ pageId: 'page', pageAccessReady: true }),
      createMetaAdStackFromCrud: async (options) => {
        const pending = options.resumePartialData?.metaCampaignId ? options.resumePartialData : partialData;
        const adId = await adCreation.create({ ...args, partialData: pending,
          reconciliationOnly: Boolean(options.resumePartialData?.metaCampaignId), onPending: options.onAdCreationPending });
        return { campaignId: pending.metaCampaignId, adSetId: pending.metaAdSetId, creativeId: pending.metaCreativeId, adId };
      },
      resumeCampaign: async () => { calls.push({ resume: true }); return {}; }
    }
  };
  const context = { exports: {}, console: quiet, process: { env: {} }, require: (name) => dependencies[name] || {} };
  const controllerSource = fs.readFileSync(path.join(__dirname, '../controllers/campaigncontroller.js'), 'utf8');
  vm.runInNewContext(controllerSource + '\nthis.continueCreation = continueCampaignMetaCreation;', context);
  const publish = async () => {
    const response = { status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await context.exports.publishCampaign({ params: { id: 'local' }, user: { id: 'user' }, companyId: 'tenant', headers: {}, body: {} }, response);
    return response;
  };
  return { campaign, calls, publish, showAd: () => { visible = true; }, continueCreation: context.continueCreation };
};

test('publish preserves pending IDs in Mongo and reconciles on retry before resuming delivery', async () => {
  const harness = controllerHarness();
  const first = await harness.publish();
  assert.equal(first.code, 409);
  assert.equal(first.body.success, false);
  assert.equal(harness.campaign.metaCampaignId, 'campaign');
  assert.equal(harness.campaign.metaAdSetId, 'adset');
  assert.equal(harness.campaign.metaCreativeId, 'creative');
  assert.equal(harness.campaign.metaResponse.reconciliationRequired, true);
  assert.equal(harness.calls.some((request) => request.resume), false);
  harness.showAd();
  const before = harness.calls.length;
  const second = await harness.publish();
  assert.equal(second.code, 200);
  assert.equal(harness.campaign.metaAdId, '123456789');
  assert.equal(harness.calls.slice(before).some((request) => request.method === 'POST'), false);
  assert.equal(harness.calls.filter((request) => request.method === 'POST').length, 1);
  assert.equal(harness.calls.filter((request) => request.resume).length, 1);
});

test('background creation preserves IDs and reconciliation marker after an ambiguous outcome', async () => {
  const harness = controllerHarness();
  await harness.continueCreation({ campaignId: 'local', userId: 'user', normalizedPayload: { name: 'Test', status: 'paused' } });
  assert.equal(harness.campaign.metaCampaignId, 'campaign');
  assert.equal(harness.campaign.metaAdSetId, 'adset');
  assert.equal(harness.campaign.metaCreativeId, 'creative');
  assert.equal(harness.campaign.metaResponse.reconciliationRequired, true);
  assert.equal(harness.campaign.metaResponse.partialData.metaCreativeId, 'creative');
  assert.equal(harness.campaign.deliveryStatus, 'rejected');
});

test('partial older campaigns and in-progress publishes never create another stack or resume delivery', async () => {
  for (const initial of [{ metaCampaignId: 'campaign' }, { lifecycleStatus: 'publishing' }]) {
    const harness = controllerHarness(initial);
    const result = await harness.publish();
    assert.equal(result.code, 409);
    assert.equal(result.body.success, false);
    assert.equal(harness.calls.length, 0);
  }
});
