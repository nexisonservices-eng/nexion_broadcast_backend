const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Exercise the real request boundary with HTTP/cache dependencies isolated.
const source = fs.readFileSync(path.join(__dirname, '../services/metaAdsService.js'), 'utf8');
const requestSource = source.slice(source.indexOf('const graphRequest = async'), source.indexOf('const normalizePreviewPlacements ='));
const runRequest = async ({ method = 'POST', status, objectPath = '123', fail = false } = {}) => {
    const logs = [];
    const requests = [];
    const context = {
        getEnvConfig: () => ({ apiVersion: 'test' }),
        buildMetaGraphRequestKey: () => 'request', getMetaTokenKey: () => 'token-key',
        getCachedMetaGraphResponse: () => null, getMetaRateLimitState: () => null,
        clearMetaRateLimitState() {}, setCachedMetaGraphResponse() {}, logMetaGraphEvent() {},
        metaGraphRequestInflight: new Map(), metaGraphRequestStats: { requestCount: 0 },
        metaGraphRequestSequence: 0, GRAPH_BASE_URL: 'https://example.invalid',
        enqueueMetaGraphRequest: (fn) => fn(),
        cloneMetaResponse: (value) => value, cloneMetaValue: (value) => value,
        isMetaRateLimitError: () => false, normalizeMetaRateLimitError: () => ({}),
        console: { info: (...args) => logs.push(args), error() {} },
        axios: async (config) => {
            requests.push(config);
            if (fail) throw new Error('HTTP failed');
            return { status: 200, data: { success: true } };
        }
    };
    vm.createContext(context);
    vm.runInContext(`${requestSource}\nthis.request = graphRequest;`, context);
    let error;
    try {
        await context.request({ method, path: objectPath, data: { status, privateField: 'private-payload' }, accessToken: 'test-only-secret' });
    } catch (caught) { error = caught; }
    return { logs, requests, error };
};

for (const [method, status, event] of [
    ['POST', 'ARCHIVED', 'meta_object_intentionally_archived'],
    ['POST', 'PAUSED', 'meta_object_intentionally_paused'],
    ['DELETE', undefined, 'meta_object_intentionally_deleted']
]) {
    test(`successful ${method} ${status || ''} has a distinct safe audit event`, async () => {
        const { logs, requests, error } = await runRequest({ method, status });
        assert.equal(error, undefined);
        assert.equal(requests.length, 1);
        assert.equal(logs.length, 1);
        assert.equal(logs[0][1].event, event);
        assert.equal(logs[0][1].objectId, '123');
        assert.equal(logs[0][1].outcome, 'api_request_succeeded');
        assert.equal(JSON.stringify(logs).includes('test-only-secret'), false);
        assert.equal(JSON.stringify(logs).includes('private-payload'), false);
    });
}

test('failed mutation produces no successful action audit', async () => {
    const { logs, error } = await runRequest({ status: 'ARCHIVED', fail: true });
    assert.equal(error.message, 'HTTP failed');
    assert.equal(logs.length, 0);
});

test('reads and creation of paused campaigns are not logged as intentional pauses', async () => {
    for (const options of [{ method: 'GET' }, { objectPath: 'act_123/campaigns', status: 'PAUSED' }]) {
        const { logs, error } = await runRequest(options);
        assert.equal(error, undefined);
        assert.equal(logs.length, 0);
    }
});
