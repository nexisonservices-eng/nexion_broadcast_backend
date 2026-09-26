const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const runEdit = async ({ visible = true, body = { name: 'Updated campaign', status: 'paused' } } = {}) => {
    const calls = [];
    const metaId = '120251270263950267';
    const dependencies = {
        '../models/campaign': {
            findOne: async () => null,
            findById: () => { throw new Error('Meta IDs must not use MongoDB findById'); }
        },
        'express-validator': { validationResult: () => ({ isEmpty: () => true }) },
        '../utils/accessControl': { buildTenantResourceFilter: () => ({ companyId: 'tenant' }) },
        '../services/metaAdsService': {
            fetchRemoteCampaigns: async ({ userId }) => {
                assert.equal(userId, 'user');
                return visible ? [{ metaCampaignId: metaId, name: 'Original', status: 'active' }] : [];
            },
            updateCampaign: async (payload) => { calls.push(payload); return { success: true }; }
        }
    };
    const context = {
        exports: {}, console,
        require: (name) => dependencies[name] || {}
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../controllers/campaigncontroller.js'), 'utf8'), context);
    const response = { status(code) { this.code = code; return this; }, json(data) { this.data = data; return this; } };
    await context.exports.updateCampaign({
        params: { id: `meta_${metaId}` }, user: { id: 'user' }, companyId: 'tenant',
        body: { audience: {}, deliveryPolicy: {}, retryPolicy: {}, compliancePolicy: {}, analytics: {}, ...body }
    }, response);
    return { response, calls };
};

test('editing an imported Meta campaign updates its name and status', async () => {
    const { response, calls } = await runEdit();
    assert.equal(response.code, 200);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].campaignId, '120251270263950267');
    assert.equal(calls[0].status, 'PAUSED');
    assert.equal(response.data.data.name, 'Updated campaign');
});

test('campaigns outside the connected account cannot be edited', async () => {
    const { response, calls } = await runEdit({ visible: false });
    assert.equal(response.code, 404);
    assert.equal(calls.length, 0);
});

test('imported campaign creative changes are rejected explicitly', async () => {
    const { response, calls } = await runEdit({ body: { name: 'Changed', primaryText: 'New creative' } });
    assert.equal(response.code, 400);
    assert.equal(calls.length, 0);
});

test('campaign list excludes Meta copies linked to local records on other pages', async () => {
    class Features {
        constructor() { this.query = []; this.filterConditions = {}; }
        filter() { return this; }
        sort() { return this; }
        limitFields() { return this; }
        paginate() { return this; }
        search() { return this; }
    }
    const dependencies = {
        '../models/campaign': {
            find: () => ({ select() { return this; }, lean: async () => [{ metaCampaignId: '123' }] }),
            countDocuments: async () => 1
        },
        '../utils/apifeature': Features,
        '../utils/accessControl': {
            normalizeRole: (role) => role,
            buildTenantResourceFilter: () => ({ companyId: 'tenant' })
        },
        '../services/metaAdsService': {
            fetchRemoteCampaigns: async () => [
                { metaCampaignId: '123', name: 'Same name' },
                { metaCampaignId: '456', name: 'Same name' }
            ]
        }
    };
    const context = { exports: {}, console, require: (name) => dependencies[name] || {} };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../controllers/campaigncontroller.js'), 'utf8'), context);
    const response = { status(code) { this.code = code; return this; }, json(data) { this.data = data; return this; } };
    await context.exports.getCampaigns({ user: { id: 'user', role: 'admin' }, query: {}, companyId: 'tenant' }, response);
    assert.equal(response.code, 200);
    assert.equal(response.data.data.length, 1);
    assert.equal(response.data.data[0].metaCampaignId, '456');
});
