const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const runArchive = async ({ linked = true, viaMetaId = false, allowed = true, saveFails = false, endDate } = {}) => {
    let saves = 0;
    const metaCalls = [];
    const logs = [];
    const campaign = {
        _id: 'local-id', companyId: 'tenant', createdBy: 'user',
        status: 'active', lifecycleStatus: 'active', localStatus: 'active',
        metaStatus: 'ACTIVE',
        endDate,
        ...(linked ? { metaCampaignId: '123', metaAdSetId: '456', metaAdId: '789' } : {}),
        async save() {
            saves += 1;
            if (saveFails) throw new Error('Save failed');
        }
    };
    const dependencies = {
        '../models/campaign': {
            findById: async () => campaign,
            findOne: async () => campaign
        },
        '../services/metaAdsService': new Proxy({}, {
            get: (_target, method) => async (args) => { metaCalls.push({ method, args }); }
        }),
        '../utils/campaignContract': { shapeCampaignContract: () => ({}) },
        '../utils/accessControl': {
            normalizeRole: (role) => role,
            canAccessOwnedResource: () => allowed
        },
        '../utils/authAuditLogger': { emitAuthAuditLog() {} }
    };
    const context = {
        exports: {}, process: { env: { NODE_ENV: 'test' } },
        console: { info(...args) { logs.push(args); }, error() {} },
        require: (name) => dependencies[name] || {}
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../controllers/campaigncontroller.js'), 'utf8'), context);
    const response = {
        headers: {},
        setHeader(name, value) { this.headers[name] = value; },
        status(code) { this.code = code; return this; },
        json(data) { this.data = data; return this; }
    };
    await context.exports.deleteCampaign({
        params: { id: viaMetaId ? 'meta_123' : 'local-id' },
        body: linked ? { metaCampaignId: '123' } : {},
        user: { id: 'user', role: 'admin' }, companyId: 'tenant'
    }, response);
    return { response, saves, metaCalls, campaign, logs };
};

for (const options of [{ linked: true }, { linked: false }, { viaMetaId: true }]) {
    test(`local archive leaves Meta untouched: ${JSON.stringify(options)}`, async () => {
        const { response, saves, metaCalls, campaign } = await runArchive(options);
        assert.equal(response.code, 200);
        assert.equal(saves, 1);
        assert.equal(campaign.status, 'archived');
        assert.equal(campaign.lifecycleStatus, 'archived');
        assert.equal(campaign.localStatus, 'archived');
        assert.equal(campaign.deliveryStatus, 'completed');
        assert.equal(campaign.metaStatus, 'ACTIVE');
        assert.equal(metaCalls.length, 0);
        assert.equal(response.headers['X-Campaign-Delete-Phase'], 'archived');
        assert.equal(response.data.message, 'Campaign archived successfully');
    });
}

test('unauthorized archive neither saves nor contacts Meta', async () => {
    const { response, saves, metaCalls } = await runArchive({ allowed: false });
    assert.equal(response.code, 403);
    assert.equal(saves, 0);
    assert.equal(metaCalls.length, 0);
});

test('failed local save does not contact Meta or report success', async () => {
    const { response, metaCalls, logs } = await runArchive({ saveFails: true });
    assert.equal(response.code, 500);
    assert.equal(metaCalls.length, 0);
    assert.equal(logs.some(([, entry]) => entry.event === 'local_campaign_archived'), false);
});

test('archiving an expired campaign preserves its date and logs no Meta action', async () => {
    const endDate = new Date('2020-09-29T00:00:00.000Z');
    const { response, campaign, metaCalls, logs } = await runArchive({ endDate });
    assert.equal(response.code, 200);
    assert.equal(campaign.endDate, endDate);
    assert.equal(metaCalls.length, 0);
    const entry = logs.find(([, entry]) => entry.event === 'local_campaign_archived')[1];
    assert.equal(entry.metaAction, 'none');
    assert.equal(entry.userId, 'user');
    assert.equal(entry.localCampaignId, 'local-id');
    assert.equal(entry.metaCampaignId, '123');
});
