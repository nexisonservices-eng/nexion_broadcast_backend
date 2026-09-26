const test = require('node:test');
const assert = require('node:assert/strict');
const APIFeatures = require('../utils/apifeature');

test('reporting date range does not hide saved campaign records', () => {
    const records = [{ name: 'Saved video campaign', status: 'active', videoUrl: 'https://example.com/ad.mp4' }];
    let matched;
    const query = {
        find(filter) {
            matched = records.filter(record => Object.entries(filter).every(([key, value]) => record[key] === value));
            return this;
        }
    };
    const features = new APIFeatures(query, {
        dateRange: 'last30days', status: 'active', page: '1'
    }).filter();
    assert.deepEqual(features.filterConditions, { status: 'active' });
    assert.deepEqual(matched, records);
});
