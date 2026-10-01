const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const context = {
  module: { exports: {} },
  console,
  require: (name) => name === '../config/metaAdsConfig' ? {
    getMetaAdsConfig: () => ({ apiVersion: 'v23.0' }),
    CANONICAL_META_OAUTH_REDIRECT_URI: 'https://example.com/api/meta-ads/oauth/callback'
  } : {}
};
vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../services/metaAuthService.js'), 'utf8'), context);

test('Meta login requests Page discovery and lead retrieval permissions', () => {
  const url = new URL(context.module.exports.getLoginDialogUrl({ appId: 'app', state: 'signed-state' }));
  const scopes = url.searchParams.get('scope').split(',');
  for (const permission of [
    'pages_show_list', 'pages_read_engagement', 'pages_manage_ads', 'leads_retrieval',
    'public_profile', 'business_management', 'ads_management', 'ads_read'
  ]) {
    assert.ok(scopes.includes(permission), `Missing permission: ${permission}`);
  }
  assert.equal(url.searchParams.get('auth_type'), 'rerequest');
});

test('permission updates preserve OAuth app, redirect, version, and state', () => {
  const url = new URL(context.module.exports.getLoginDialogUrl({
    appId: 'app-123', apiVersion: 'v24.0', state: 'signed+state&value=1'
  }));
  assert.equal(url.pathname, '/v24.0/dialog/oauth');
  assert.equal(url.searchParams.get('client_id'), 'app-123');
  assert.equal(url.searchParams.get('redirect_uri'), 'https://example.com/api/meta-ads/oauth/callback');
  assert.equal(url.searchParams.get('response_type'), 'code');
  assert.equal(url.searchParams.get('state'), 'signed+state&value=1');
});
