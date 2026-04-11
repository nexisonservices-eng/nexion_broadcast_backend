const test = require('node:test');
const assert = require('node:assert/strict');

const axiosPath = require.resolve('axios');
const contactModelPath = require.resolve('../models/Contact');
const metaAuthServicePath = require.resolve('../services/metaAuthService');
const metaLeadConsentServicePath = require.resolve('../services/metaLeadConsentService');

const originalCacheEntries = new Map(
  [axiosPath, contactModelPath, metaAuthServicePath, metaLeadConsentServicePath].map((path) => [
    path,
    require.cache[path]
  ])
);

const mockState = {
  axiosGetResult: null,
  contactFindOneResult: null,
  savedContacts: []
};

const resetState = () => {
  mockState.axiosGetResult = null;
  mockState.contactFindOneResult = null;
  mockState.savedContacts = [];
};

function MockContact(payload = {}) {
  Object.assign(this, payload);
}

MockContact.findOne = async () => {
  return mockState.contactFindOneResult ? new MockContact(mockState.contactFindOneResult) : null;
};

MockContact.prototype.save = async function save() {
  if (!this._id) {
    this._id = `contact-${mockState.savedContacts.length + 1}`;
  }
  mockState.savedContacts.push({ ...this });
  return this;
};

require.cache[axiosPath] = {
  id: axiosPath,
  filename: axiosPath,
  loaded: true,
  exports: {
    get: async () => ({ data: mockState.axiosGetResult })
  }
};

require.cache[contactModelPath] = {
  id: contactModelPath,
  filename: contactModelPath,
  loaded: true,
  exports: MockContact
};

require.cache[metaAuthServicePath] = {
  id: metaAuthServicePath,
  filename: metaAuthServicePath,
  loaded: true,
  exports: {
    GRAPH_BASE_URL: 'https://graph.facebook.com',
    getAccessContextForUser: async () => ({
      accessToken: 'meta-test-token',
      apiVersion: 'v22.0'
    })
  }
};

delete require.cache[metaLeadConsentServicePath];
const {
  buildResolvedLeadPayload,
  syncMetaLeadConsent
} = require('../services/metaLeadConsentService');

test.after(() => {
  for (const [path, entry] of originalCacheEntries.entries()) {
    if (entry) {
      require.cache[path] = entry;
    } else {
      delete require.cache[path];
    }
  }
});

test.beforeEach(() => {
  resetState();
});

test('buildResolvedLeadPayload resolves consent using mapping keys', () => {
  const resolved = buildResolvedLeadPayload(
    {
      field_data: [
        { name: 'Phone Number', values: ['+91 98765 43210'] },
        { name: 'Full Name', values: ['Meta Lead'] },
        { name: 'WhatsApp Consent', values: ['YES'] }
      ]
    },
    {
      phoneFieldKeys: ['phone number'],
      nameFieldKeys: ['full name'],
      consentFieldKeys: ['whatsapp consent'],
      consentApprovedValues: ['yes']
    }
  );

  assert.equal(resolved.phone, '919876543210');
  assert.equal(resolved.name, 'Meta Lead');
  assert.equal(resolved.consentApproved, true);
  assert.equal(resolved.consentRawValue, 'YES');
});

test('syncMetaLeadConsent creates a proof-backed opted-in contact when mapping approves consent', async () => {
  mockState.axiosGetResult = {
    id: 'lead-123',
    form_id: 'form-123',
    ad_id: 'ad-123',
    campaign_id: 'campaign-123',
    created_time: '2026-04-11T10:00:00.000Z',
    field_data: [
      { name: 'Phone Number', values: ['+91 98765 43210'] },
      { name: 'Full Name', values: ['Meta Consent Lead'] },
      { name: 'WhatsApp Consent', values: ['yes'] }
    ]
  };

  const result = await syncMetaLeadConsent({
    userId: 'user-1',
    companyId: 'company-1',
    leadId: 'lead-123',
    mapping: {
      phoneFieldKeys: ['phone number'],
      nameFieldKeys: ['full name'],
      consentFieldKeys: ['whatsapp consent'],
      consentApprovedValues: ['yes'],
      consentText: 'Meta lead form consent for WhatsApp marketing updates.',
      scope: 'marketing'
    }
  });

  assert.equal(result.contact.phone, '919876543210');
  assert.equal(result.contact.whatsappOptInStatus, 'opted_in');
  assert.equal(result.contact.whatsappOptInProofType, 'meta_lead_ads');
  assert.equal(result.contact.whatsappOptInProofId, 'lead-123');
  assert.equal(mockState.savedContacts.length, 1);
});

test('syncMetaLeadConsent rejects leads without an approved consent answer', async () => {
  mockState.axiosGetResult = {
    id: 'lead-456',
    field_data: [
      { name: 'Phone Number', values: ['+91 99999 00000'] },
      { name: 'WhatsApp Consent', values: ['no'] }
    ]
  };

  await assert.rejects(
    () =>
      syncMetaLeadConsent({
        userId: 'user-1',
        leadId: 'lead-456',
        mapping: {
          phoneFieldKeys: ['phone number'],
          consentFieldKeys: ['whatsapp consent'],
          consentApprovedValues: ['yes']
        }
      }),
    /does not have a valid WhatsApp consent answer/i
  );
});
