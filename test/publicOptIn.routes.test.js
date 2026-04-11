const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

const contactModelPath = require.resolve('../models/Contact');
const conversationModelPath = require.resolve('../models/Conversation');
const publicOptInRoutePath = require.resolve('../routes/publicOptIn');

const originalCacheEntries = new Map(
  [contactModelPath, conversationModelPath, publicOptInRoutePath].map((path) => [path, require.cache[path]])
);

const mockState = {
  findOneQueue: [],
  findOneQueries: [],
  savedContacts: []
};

const resetState = () => {
  mockState.findOneQueue = [];
  mockState.findOneQueries = [];
  mockState.savedContacts = [];
};

function MockContact(payload = {}) {
  Object.assign(this, payload);
}

MockContact.findOne = async (query) => {
  mockState.findOneQueries.push(query);
  if (!mockState.findOneQueue.length) return null;
  const next = mockState.findOneQueue.shift();
  return next ? new MockContact(next) : null;
};

MockContact.prototype.save = async function save() {
  if (!this._id) {
    this._id = `contact-${mockState.savedContacts.length + 1}`;
  }
  mockState.savedContacts.push({ ...this });
  return this;
};

require.cache[contactModelPath] = {
  id: contactModelPath,
  filename: contactModelPath,
  loaded: true,
  exports: MockContact
};

require.cache[conversationModelPath] = {
  id: conversationModelPath,
  filename: conversationModelPath,
  loaded: true,
  exports: {}
};

delete require.cache[publicOptInRoutePath];
const publicOptInRouter = require('../routes/publicOptIn');

const app = express();
app.use(express.json());
app.use('/api/public', publicOptInRouter);

let server;
let baseUrl = '';

const requestJson = async (body, headers = {}) => {
  const response = await fetch(`${baseUrl}/api/public/whatsapp-opt-in`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...headers
    },
    body: JSON.stringify(body)
  });

  return {
    status: response.status,
    data: await response.json()
  };
};

test.before(async () => {
  await new Promise((resolve) => {
    server = app.listen(0, resolve);
  });
  const { port } = server.address();
  baseUrl = `http://127.0.0.1:${port}`;
});

test.after(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }

  for (const [path, entry] of originalCacheEntries.entries()) {
    if (entry) {
      require.cache[path] = entry;
    } else {
      delete require.cache[path];
    }
  }
});

test.beforeEach(() => {
  process.env.WHATSAPP_OPTIN_PUBLIC_KEY = 'public-test-key';
  resetState();
});

test.after(() => {
  delete process.env.WHATSAPP_OPTIN_PUBLIC_KEY;
});

test('POST /api/public/whatsapp-opt-in rejects invalid public key', async () => {
  const { status, data } = await requestJson({
    userId: 'user-1',
    phone: '919876543210',
    consentChecked: true,
    consentText: 'I agree to receive WhatsApp updates.'
  });

  assert.equal(status, 401);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /invalid public opt-in key/i);
});

test('POST /api/public/whatsapp-opt-in creates an opted-in contact with proof data', async () => {
  const { status, data } = await requestJson(
    {
      userId: 'user-1',
      companyId: 'company-1',
      name: 'Public Lead',
      phone: '+91 98765 43210',
      consentChecked: true,
      consentText: 'I agree to receive WhatsApp updates from Technovohub.',
      source: 'website_form',
      scope: 'marketing',
      proofId: 'landing-001'
    },
    {
      'x-opt-in-public-key': 'public-test-key',
      'user-agent': 'node-test'
    }
  );

  assert.equal(status, 201);
  assert.equal(data.success, true);
  assert.equal(mockState.savedContacts.length, 1);
  assert.equal(mockState.savedContacts[0].whatsappOptInStatus, 'opted_in');
  assert.equal(mockState.savedContacts[0].whatsappOptInProofType, 'website_form');
  assert.equal(
    mockState.savedContacts[0].whatsappOptInTextSnapshot,
    'I agree to receive WhatsApp updates from Technovohub.'
  );
  assert.equal(mockState.savedContacts[0].phone, '919876543210');
});
