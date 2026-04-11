const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

const originalFetch = global.fetch;

const authPath = require.resolve('../middleware/auth');
const contactModelPath = require.resolve('../models/Contact');
const contactsRoutePath = require.resolve('../routes/contacts');

const originalCacheEntries = new Map(
  [authPath, contactModelPath, contactsRoutePath].map((path) => [path, require.cache[path]])
);

const buildMockContactDoc = (payload = {}) => ({
  _id: payload._id || 'contact-test-1',
  userId: payload.userId || 'contacts-route-user',
  companyId: payload.companyId || 'contacts-route-company',
  name: payload.name || 'Test Contact',
  phone: payload.phone || '919999999999',
  email: payload.email || '',
  whatsappOptInStatus: payload.whatsappOptInStatus || 'unknown',
  whatsappOptInAt: payload.whatsappOptInAt || null,
  whatsappOptInSource: payload.whatsappOptInSource || null,
  whatsappOptInScope: payload.whatsappOptInScope || null,
  whatsappOptInTextSnapshot: payload.whatsappOptInTextSnapshot || null,
  whatsappOptInProofType: payload.whatsappOptInProofType || null,
  whatsappOptInProofId: payload.whatsappOptInProofId || null,
  whatsappOptInProofUrl: payload.whatsappOptInProofUrl || null,
  whatsappOptInCapturedBy: payload.whatsappOptInCapturedBy || null,
  whatsappOptInPageUrl: payload.whatsappOptInPageUrl || null,
  whatsappOptInIp: payload.whatsappOptInIp || null,
  whatsappOptInUserAgent: payload.whatsappOptInUserAgent || null,
  whatsappOptInMetadata: payload.whatsappOptInMetadata || null,
  whatsappOptOutAt: payload.whatsappOptOutAt || null,
  isBlocked: payload.isBlocked || false,
  async save() {
    return this;
  },
  toObject() {
    return { ...this };
  }
});

const mockState = {
  findOneQueue: [],
  findOneQueries: [],
  findOneAndUpdateCalls: []
};

const resetState = () => {
  mockState.findOneQueue = [];
  mockState.findOneQueries = [];
  mockState.findOneAndUpdateCalls = [];
};

const contactModelMock = {
  findOne: async (query) => {
    mockState.findOneQueries.push(query);
    if (!mockState.findOneQueue.length) return null;
    const next = mockState.findOneQueue.shift();
    return next ? buildMockContactDoc(next) : null;
  },
  findOneAndUpdate: async (filter, update) => {
    mockState.findOneAndUpdateCalls.push({ filter, update });
    return buildMockContactDoc({
      _id: filter._id,
      userId: filter.userId,
      ...update
    });
  },
  findOneAndDelete: async () => null,
  create: async (payload) => buildMockContactDoc(payload),
  find: () => ({
    select: () => ({
      sort: () => ({
        lean: async () => []
      })
    })
  })
};

require.cache[authPath] = {
  id: authPath,
  filename: authPath,
  loaded: true,
  exports: (req, _res, next) => {
    req.user = { id: 'contacts-route-user' };
    req.companyId = 'contacts-route-company';
    next();
  }
};

require.cache[contactModelPath] = {
  id: contactModelPath,
  filename: contactModelPath,
  loaded: true,
  exports: contactModelMock
};

delete require.cache[contactsRoutePath];
const contactsRouter = require('../routes/contacts');

const app = express();
app.use(express.json());
app.use('/api/contacts', contactsRouter);

let server;
let baseUrl = '';

const requestJson = async (method, path, body) => {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: 'Bearer contacts-route-user'
    },
    body: body ? JSON.stringify(body) : undefined
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

  global.fetch = originalFetch;
});

test.beforeEach(() => {
  resetState();
});

test('POST /:id/whatsapp-opt-in requires consent text and proof type', async () => {
  mockState.findOneQueue = [
    {
      _id: 'contact-1',
      whatsappOptInStatus: 'unknown'
    }
  ];

  const { status, data } = await requestJson('POST', '/api/contacts/contact-1/whatsapp-opt-in', {
    source: 'manual',
    proofType: 'call_record'
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /consent text/i);
});

test('POST /:id/whatsapp-opt-in saves proof-backed opt-in data', async () => {
  mockState.findOneQueue = [
    {
      _id: 'contact-2',
      whatsappOptInStatus: 'unknown'
    }
  ];

  const { status, data } = await requestJson('POST', '/api/contacts/contact-2/whatsapp-opt-in', {
    source: 'website_form',
    scope: 'marketing',
    consentText: 'I agree to receive WhatsApp updates.',
    proofType: 'website_form',
    proofId: 'form-001'
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(data.data.contact.whatsappOptInStatus, 'opted_in');
  assert.equal(data.data.contact.whatsappOptInProofType, 'website_form');
  assert.equal(data.data.contact.whatsappOptInTextSnapshot, 'I agree to receive WhatsApp updates.');
});

test('PUT /:id blocks opted-in promotion without proof for a non-opted-in contact', async () => {
  mockState.findOneQueue = [
    {
      _id: 'contact-3',
      whatsappOptInStatus: 'unknown'
    }
  ];

  const { status, data } = await requestJson('PUT', '/api/contacts/contact-3', {
    whatsappOptInStatus: 'opted_in'
  });

  assert.equal(status, 400);
  assert.match(String(data.error || ''), /consent text/i);
  assert.equal(mockState.findOneAndUpdateCalls.length, 0);
});

test('PUT /:id allows opted-in promotion when proof fields are supplied', async () => {
  mockState.findOneQueue = [
    {
      _id: 'contact-4',
      whatsappOptInStatus: 'unknown'
    }
  ];

  const { status, data } = await requestJson('PUT', '/api/contacts/contact-4', {
    whatsappOptInStatus: 'opted_in',
    consentText: 'Customer consented over the phone.',
    proofType: 'call_record',
    proofId: 'call-123',
    scope: 'service'
  });

  assert.equal(status, 200);
  assert.equal(data.whatsappOptInStatus, 'opted_in');
  assert.equal(mockState.findOneAndUpdateCalls.length, 1);
  assert.equal(
    mockState.findOneAndUpdateCalls[0].update.whatsappOptInTextSnapshot,
    'Customer consented over the phone.'
  );
  assert.equal(mockState.findOneAndUpdateCalls[0].update.whatsappOptInProofType, 'call_record');
});
