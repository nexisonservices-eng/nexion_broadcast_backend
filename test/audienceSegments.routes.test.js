const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

const authPath = require.resolve('../middleware/auth');
const tenantPolicyPath = require.resolve('../middleware/tenantPolicy');
const segmentModelPath = require.resolve('../models/AudienceSegment');
const routePath = require.resolve('../routes/audienceSegments');

const originalCacheEntries = new Map(
  [authPath, tenantPolicyPath, segmentModelPath, routePath].map((path) => [path, require.cache[path]])
);

const mockState = {
  findResults: [],
  findOneAndUpdateResult: null,
  deleteOneResult: { deletedCount: 1 },
  findQueries: [],
  findOneAndUpdateCalls: [],
  deleteOneCalls: []
};

const resetState = () => {
  mockState.findResults = [];
  mockState.findOneAndUpdateResult = null;
  mockState.deleteOneResult = { deletedCount: 1 };
  mockState.findQueries = [];
  mockState.findOneAndUpdateCalls = [];
  mockState.deleteOneCalls = [];
};

const buildSegmentDoc = (payload = {}) => ({
  _id: payload._id || 'segment-1',
  id: payload.id || payload._id || 'segment-1',
  name: payload.name || 'Segment',
  description: payload.description || '',
  filters: payload.filters || {},
  contacts: Array.isArray(payload.contacts) ? payload.contacts : [],
  recipientCount: Number(payload.recipientCount || 0) || 0,
  createdAt: payload.createdAt || new Date('2026-01-01T00:00:00.000Z'),
  updatedAt: payload.updatedAt || new Date('2026-01-02T00:00:00.000Z')
});

const segmentModelMock = {
  find: (query) => {
    mockState.findQueries.push(query);
    return {
      sort: () => ({
        lean: async () => mockState.findResults.map((segment) => buildSegmentDoc(segment))
      })
    };
  },
  findOneAndUpdate: (filter, update) => {
    mockState.findOneAndUpdateCalls.push({ filter, update });
    const saved = mockState.findOneAndUpdateResult || {
      ...update,
      _id: update.id || filter._id || 'segment-saved-1',
      id: update.id || filter._id || 'segment-saved-1'
    };
    return {
      lean: async () => buildSegmentDoc(saved)
    };
  },
  deleteOne: async (filter) => {
    mockState.deleteOneCalls.push(filter);
    return mockState.deleteOneResult;
  }
};

require.cache[authPath] = {
  id: authPath,
  filename: authPath,
  loaded: true,
  exports: (req, _res, next) => {
    req.user = { id: 'segment-user-1', email: 'segment@example.com' };
    req.companyId = 'segment-company-1';
    next();
  }
};

require.cache[tenantPolicyPath] = {
  id: tenantPolicyPath,
  filename: tenantPolicyPath,
  loaded: true,
  exports: {
    requireTenantPolicy: () => (_req, _res, next) => next()
  }
};

require.cache[segmentModelPath] = {
  id: segmentModelPath,
  filename: segmentModelPath,
  loaded: true,
  exports: segmentModelMock
};

delete require.cache[routePath];
const audienceSegmentsRouter = require('../routes/audienceSegments');

const app = express();
app.use(express.json());
app.use('/api/audience-segments', audienceSegmentsRouter);

let server;
let baseUrl = '';

const requestJson = async (method, path, body) => {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: 'Bearer segment-user-1'
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
});

test.beforeEach(() => {
  resetState();
});

test('GET /api/audience-segments returns saved segments', async () => {
  mockState.findResults = [
    {
      _id: 'segment-a',
      name: 'VIP',
      contacts: [{ phone: '919999999999' }],
      recipientCount: 1
    }
  ];

  const { status, data } = await requestJson('GET', '/api/audience-segments');

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(Array.isArray(data.data), true);
  assert.equal(data.data[0].name, 'VIP');
});

test('POST /api/audience-segments saves a segment', async () => {
  const { status, data } = await requestJson('POST', '/api/audience-segments', {
    name: 'Marketing Leads',
    filters: {
      optInFilter: 'opted_in'
    },
    contacts: [
      {
        _id: 'contact-1',
        phone: '919888888888',
        name: 'Lead 1',
        sourceType: 'public_opt_in',
        whatsappOptInStatus: 'opted_in'
      }
    ]
  });

  assert.equal(status, 201);
  assert.equal(data.success, true);
  assert.equal(mockState.findOneAndUpdateCalls.length, 1);
  assert.equal(mockState.findOneAndUpdateCalls[0].update.name, 'Marketing Leads');
  assert.equal(mockState.findOneAndUpdateCalls[0].update.recipientCount, 1);
});

test('DELETE /api/audience-segments/:id deletes a segment', async () => {
  const { status, data } = await requestJson('DELETE', '/api/audience-segments/segment-delete-1');

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(mockState.deleteOneCalls.length, 1);
});
