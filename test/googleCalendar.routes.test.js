const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const axios = require('axios');
const GoogleCalendarConnection = require('../models/GoogleCalendarConnection');

const authPath = require.resolve('../middleware/auth');
require.cache[authPath] = {
  id: authPath,
  filename: authPath,
  loaded: true,
  exports: (req, res, next) => {
    const authHeader = String(req.headers.authorization || '');
    if (!authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, error: 'Unauthorized: token missing' });
    }
    const userId = String(authHeader.slice(7) || '').trim() || 'test-user';
    req.user = { id: userId };
    req.companyId = 'test-company';
    return next();
  }
};

const router = require('../routes/googleCalendar');

const originalAxiosPost = axios.post;
const originalAxiosGet = axios.get;
const envBackup = {
  JWT_SECRET: process.env.JWT_SECRET,
  GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
  GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
  GOOGLE_OAUTH_REDIRECT_URI: process.env.GOOGLE_OAUTH_REDIRECT_URI,
  GOOGLE_ACCESS_TOKEN: process.env.GOOGLE_ACCESS_TOKEN,
  GOOGLE_REFRESH_TOKEN: process.env.GOOGLE_REFRESH_TOKEN,
  GOOGLE_TOKEN_ENCRYPTION_KEY: process.env.GOOGLE_TOKEN_ENCRYPTION_KEY
};

const originalGoogleCalendarConnectionFindOne = GoogleCalendarConnection.findOne;
const originalGoogleCalendarConnectionFindOneAndUpdate = GoogleCalendarConnection.findOneAndUpdate;
const originalGoogleCalendarConnectionDeleteOne = GoogleCalendarConnection.deleteOne;

const mockState = {
  deleteOneCalls: [],
  findOneCalls: []
};

const app = express();
app.use(express.json());
app.use('/api/google-calendar', router);

let server;
let baseUrl = '';

const restoreAxios = () => {
  axios.post = originalAxiosPost;
  axios.get = originalAxiosGet;
};

const restoreEnv = () => {
  process.env.JWT_SECRET = envBackup.JWT_SECRET || 'test-jwt-secret';
  process.env.GOOGLE_CLIENT_ID = envBackup.GOOGLE_CLIENT_ID || '';
  process.env.GOOGLE_CLIENT_SECRET = envBackup.GOOGLE_CLIENT_SECRET || '';
  process.env.GOOGLE_OAUTH_REDIRECT_URI = envBackup.GOOGLE_OAUTH_REDIRECT_URI || '';
  process.env.GOOGLE_ACCESS_TOKEN = envBackup.GOOGLE_ACCESS_TOKEN || '';
  process.env.GOOGLE_REFRESH_TOKEN = envBackup.GOOGLE_REFRESH_TOKEN || '';
  process.env.GOOGLE_TOKEN_ENCRYPTION_KEY =
    envBackup.GOOGLE_TOKEN_ENCRYPTION_KEY || 'test-google-token-encryption-key';
};

const restoreGoogleConnectionModel = () => {
  GoogleCalendarConnection.findOne = originalGoogleCalendarConnectionFindOne;
  GoogleCalendarConnection.findOneAndUpdate = originalGoogleCalendarConnectionFindOneAndUpdate;
  GoogleCalendarConnection.deleteOne = originalGoogleCalendarConnectionDeleteOne;
};

const toFindOneQueryResult = (value) => ({
  lean: async () => value,
  then: (resolve, reject) => Promise.resolve(value).then(resolve, reject),
  catch: (reject) => Promise.resolve(value).catch(reject),
  finally: (handler) => Promise.resolve(value).finally(handler)
});

const resetModelMockState = () => {
  mockState.deleteOneCalls = [];
  mockState.findOneCalls = [];
};

const mockNoGoogleConnection = () => {
  GoogleCalendarConnection.findOne = (query = {}) => {
    mockState.findOneCalls.push(query);
    return toFindOneQueryResult(null);
  };
  GoogleCalendarConnection.deleteOne = async (query = {}) => {
    mockState.deleteOneCalls.push(query);
    return { acknowledged: true, deletedCount: 0 };
  };
};

const setGoogleClientEnv = ({ clientId = '', clientSecret = '', redirectUri = '' } = {}) => {
  process.env.GOOGLE_CLIENT_ID = clientId;
  process.env.GOOGLE_CLIENT_SECRET = clientSecret;
  process.env.GOOGLE_OAUTH_REDIRECT_URI = redirectUri;
};

const setGoogleTokenEnv = ({ accessToken = '', refreshToken = '' } = {}) => {
  process.env.GOOGLE_ACCESS_TOKEN = accessToken;
  process.env.GOOGLE_REFRESH_TOKEN = refreshToken;
};

restoreEnv();

const requestJson = async (method, path, { token = 'test-user', body } = {}) => {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: body ? JSON.stringify(body) : undefined
  });
  const data = await response.json();
  return { status: response.status, data, headers: response.headers };
};

const requestText = async (method, path, { token = 'test-user', body } = {}) => {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      Authorization: token ? `Bearer ${token}` : '',
      'Content-Type': 'application/json'
    },
    body: body ? JSON.stringify(body) : undefined
  });
  const data = await response.text();
  return { status: response.status, data, headers: response.headers };
};

test.before(async () => {
  await new Promise((resolve) => {
    server = app.listen(0, resolve);
  });
  const { port } = server.address();
  baseUrl = `http://127.0.0.1:${port}`;
});

test.after(async () => {
  restoreAxios();
  restoreEnv();
  if (!server) return;
  await new Promise((resolve) => server.close(resolve));
});

test.afterEach(() => {
  restoreAxios();
  restoreEnv();
  restoreGoogleConnectionModel();
});

test.beforeEach(() => {
  resetModelMockState();
  mockNoGoogleConnection();
});

test('POST /connect/auth-url returns 400 when Google OAuth env is missing', async () => {
  setGoogleClientEnv({ clientId: '', clientSecret: '', redirectUri: '' });

  const { status, data } = await requestJson('POST', '/api/google-calendar/connect/auth-url', {
    token: 'auth-missing-config',
    body: { origin: 'http://localhost:5173' }
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(
    String(data.error || ''),
    /GOOGLE_CLIENT_ID/i
  );
});

test('POST /connect/auth-url returns a valid Google authorization URL', async () => {
  setGoogleClientEnv({
    clientId: 'test-google-client-id',
    clientSecret: 'test-google-client-secret',
    redirectUri: 'http://localhost:3001/api/google-calendar/oauth/callback'
  });

  const { status, data } = await requestJson('POST', '/api/google-calendar/connect/auth-url', {
    token: 'auth-url-success',
    body: { origin: 'http://localhost:5173' }
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.ok(data.authUrl);

  const authUrl = new URL(data.authUrl);
  assert.equal(authUrl.origin, 'https://accounts.google.com');
  assert.equal(authUrl.pathname, '/o/oauth2/v2/auth');
  assert.equal(authUrl.searchParams.get('client_id'), 'test-google-client-id');
  assert.equal(
    authUrl.searchParams.get('redirect_uri'),
    'http://localhost:3001/api/google-calendar/oauth/callback'
  );
  assert.ok(String(authUrl.searchParams.get('state') || '').length > 10);
});

test('GET /auth-status returns disconnected status when no connection and no env auth', async () => {
  setGoogleClientEnv({ clientId: '', clientSecret: '', redirectUri: '' });
  setGoogleTokenEnv({ accessToken: '', refreshToken: '' });

  const { status, data } = await requestJson('GET', '/api/google-calendar/auth-status', {
    token: 'auth-status-none-user'
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(data?.data?.connected, false);
  assert.equal(data?.data?.hasBackendGoogleAuth, false);
  assert.equal(data?.data?.source, 'none');
  assert.equal(data?.data?.envMode, 'none');
  assert.equal(mockState.findOneCalls.length, 1);
  assert.equal(mockState.findOneCalls[0]?.userId, 'auth-status-none-user');
  assert.equal(mockState.findOneCalls[0]?.companyId, 'test-company');
});

test('GET /auth-status returns env access-token mode when no saved user connection', async () => {
  setGoogleClientEnv({ clientId: '', clientSecret: '', redirectUri: '' });
  setGoogleTokenEnv({ accessToken: 'env-access-token-123', refreshToken: '' });

  const { status, data } = await requestJson('GET', '/api/google-calendar/auth-status', {
    token: 'auth-status-env-access-user'
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(data?.data?.connected, false);
  assert.equal(data?.data?.hasBackendGoogleAuth, true);
  assert.equal(data?.data?.source, 'env_access_token');
  assert.equal(data?.data?.envMode, 'env_access_token');
});

test('GET /auth-status prioritizes user connection when tokens exist on connection record', async () => {
  setGoogleClientEnv({ clientId: '', clientSecret: '', redirectUri: '' });
  setGoogleTokenEnv({ accessToken: 'env-access-token-should-not-win', refreshToken: '' });

  const connectedAt = new Date('2026-03-31T10:00:00.000Z');
  const expiresAt = new Date('2026-03-31T11:00:00.000Z');
  GoogleCalendarConnection.findOne = (query = {}) => {
    mockState.findOneCalls.push(query);
    return toFindOneQueryResult({
      email: 'owner@example.com',
      name: 'Owner Name',
      picture: 'https://example.com/avatar.png',
      refreshToken: 'user-refresh-token',
      accessToken: '',
      connectedAt,
      expiresAt
    });
  };

  const { status, data } = await requestJson('GET', '/api/google-calendar/auth-status', {
    token: 'auth-status-user-connection'
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(data?.data?.connected, true);
  assert.equal(data?.data?.hasBackendGoogleAuth, true);
  assert.equal(data?.data?.source, 'user_connection');
  assert.equal(data?.data?.envMode, 'env_access_token');
  assert.equal(data?.data?.profile?.email, 'owner@example.com');
  assert.equal(data?.data?.profile?.name, 'Owner Name');
  assert.equal(data?.data?.profile?.picture, 'https://example.com/avatar.png');
  assert.equal(data?.data?.connectionMeta?.hasRefreshToken, true);
  assert.equal(
    new Date(data?.data?.connectionMeta?.connectedAt || '').toISOString(),
    connectedAt.toISOString()
  );
  assert.equal(
    new Date(data?.data?.connectionMeta?.expiresAt || '').toISOString(),
    expiresAt.toISOString()
  );
});

test('GET /auth-status returns 500 when reading connection fails', async () => {
  GoogleCalendarConnection.findOne = () => {
    throw new Error('Database read failed');
  };

  const { status, data } = await requestJson('GET', '/api/google-calendar/auth-status', {
    token: 'auth-status-db-error'
  });

  assert.equal(status, 500);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /Database read failed/i);
});

test('GET /oauth/callback returns success page for a valid signed state', async () => {
  setGoogleClientEnv({
    clientId: 'test-google-client-id',
    clientSecret: 'test-google-client-secret',
    redirectUri: 'http://localhost:3001/api/google-calendar/oauth/callback'
  });

  const authUrlResponse = await requestJson('POST', '/api/google-calendar/connect/auth-url', {
    token: 'oauth-callback-success-user',
    body: { origin: 'http://localhost:5173' }
  });

  assert.equal(authUrlResponse.status, 200);
  assert.equal(authUrlResponse.data.success, true);

  const authUrl = new URL(authUrlResponse.data.authUrl);
  const state = String(authUrl.searchParams.get('state') || '');
  assert.ok(state.length > 10);

  axios.post = async (url, payload) => {
    assert.equal(url, 'https://oauth2.googleapis.com/token');
    const form = new URLSearchParams(String(payload || ''));
    assert.equal(form.get('code'), 'test-auth-code');
    assert.equal(form.get('client_id'), 'test-google-client-id');
    assert.equal(form.get('client_secret'), 'test-google-client-secret');
    return {
      data: {
        access_token: 'google-access-token',
        refresh_token: 'google-refresh-token',
        token_type: 'Bearer',
        expires_in: 3600,
        scope: 'openid email profile https://www.googleapis.com/auth/calendar.events'
      }
    };
  };

  axios.get = async (url) => {
    assert.equal(url, 'https://www.googleapis.com/oauth2/v2/userinfo');
    return {
      data: {
        email: 'leadowner@example.com',
        name: 'Lead Owner',
        picture: 'https://example.com/avatar.png'
      }
    };
  };

  GoogleCalendarConnection.findOne = () => ({
    lean: async () => null
  });
  GoogleCalendarConnection.findOneAndUpdate = async () => ({});

  const callbackResponse = await requestText(
    'GET',
    `/api/google-calendar/oauth/callback?code=test-auth-code&state=${encodeURIComponent(state)}`,
    { token: '' }
  );

  assert.equal(callbackResponse.status, 200);
  assert.match(callbackResponse.data, /Google Calendar connected/i);
  assert.match(callbackResponse.data, /google_oauth_success/i);
});

test('GET /oauth/callback rejects tampered OAuth state', async () => {
  setGoogleClientEnv({
    clientId: 'test-google-client-id',
    clientSecret: 'test-google-client-secret',
    redirectUri: 'http://localhost:3001/api/google-calendar/oauth/callback'
  });

  const authUrlResponse = await requestJson('POST', '/api/google-calendar/connect/auth-url', {
    token: 'oauth-callback-invalid-state-user',
    body: { origin: 'http://localhost:5173' }
  });

  assert.equal(authUrlResponse.status, 200);
  assert.equal(authUrlResponse.data.success, true);

  const authUrl = new URL(authUrlResponse.data.authUrl);
  const originalState = String(authUrl.searchParams.get('state') || '');
  assert.ok(originalState.length > 10);

  const parts = originalState.split('.');
  const tamperedState = `${parts[0]}.tampered-signature`;

  const callbackResponse = await requestText(
    'GET',
    `/api/google-calendar/oauth/callback?code=test-auth-code&state=${encodeURIComponent(tamperedState)}`,
    { token: '' }
  );

  assert.equal(callbackResponse.status, 200);
  assert.match(callbackResponse.data, /Google Calendar connection failed/i);
  assert.match(callbackResponse.data, /state signature mismatch/i);
});

test('POST /disconnect returns already disconnected when no connection exists', async () => {
  const { status, data } = await requestJson('POST', '/api/google-calendar/disconnect', {
    token: 'disconnect-no-connection',
    body: {}
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.match(String(data.message || ''), /already disconnected/i);
  assert.equal(mockState.deleteOneCalls.length, 0);
});

test('POST /disconnect revokes token and removes connection when present', async () => {
  GoogleCalendarConnection.findOne = (query = {}) => {
    mockState.findOneCalls.push(query);
    return toFindOneQueryResult({
      refreshToken: 'refresh-token-123',
      accessToken: 'access-token-123'
    });
  };

  GoogleCalendarConnection.deleteOne = async (query = {}) => {
    mockState.deleteOneCalls.push(query);
    return { acknowledged: true, deletedCount: 1 };
  };

  let revokeUrl = '';
  axios.post = async (url) => {
    revokeUrl = String(url || '');
    return { status: 200, data: {} };
  };

  const { status, data } = await requestJson('POST', '/api/google-calendar/disconnect', {
    token: 'disconnect-success-user',
    body: {}
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.match(String(data.message || ''), /disconnected successfully/i);
  assert.match(revokeUrl, /oauth2\.googleapis\.com\/revoke\?token=refresh-token-123/i);
  assert.equal(mockState.deleteOneCalls.length, 1);
  assert.equal(mockState.deleteOneCalls[0]?.userId, 'disconnect-success-user');
  assert.equal(mockState.deleteOneCalls[0]?.companyId, 'test-company');
});

test('POST /disconnect still removes local connection if revoke call fails', async () => {
  GoogleCalendarConnection.findOne = (query = {}) => {
    mockState.findOneCalls.push(query);
    return toFindOneQueryResult({
      refreshToken: '',
      accessToken: 'access-token-fallback'
    });
  };

  GoogleCalendarConnection.deleteOne = async (query = {}) => {
    mockState.deleteOneCalls.push(query);
    return { acknowledged: true, deletedCount: 1 };
  };

  axios.post = async () => {
    throw new Error('Google revoke endpoint unavailable');
  };

  const { status, data } = await requestJson('POST', '/api/google-calendar/disconnect', {
    token: 'disconnect-revoke-failure-user',
    body: {}
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.match(String(data.message || ''), /disconnected successfully/i);
  assert.equal(mockState.deleteOneCalls.length, 1);
  assert.equal(mockState.deleteOneCalls[0]?.userId, 'disconnect-revoke-failure-user');
  assert.equal(mockState.deleteOneCalls[0]?.companyId, 'test-company');
});

test('POST /meet-link validates that Google auth is provided', async () => {
  setGoogleClientEnv({ clientId: '', clientSecret: '', redirectUri: '' });
  setGoogleTokenEnv({ accessToken: '', refreshToken: '' });

  const { status, data } = await requestJson('POST', '/api/google-calendar/meet-link', {
    token: 'meet-missing-auth',
    body: {
      summary: 'QA Meeting',
      startDateTime: '2026-04-01T10:00:00.000Z',
      endDateTime: '2026-04-01T10:30:00.000Z'
    }
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /Google auth is missing/i);
});

test('POST /meet-link validates start and end time order', async () => {
  const { status, data } = await requestJson('POST', '/api/google-calendar/meet-link', {
    token: 'meet-invalid-time-range',
    body: {
      googleAccessToken: 'token-from-request',
      summary: 'Invalid Date Range',
      startDateTime: '2026-04-01T11:30:00.000Z',
      endDateTime: '2026-04-01T10:00:00.000Z'
    }
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /endDateTime must be later/i);
});

test('POST /meet-link returns Google Meet URL on success', async () => {
  axios.post = async (url, payload, config) => {
    assert.match(String(url), /\/events$/);
    assert.equal(config?.params?.conferenceDataVersion, 1);
    return {
      data: {
        id: 'event-123',
        htmlLink: 'https://calendar.google.com/event?eid=event-123',
        hangoutLink: 'https://meet.google.com/abc-defg-hij',
        start: payload.start,
        end: payload.end
      }
    };
  };

  const { status, data } = await requestJson('POST', '/api/google-calendar/meet-link', {
    token: 'meet-success',
    body: {
      googleAccessToken: 'token-from-request',
      summary: 'Sales Follow-up',
      timeZone: 'Asia/Kolkata',
      startDateTime: '2026-04-01T10:00:00.000Z',
      endDateTime: '2026-04-01T10:30:00.000Z'
    }
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(data?.data?.meetingUrl, 'https://meet.google.com/abc-defg-hij');
  assert.equal(data?.data?.authSource, 'request_token');
});

test('POST /meet-link maps Google 401 to a user-friendly auth error', async () => {
  axios.post = async () => {
    const error = new Error('Request failed');
    error.response = {
      status: 401,
      data: {
        error: { message: 'Invalid Credentials' }
      }
    };
    throw error;
  };

  const { status, data } = await requestJson('POST', '/api/google-calendar/meet-link', {
    token: 'meet-google-401',
    body: {
      googleAccessToken: 'token-from-request',
      summary: 'Auth Error Test',
      startDateTime: '2026-04-01T10:00:00.000Z',
      endDateTime: '2026-04-01T10:30:00.000Z'
    }
  });

  assert.equal(status, 401);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /Google authentication failed/i);
  assert.match(String(data.hint || ''), /Token may be expired/i);
});

test('POST /meet-link enforces rate limits per user', async () => {
  axios.post = async (_url, payload) => ({
    data: {
      id: `event-${Date.now()}`,
      htmlLink: 'https://calendar.google.com/event?eid=rate-limit-event',
      hangoutLink: 'https://meet.google.com/rate-limit-test',
      start: payload.start,
      end: payload.end
    }
  });

  let lastResponse = null;
  for (let index = 0; index < 26; index += 1) {
    lastResponse = await requestJson('POST', '/api/google-calendar/meet-link', {
      token: 'meet-rate-limit-user',
      body: {
        googleAccessToken: 'token-from-request',
        summary: `Rate Limit Run ${index + 1}`,
        startDateTime: '2026-04-01T10:00:00.000Z',
        endDateTime: '2026-04-01T10:30:00.000Z'
      }
    });
  }

  assert.ok(lastResponse);
  assert.equal(lastResponse.status, 429);
  assert.equal(lastResponse.data.success, false);
  assert.match(String(lastResponse.data.error || ''), /Too many requests/i);
  assert.ok(Number(lastResponse.data.retryAfterSeconds) >= 1);
});
