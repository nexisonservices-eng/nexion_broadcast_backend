const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const mongoose = require('mongoose');

const auth = require('../middleware/auth');
const GoogleCalendarConnection = require('../models/GoogleCalendarConnection');
const Contact = require('../models/Contact');
const LeadActivity = require('../models/LeadActivity');
const LeadTask = require('../models/LeadTask');
const { encryptGoogleToken, decryptGoogleToken } = require('../utils/googleTokenCrypto');
const { requireJwtSecret } = require('../utils/securityConfig');

const router = express.Router();

const GOOGLE_CALENDAR_API_BASE = 'https://www.googleapis.com/calendar/v3';
const GOOGLE_OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_OAUTH_REVOKE_URL = 'https://oauth2.googleapis.com/revoke';
const GOOGLE_OAUTH_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_OAUTH_USERINFO_URL = 'https://www.googleapis.com/oauth2/v2/userinfo';
const GOOGLE_OAUTH_SCOPES = [
  'openid',
  'email',
  'profile',
  'https://www.googleapis.com/auth/calendar.events'
].join(' ');

const OAUTH_STATE_CACHE_TTL_MS = 15 * 60 * 1000;
const oauthStateConfigCache = new Map();

const toCleanString = (value) => String(value || '').trim();
const normalizeOrigin = (value) => toCleanString(value).replace(/\/+$/, '');
const normalizeCompanyId = (value) => toCleanString(value);
const isSafeFrontendOrigin = (value) => /^https?:\/\/[^/\s]+$/i.test(normalizeOrigin(value));
const isSafeRedirectUri = (value) => /^https?:\/\/[^\s]+$/i.test(toCleanString(value));
const escapeHtml = (value) =>
  String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const getBackendOrigin = (req) =>
  normalizeOrigin(process.env.PUBLIC_BACKEND_URL) || `${req.protocol}://${req.get('host')}`;

const getCallbackUrl = (req) => {
  const configured = toCleanString(process.env.GOOGLE_OAUTH_REDIRECT_URI);
  if (configured && isSafeRedirectUri(configured)) return configured;
  return `${getBackendOrigin(req)}/api/google-calendar/oauth/callback`;
};

const getOAuthClientConfig = (req) => ({
  clientId: toCleanString(process.env.GOOGLE_CLIENT_ID),
  clientSecret: toCleanString(process.env.GOOGLE_CLIENT_SECRET),
  redirectUri: getCallbackUrl(req)
});

const getConnectionFilter = ({ userId, companyId }) => ({
  userId: toCleanString(userId),
  companyId: normalizeCompanyId(companyId)
});

const toObjectIdIfValid = (value) => (mongoose.Types.ObjectId.isValid(value) ? value : null);
const TASK_PRIORITIES = ['low', 'medium', 'high'];

const rateLimitStore = new Map();
const cleanupRateLimitStore = () => {
  const now = Date.now();
  for (const [key, entry] of rateLimitStore.entries()) {
    if (!entry || Number(entry.resetAt || 0) <= now) {
      rateLimitStore.delete(key);
    }
  }
};

const createRouteRateLimiter = ({ keyPrefix, windowMs, max }) => (req, res, next) => {
  try {
    cleanupRateLimitStore();
    const userKey = toCleanString(req.user?.id || req.ip || 'anonymous');
    const key = `${keyPrefix}:${userKey}`;
    const now = Date.now();
    const existing = rateLimitStore.get(key);

    if (!existing || now >= existing.resetAt) {
      rateLimitStore.set(key, {
        count: 1,
        resetAt: now + windowMs
      });
      return next();
    }

    if (existing.count >= max) {
      const retryAfterSeconds = Math.max(
        1,
        Math.ceil((existing.resetAt - now) / 1000)
      );
      res.setHeader('Retry-After', String(retryAfterSeconds));
      return res.status(429).json({
        success: false,
        error: 'Too many requests. Please try again shortly.',
        retryAfterSeconds
      });
    }

    existing.count += 1;
    rateLimitStore.set(key, existing);
    return next();
  } catch {
    return next();
  }
};

const toIsoDateTime = (value) => {
  const raw = toCleanString(value);
  if (!raw) return null;
  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
};

const toAttendeeList = (value) => {
  if (!Array.isArray(value)) return [];
  return value
    .map((attendee) => {
      if (typeof attendee === 'string') {
        const email = toCleanString(attendee);
        return email ? { email } : null;
      }
      const email = toCleanString(attendee?.email);
      if (!email) return null;
      return {
        email,
        displayName: toCleanString(attendee?.displayName) || undefined,
        optional: Boolean(attendee?.optional)
      };
    })
    .filter(Boolean);
};

const buildMeetUrlFromEvent = (eventData = {}) => {
  const directLink = toCleanString(eventData?.hangoutLink);
  if (directLink) return directLink;

  const entryPoints = Array.isArray(eventData?.conferenceData?.entryPoints)
    ? eventData.conferenceData.entryPoints
    : [];
  const videoEntry = entryPoints.find(
    (entry) =>
      toCleanString(entry?.entryPointType).toLowerCase() === 'video' &&
      toCleanString(entry?.uri)
  );
  if (videoEntry?.uri) return toCleanString(videoEntry.uri);

  const conferenceId = toCleanString(eventData?.conferenceData?.conferenceId);
  if (conferenceId) return `https://meet.google.com/${conferenceId}`;

  return '';
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const logMeetingActivity = async ({
  userId,
  companyId,
  contactId,
  conversationId,
  summary,
  meetingUrl,
  eventId,
  eventHtmlLink,
  start,
  end,
  meta = {}
}) => {
  try {
    const normalizedUserId = toObjectIdIfValid(userId);
    const normalizedContactId = toObjectIdIfValid(contactId);
    if (!normalizedUserId || !normalizedContactId) return;

    const companyFilter = companyId
      ? {
          $or: [
            { companyId },
            { companyId: null },
            { companyId: { $exists: false } }
          ]
        }
      : {};
    const contact = await Contact.findOne({
      _id: normalizedContactId,
      userId: normalizedUserId,
      ...companyFilter
    })
      .select('_id')
      .lean();
    if (!contact?._id) return;

    const normalizedConversationId = toObjectIdIfValid(conversationId) || null;

    await LeadActivity.create({
      userId: normalizedUserId,
      companyId: companyId || null,
      contactId: normalizedContactId,
      conversationId: normalizedConversationId,
      type: 'meeting_scheduled',
      meta: {
        summary: toCleanString(summary),
        meetingUrl: toCleanString(meetingUrl),
        eventId: toCleanString(eventId),
        eventHtmlLink: toCleanString(eventHtmlLink),
        start: start || null,
        end: end || null,
        ...meta
      },
      createdBy: toCleanString(userId) || null
    });
  } catch (error) {
    console.error('Google meeting activity log failed:', error?.message || error);
  }
};

const appendMeetingNoteToContact = async ({
  userId,
  companyId,
  contactId,
  summary,
  meetingUrl,
  start
}) => {
  try {
    const normalizedUserId = toObjectIdIfValid(userId);
    const normalizedContactId = toObjectIdIfValid(contactId);
    if (!normalizedUserId || !normalizedContactId) {
      return { updated: false, contact: null };
    }

    const companyFilter = companyId
      ? {
          $or: [
            { companyId },
            { companyId: null },
            { companyId: { $exists: false } }
          ]
        }
      : {};

    const contact = await Contact.findOne({
      _id: normalizedContactId,
      userId: normalizedUserId,
      ...companyFilter
    });
    if (!contact) {
      return { updated: false, contact: null };
    }

    const summaryText = toCleanString(summary) || 'Google Meet';
    const startDateText = toCleanString(start?.dateTime || start);
    const formattedStart = startDateText && !Number.isNaN(new Date(startDateText).getTime())
      ? new Date(startDateText).toLocaleString()
      : '';

    const noteLines = [
      `[Meeting Scheduled] ${summaryText}`,
      formattedStart ? `When: ${formattedStart}` : '',
      `Link: ${meetingUrl}`
    ].filter(Boolean);
    const noteBlock = noteLines.join('\n');

    const existingNotes = String(contact.notes || '').trim();
    const alreadyIncluded = existingNotes.includes(meetingUrl);
    if (alreadyIncluded) {
      return { updated: false, contact };
    }

    contact.notes = existingNotes ? `${existingNotes}\n\n${noteBlock}` : noteBlock;
    await contact.save();

    return { updated: true, contact };
  } catch (error) {
    console.error('Google meeting note append failed:', error?.message || error);
    return { updated: false, contact: null };
  }
};

const createFollowUpTaskForMeeting = async ({
  userId,
  companyId,
  contactId,
  conversationId,
  summary,
  start,
  payload = {}
}) => {
  try {
    const normalizedUserId = toObjectIdIfValid(userId);
    const normalizedContactId = toObjectIdIfValid(contactId);
    if (!normalizedUserId || !normalizedContactId) return null;

    const priorityInput = toCleanString(payload.followUpPriority).toLowerCase();
    const priority = TASK_PRIORITIES.includes(priorityInput) ? priorityInput : 'medium';
    const title =
      toCleanString(payload.followUpTitle) ||
      `Follow up after meeting: ${toCleanString(summary) || 'Google Meet'}`;
    const dueAtInput =
      toCleanString(payload.followUpDueAt) ||
      toCleanString(start?.dateTime || start || '');
    const parsedDueAt = dueAtInput ? new Date(dueAtInput) : null;
    const dueAt =
      parsedDueAt && !Number.isNaN(parsedDueAt.getTime())
        ? parsedDueAt
        : null;

    const task = await LeadTask.create({
      userId: normalizedUserId,
      companyId: companyId || null,
      contactId: normalizedContactId,
      conversationId: toObjectIdIfValid(conversationId) || null,
      title,
      description: toCleanString(payload.followUpDescription),
      dueAt,
      priority,
      status: 'pending',
      assignedTo: toCleanString(payload.followUpAssignedTo) || null,
      createdBy: toCleanString(userId) || null
    });

    await LeadActivity.create({
      userId: normalizedUserId,
      companyId: companyId || null,
      contactId: normalizedContactId,
      conversationId: toObjectIdIfValid(conversationId) || null,
      type: 'task_created',
      meta: {
        taskId: String(task?._id || ''),
        title: task?.title || title,
        dueAt: task?.dueAt || dueAt,
        priority: task?.priority || priority,
        status: task?.status || 'pending',
        source: 'meeting_schedule'
      },
      createdBy: toCleanString(userId) || null
    });

    return task;
  } catch (error) {
    console.error('Google meeting follow-up task create failed:', error?.message || error);
    return null;
  }
};

const readGoogleEnvAuthConfig = () => {
  const accessToken = toCleanString(process.env.GOOGLE_ACCESS_TOKEN);
  const refreshToken = toCleanString(process.env.GOOGLE_REFRESH_TOKEN);
  const clientId = toCleanString(process.env.GOOGLE_CLIENT_ID);
  const clientSecret = toCleanString(process.env.GOOGLE_CLIENT_SECRET);

  return {
    accessToken,
    refreshToken,
    clientId,
    clientSecret,
    hasDirectAccessToken: Boolean(accessToken),
    hasRefreshFlow: Boolean(refreshToken && clientId && clientSecret)
  };
};

const refreshAccessToken = async ({ refreshToken, clientId, clientSecret }) => {
  const formData = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    refresh_token: refreshToken,
    grant_type: 'refresh_token'
  });

  const response = await axios.post(
    GOOGLE_OAUTH_TOKEN_URL,
    formData.toString(),
    {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 20000
    }
  );

  return {
    accessToken: toCleanString(response?.data?.access_token),
    tokenType: toCleanString(response?.data?.token_type) || 'Bearer',
    expiresIn: Number(response?.data?.expires_in || 0)
  };
};

const fetchGoogleProfile = async (accessToken) => {
  const response = await axios.get(GOOGLE_OAUTH_USERINFO_URL, {
    headers: {
      Authorization: `Bearer ${accessToken}`
    },
    timeout: 20000
  });

  return {
    email: toCleanString(response?.data?.email),
    name: toCleanString(response?.data?.name),
    picture: toCleanString(response?.data?.picture)
  };
};

const getGoogleAccessTokenFromConnection = async ({ userId, companyId }) => {
  const normalizedUserId = toCleanString(userId);
  if (!normalizedUserId) {
    return { token: '', source: 'none', connection: null };
  }

  const filter = getConnectionFilter({ userId: normalizedUserId, companyId });
  const connection = await GoogleCalendarConnection.findOne(filter);
  if (!connection) {
    return { token: '', source: 'none', connection: null };
  }

  const storedAccessToken = decryptGoogleToken(connection.accessToken);
  const now = Date.now();
  const expiresAtMs = connection?.expiresAt ? new Date(connection.expiresAt).getTime() : 0;
  const isAccessTokenValid = Boolean(storedAccessToken) && (!expiresAtMs || expiresAtMs - now > 60 * 1000);

  if (isAccessTokenValid) {
    return { token: storedAccessToken, source: 'user_connection', connection };
  }

  const storedRefreshToken = decryptGoogleToken(connection.refreshToken);
  const clientId = toCleanString(process.env.GOOGLE_CLIENT_ID);
  const clientSecret = toCleanString(process.env.GOOGLE_CLIENT_SECRET);

  if (storedRefreshToken && clientId && clientSecret) {
    try {
      const refreshed = await refreshAccessToken({
        refreshToken: storedRefreshToken,
        clientId,
        clientSecret
      });

      if (!refreshed.accessToken) {
        throw new Error('Google OAuth refresh returned no access token.');
      }

      connection.accessToken = encryptGoogleToken(refreshed.accessToken);
      connection.tokenType = refreshed.tokenType || connection.tokenType || 'Bearer';
      if (refreshed.expiresIn > 0) {
        connection.expiresAt = new Date(Date.now() + refreshed.expiresIn * 1000);
      }
      connection.lastSyncedAt = new Date();
      connection.lastError = '';
      await connection.save();

      return {
        token: refreshed.accessToken,
        source: 'user_connection_refreshed',
        connection
      };
    } catch (error) {
      connection.lastError = toCleanString(error?.message) || 'Google token refresh failed.';
      await connection.save();
    }
  }

  if (storedAccessToken) {
    return {
      token: storedAccessToken,
      source: 'user_connection_stale',
      connection
    };
  }

  return { token: '', source: 'none', connection };
};

const getGoogleAccessTokenFromEnv = async () => {
  const config = readGoogleEnvAuthConfig();
  if (config.hasDirectAccessToken) {
    return { token: config.accessToken, source: 'env_access_token' };
  }

  if (!config.hasRefreshFlow) {
    return { token: '', source: 'none' };
  }

  const refreshed = await refreshAccessToken({
    refreshToken: config.refreshToken,
    clientId: config.clientId,
    clientSecret: config.clientSecret
  });

  if (!refreshed.accessToken) {
    throw new Error('Google env refresh token flow returned no access token.');
  }

  return { token: refreshed.accessToken, source: 'env_refresh_token' };
};

const resolveGoogleAccessToken = async (req, { userId, companyId } = {}) => {
  const requestToken = toCleanString(
    req.body?.googleAccessToken || req.headers['x-google-access-token']
  );
  if (requestToken) {
    return { token: requestToken, source: 'request_token' };
  }

  try {
    const fromConnection = await getGoogleAccessTokenFromConnection({ userId, companyId });
    if (fromConnection.token) return fromConnection;
  } catch {
    // Continue with env fallback.
  }

  return getGoogleAccessTokenFromEnv();
};

const encodeStatePayload = (payload) => Buffer.from(JSON.stringify(payload)).toString('base64url');
const signStatePayload = (payload) =>
  crypto
    .createHmac('sha256', requireJwtSecret('Google OAuth state signing'))
    .update(payload)
    .digest('hex');

const buildSignedState = ({ userId, companyId, origin }) => {
  const payload = encodeStatePayload({
    userId: toCleanString(userId),
    companyId: normalizeCompanyId(companyId),
    origin: toCleanString(origin),
    issuedAt: Date.now(),
    expiresAt: Date.now() + 10 * 60 * 1000
  });
  const signature = signStatePayload(payload);
  return `${payload}.${signature}`;
};

const parseSignedState = (state) => {
  const [payload = '', signature = ''] = toCleanString(state).split('.');
  if (!payload || !signature) {
    throw new Error('Google OAuth state is invalid.');
  }

  let decoded;
  try {
    decoded = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
  } catch {
    throw new Error('Google OAuth state payload is invalid.');
  }

  return { payload, signature, decoded };
};

const setCachedOAuthConfig = (state, oauthConfig) => {
  const cacheKey = toCleanString(state);
  if (!cacheKey || !oauthConfig?.clientId || !oauthConfig?.clientSecret) return;

  oauthStateConfigCache.set(cacheKey, {
    oauthConfig,
    expiresAt: Date.now() + OAUTH_STATE_CACHE_TTL_MS
  });
};

const getCachedOAuthConfig = (state) => {
  const cacheKey = toCleanString(state);
  if (!cacheKey) return null;

  const cached = oauthStateConfigCache.get(cacheKey);
  if (!cached) return null;
  if (Number(cached.expiresAt || 0) < Date.now()) {
    oauthStateConfigCache.delete(cacheKey);
    return null;
  }

  return cached.oauthConfig || null;
};

const deleteCachedOAuthConfig = (state) => {
  const cacheKey = toCleanString(state);
  if (!cacheKey) return;
  oauthStateConfigCache.delete(cacheKey);
};

const renderCallbackPage = ({ message, payload, targetOrigin = '' }) => `
  <!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Google Calendar Connect</title>
      <style>
        body { font-family: Arial, sans-serif; background:#f5f7fb; color:#102042; display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; }
        .card { background:#fff; padding:24px 28px; border-radius:16px; box-shadow:0 18px 48px rgba(16,32,66,.14); max-width:520px; width:calc(100% - 32px); }
        h1 { margin:0 0 12px; font-size:22px; }
        p { margin:0 0 16px; line-height:1.5; }
        .detail { background:#eff6ff; color:#1e3a8a; border-radius:10px; padding:12px; font-size:14px; word-break:break-word; }
        button { border:0; border-radius:10px; background:#2563eb; color:#fff; padding:10px 16px; font-weight:600; cursor:pointer; }
      </style>
    </head>
    <body>
      <div class="card">
        <h1>${escapeHtml(message)}</h1>
        ${payload?.error ? `<p class="detail">${escapeHtml(payload.error)}</p>` : ''}
        <p>You can close this window and return to the app if it does not close automatically.</p>
        <button onclick="window.close()">Close Window</button>
      </div>
      <script>
        (function () {
          try {
            var targetOrigin = ${JSON.stringify(targetOrigin)};
            if (window.opener && !window.opener.closed && targetOrigin) {
              window.opener.postMessage(${JSON.stringify(payload)}, targetOrigin);
              setTimeout(function () { window.close(); }, 250);
            }
          } catch (error) {
            console.error('Google OAuth callback handoff failed:', error);
          }
        }());
      </script>
    </body>
  </html>
`;

const connectAuthUrlRateLimiter = createRouteRateLimiter({
  keyPrefix: 'google_connect',
  windowMs: 10 * 60 * 1000,
  max: 20
});
const disconnectRateLimiter = createRouteRateLimiter({
  keyPrefix: 'google_disconnect',
  windowMs: 5 * 60 * 1000,
  max: 20
});
const meetLinkRateLimiter = createRouteRateLimiter({
  keyPrefix: 'google_meet_link',
  windowMs: 60 * 1000,
  max: 25
});

router.get('/oauth/callback', async (req, res) => {
  const { code, state, error: authError, error_description: authErrorDescription } = req.query;

  if (authError) {
    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Google Calendar connection failed',
          payload: {
            type: 'google_oauth_error',
            error: toCleanString(authErrorDescription || authError || 'Google OAuth failed.')
          }
        })
      );
  }

  try {
    const { payload, signature, decoded } = parseSignedState(state);
    const expectedSignature = signStatePayload(payload);
    if (signature !== expectedSignature) {
      throw new Error('Google OAuth state signature mismatch.');
    }
    if (!decoded?.expiresAt || Number(decoded.expiresAt) < Date.now()) {
      throw new Error('Google OAuth state expired.');
    }

    const cachedConfig = getCachedOAuthConfig(state);
    const oauthConfig = cachedConfig || getOAuthClientConfig(req);
    if (!oauthConfig.clientId || !oauthConfig.clientSecret) {
      throw new Error(
        'Google OAuth client credentials are missing. Configure GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in backend env.'
      );
    }

    const formData = new URLSearchParams({
      code: toCleanString(code),
      client_id: oauthConfig.clientId,
      client_secret: oauthConfig.clientSecret,
      redirect_uri: oauthConfig.redirectUri || getCallbackUrl(req),
      grant_type: 'authorization_code'
    });

    const tokenResponse = await axios.post(
      GOOGLE_OAUTH_TOKEN_URL,
      formData.toString(),
      {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: 20000
      }
    );

    const accessToken = toCleanString(tokenResponse?.data?.access_token);
    const refreshToken = toCleanString(tokenResponse?.data?.refresh_token);
    const tokenType = toCleanString(tokenResponse?.data?.token_type) || 'Bearer';
    const expiresIn = Number(tokenResponse?.data?.expires_in || 0);
    const scopeValue = toCleanString(tokenResponse?.data?.scope);
    const scopes = scopeValue ? scopeValue.split(/\s+/).filter(Boolean) : [];

    if (!accessToken) {
      throw new Error('Google OAuth did not return an access token.');
    }

    const profile = await fetchGoogleProfile(accessToken).catch(() => ({
      email: '',
      name: '',
      picture: ''
    }));

    const filter = getConnectionFilter({
      userId: decoded.userId,
      companyId: decoded.companyId
    });
    const existingConnection = await GoogleCalendarConnection.findOne(filter).lean();
    const existingRefreshToken = decryptGoogleToken(existingConnection?.refreshToken || '');
    const effectiveRefreshToken = refreshToken || existingRefreshToken;

    const updateDoc = {
      userId: toCleanString(decoded.userId),
      companyId: normalizeCompanyId(decoded.companyId),
      email: profile.email || toCleanString(existingConnection?.email),
      name: profile.name || toCleanString(existingConnection?.name),
      picture: profile.picture || toCleanString(existingConnection?.picture),
      accessToken: encryptGoogleToken(accessToken),
      refreshToken: encryptGoogleToken(effectiveRefreshToken),
      tokenType,
      scope: scopes,
      connectedAt: new Date(),
      lastSyncedAt: new Date(),
      lastError: ''
    };

    if (expiresIn > 0) {
      updateDoc.expiresAt = new Date(Date.now() + expiresIn * 1000);
    }

    await GoogleCalendarConnection.findOneAndUpdate(filter, updateDoc, {
      new: true,
      upsert: true,
      setDefaultsOnInsert: true
    });

    deleteCachedOAuthConfig(state);

    const targetOrigin = isSafeFrontendOrigin(decoded.origin)
      ? normalizeOrigin(decoded.origin)
      : (isSafeFrontendOrigin(process.env.FRONTEND_URL)
          ? normalizeOrigin(process.env.FRONTEND_URL)
          : '');

    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Google Calendar connected',
          payload: {
            type: 'google_oauth_success',
            profile
          },
          targetOrigin
        })
      );
  } catch (error) {
    deleteCachedOAuthConfig(state);
    return res
      .status(200)
      .send(
        renderCallbackPage({
          message: 'Google Calendar connection failed',
          payload: {
            type: 'google_oauth_error',
            error: toCleanString(error?.message || 'Google OAuth failed.')
          }
        })
      );
  }
});

router.use(auth);

router.get('/auth-status', async (req, res) => {
  try {
    const userId = toCleanString(req.user?.id);
    const companyId = normalizeCompanyId(req.companyId);
    const filter = getConnectionFilter({ userId, companyId });
    const connection = userId ? await GoogleCalendarConnection.findOne(filter).lean() : null;
    const envConfig = readGoogleEnvAuthConfig();

    const hasConnectionToken = Boolean(
      decryptGoogleToken(connection?.refreshToken || '') ||
      decryptGoogleToken(connection?.accessToken || '')
    );
    const envMode = envConfig.hasDirectAccessToken
      ? 'env_access_token'
      : envConfig.hasRefreshFlow
        ? 'env_refresh_token'
        : 'none';

    return res.json({
      success: true,
      data: {
        connected: hasConnectionToken,
        hasBackendGoogleAuth: hasConnectionToken || envMode !== 'none',
        source: hasConnectionToken ? 'user_connection' : envMode,
        envMode,
        profile: {
          email: toCleanString(connection?.email),
          name: toCleanString(connection?.name),
          picture: toCleanString(connection?.picture)
        },
        connectionMeta: {
          connectedAt: connection?.connectedAt || null,
          expiresAt: connection?.expiresAt || null,
          hasRefreshToken: Boolean(decryptGoogleToken(connection?.refreshToken || ''))
        }
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: toCleanString(error?.message || 'Failed to load Google auth status.')
    });
  }
});

router.post('/connect/auth-url', connectAuthUrlRateLimiter, async (req, res) => {
  try {
    const oauthConfig = getOAuthClientConfig(req);
    if (!oauthConfig.clientId || !oauthConfig.clientSecret) {
      return res.status(400).json({
        success: false,
        error:
          'Google OAuth is not configured. Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET and GOOGLE_OAUTH_REDIRECT_URI in backend env.'
      });
    }

    const state = buildSignedState({
      userId: req.user?.id,
      companyId: req.companyId,
      origin: req.body?.origin || process.env.FRONTEND_URL || ''
    });
    setCachedOAuthConfig(state, oauthConfig);

    const params = new URLSearchParams({
      client_id: oauthConfig.clientId,
      redirect_uri: oauthConfig.redirectUri,
      response_type: 'code',
      access_type: 'offline',
      include_granted_scopes: 'true',
      prompt: 'consent',
      scope: GOOGLE_OAUTH_SCOPES,
      state
    });

    return res.json({
      success: true,
      authUrl: `${GOOGLE_OAUTH_AUTH_URL}?${params.toString()}`
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: toCleanString(error?.message || 'Failed to generate Google auth URL.')
    });
  }
});

router.post('/disconnect', disconnectRateLimiter, async (req, res) => {
  try {
    const userId = toCleanString(req.user?.id);
    const companyId = normalizeCompanyId(req.companyId);
    const filter = getConnectionFilter({ userId, companyId });
    const connection = await GoogleCalendarConnection.findOne(filter);

    if (!connection) {
      return res.json({
        success: true,
        message: 'Google Calendar was already disconnected.'
      });
    }

    const revokeToken =
      decryptGoogleToken(connection.refreshToken) || decryptGoogleToken(connection.accessToken);

    if (revokeToken) {
      try {
        await axios.post(
          `${GOOGLE_OAUTH_REVOKE_URL}?token=${encodeURIComponent(revokeToken)}`,
          null,
          { timeout: 15000 }
        );
      } catch {
        // Ignore revoke errors and proceed with local disconnect.
      }
    }

    await GoogleCalendarConnection.deleteOne(filter);

    return res.json({
      success: true,
      message: 'Google Calendar disconnected successfully.'
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: toCleanString(error?.message || 'Failed to disconnect Google Calendar.')
    });
  }
});

router.post('/meet-link', meetLinkRateLimiter, async (req, res) => {
  try {
    const { token: googleAccessToken, source: authSource } = await resolveGoogleAccessToken(req, {
      userId: req.user?.id,
      companyId: req.companyId
    });
    const summary = toCleanString(req.body?.summary) || 'Google Meet Meeting';
    const description = toCleanString(req.body?.description);
    const timeZone = toCleanString(req.body?.timeZone) || 'UTC';
    const calendarIdRaw = toCleanString(req.body?.calendarId) || 'primary';
    const startDateTime = toIsoDateTime(req.body?.startDateTime);
    const endDateTime = toIsoDateTime(req.body?.endDateTime);
    const attendees = toAttendeeList(req.body?.attendees);
    const activityContactId = toCleanString(req.body?.contactId);
    const activityConversationId = toCleanString(req.body?.conversationId);
    const shouldAppendToNotes = req.body?.appendToNotes !== false;
    const shouldCreateFollowUpTask = Boolean(req.body?.createFollowUpTask);

    const sendUpdatesRaw = toCleanString(req.body?.sendUpdates);
    const sendUpdates = ['all', 'externalOnly', 'none'].includes(sendUpdatesRaw)
      ? sendUpdatesRaw
      : 'none';

    if (!googleAccessToken) {
      return res.status(400).json({
        success: false,
        error:
          'Google auth is missing. Connect Google in Settings, provide googleAccessToken, or configure backend env credentials.'
      });
    }

    if (!startDateTime || !endDateTime) {
      return res.status(400).json({
        success: false,
        error: 'Valid startDateTime and endDateTime are required'
      });
    }

    if (new Date(endDateTime).getTime() <= new Date(startDateTime).getTime()) {
      return res.status(400).json({
        success: false,
        error: 'endDateTime must be later than startDateTime'
      });
    }

    const calendarId = encodeURIComponent(calendarIdRaw);
    const requestId = `meet-${Date.now()}-${crypto.randomUUID()}`;
    const createEventUrl = `${GOOGLE_CALENDAR_API_BASE}/calendars/${calendarId}/events`;

    const payload = {
      summary,
      description,
      start: { dateTime: startDateTime, timeZone },
      end: { dateTime: endDateTime, timeZone },
      attendees,
      conferenceData: {
        createRequest: {
          requestId,
          conferenceSolutionKey: { type: 'hangoutsMeet' }
        }
      }
    };

    const requestConfig = {
      headers: {
        Authorization: `Bearer ${googleAccessToken}`,
        'Content-Type': 'application/json'
      },
      params: {
        conferenceDataVersion: 1,
        sendUpdates
      },
      timeout: 20000
    };

    const createResponse = await axios.post(createEventUrl, payload, requestConfig);
    let eventData = createResponse?.data || {};
    let meetingUrl = buildMeetUrlFromEvent(eventData);

    if (!meetingUrl && toCleanString(eventData?.id)) {
      await sleep(700);
      const eventId = encodeURIComponent(toCleanString(eventData.id));
      const readResponse = await axios.get(
        `${GOOGLE_CALENDAR_API_BASE}/calendars/${calendarId}/events/${eventId}`,
        {
          headers: {
            Authorization: `Bearer ${googleAccessToken}`
          },
          params: {
            conferenceDataVersion: 1
          },
          timeout: 20000
        }
      );
      eventData = readResponse?.data || eventData;
      meetingUrl = buildMeetUrlFromEvent(eventData);
    }

    if (!meetingUrl) {
      return res.status(502).json({
        success: false,
        error: 'Calendar event created but Google Meet URL was not generated',
        data: {
          eventId: eventData?.id || null,
          eventHtmlLink: eventData?.htmlLink || null,
          conferenceStatus:
            eventData?.conferenceData?.createRequest?.status?.statusCode || null
        }
      });
    }

    const noteUpdateResult =
      activityContactId && shouldAppendToNotes
        ? await appendMeetingNoteToContact({
            userId: req.user?.id,
            companyId: req.companyId || null,
            contactId: activityContactId,
            summary,
            meetingUrl,
            start: eventData?.start || { dateTime: startDateTime, timeZone }
          })
        : { updated: false, contact: null };

    const followUpTask =
      activityContactId && shouldCreateFollowUpTask
        ? await createFollowUpTaskForMeeting({
            userId: req.user?.id,
            companyId: req.companyId || null,
            contactId: activityContactId,
            conversationId: activityConversationId,
            summary,
            start: eventData?.start || { dateTime: startDateTime, timeZone },
            payload: req.body || {}
          })
        : null;

    await logMeetingActivity({
      userId: req.user?.id,
      companyId: req.companyId || null,
      contactId: activityContactId,
      conversationId: activityConversationId,
      summary,
      meetingUrl,
      eventId: eventData?.id || null,
      eventHtmlLink: eventData?.htmlLink || null,
      start: eventData?.start || { dateTime: startDateTime, timeZone },
      end: eventData?.end || { dateTime: endDateTime, timeZone },
      meta: {
        noteUpdated: Boolean(noteUpdateResult?.updated),
        followUpTaskCreated: Boolean(followUpTask?._id)
      }
    });

    return res.json({
      success: true,
      data: {
        meetingUrl,
        eventId: eventData?.id || null,
        eventHtmlLink: eventData?.htmlLink || null,
        calendarId: calendarIdRaw,
        authSource,
        start: eventData?.start || { dateTime: startDateTime, timeZone },
        end: eventData?.end || { dateTime: endDateTime, timeZone },
        noteUpdated: Boolean(noteUpdateResult?.updated),
        updatedContact: noteUpdateResult?.contact
          ? (typeof noteUpdateResult.contact.toObject === 'function'
              ? noteUpdateResult.contact.toObject()
              : noteUpdateResult.contact)
          : null,
        followUpTask: followUpTask
          ? {
              id: String(followUpTask?._id || ''),
              title: String(followUpTask?.title || ''),
              dueAt: followUpTask?.dueAt || null,
              priority: String(followUpTask?.priority || 'medium'),
              status: String(followUpTask?.status || 'pending')
            }
          : null
      }
    });
  } catch (error) {
    const googleError = error?.response?.data?.error;
    const statusCode = Number(error?.response?.status) || 500;
    const rawMessage =
      toCleanString(googleError?.message) ||
      toCleanString(error?.response?.data?.message) ||
      toCleanString(error?.message) ||
      'Failed to create Google Calendar event';
    let message = rawMessage;
    let hint = '';

    if (statusCode === 401 || statusCode === 403) {
      message = 'Google authentication failed. Reconnect Google Calendar and try again.';
      hint = 'Token may be expired, revoked, or missing required scope calendar.events.';
    } else if (statusCode === 404) {
      message = 'Google Calendar resource not found. Verify calendarId and account access.';
      hint = rawMessage;
    } else if (statusCode === 429) {
      message = 'Google API rate limit reached. Please retry after a short delay.';
      hint = rawMessage;
    } else if (statusCode >= 500) {
      hint = 'Google API is temporarily unavailable or returned an internal error.';
    }

    return res.status(statusCode).json({
      success: false,
      error: message,
      hint,
      details: googleError || null
    });
  }
});

module.exports = router;
