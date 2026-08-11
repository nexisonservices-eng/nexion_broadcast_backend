const axios = require('axios');
const MetaAdCampaign = require('../models/MetaAdCampaign');
const MetaAdsConnection = require('../models/MetaAdsConnection');
const MetaAdsTransaction = require('../models/MetaAdsTransaction');
const MetaAdsWallet = require('../models/MetaAdsWallet');
const Campaign = require('../models/campaign');
const { getMetaAdsConfig } = require('../config/metaAdsConfig');
const metaAuthService = require('./metaAuthService');
const metaCreativeService = require('./metaCreativeService');

const { GRAPH_BASE_URL, decryptMetaToken, encryptMetaToken } = metaAuthService;

const normalizeArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const normalizeAdAccountId = (value) => {
  const raw = String(value || '')
    .trim()
    .replace(/\s+/g, '')
    .replace(/^(?:act_)+/i, '');

  return raw ? `act_${raw}` : '';
};
const normalizeAdAccountIdForPath = (value) => {
  const canonical = normalizeAdAccountId(value);
  return canonical.replace(/^act_/i, '');
};
const summarizePage = (page) => ({
  id: String(page?.id || '').trim(),
  name: String(page?.name || '').trim()
});
const buildAdAccountPath = (adAccountId, resource = '') => {
  const normalizedId = normalizeAdAccountIdForPath(adAccountId);
  const cleanResource = String(resource || '').replace(/^\/+/, '');
  return cleanResource ? `act_${normalizedId}/${cleanResource}` : `act_${normalizedId}`;
};
const toCanonicalAdAccountId = (value) => normalizeAdAccountId(value);

const normalizeCountryToken = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z]/g, '');
const COUNTRY_NAME_TO_CODE = {
  india: 'IN',
  unitedstates: 'US',
  usa: 'US',
  us: 'US',
  canada: 'CA',
  unitedkingdom: 'GB',
  uk: 'GB',
  greatbritain: 'GB',
  england: 'GB',
  australia: 'AU',
  newzealand: 'NZ',
  singapore: 'SG',
  unitedarabemirates: 'AE',
  uae: 'AE',
  saudiarabia: 'SA',
  qatar: 'QA',
  oman: 'OM',
  kuwait: 'KW',
  bahrain: 'BH',
  malaysia: 'MY',
  indonesia: 'ID',
  philippines: 'PH',
  thailand: 'TH',
  vietnam: 'VN',
  southafrica: 'ZA',
  nigeria: 'NG',
  kenya: 'KE',
  germany: 'DE',
  france: 'FR',
  italy: 'IT',
  spain: 'ES',
  netherlands: 'NL',
  sweden: 'SE',
  norway: 'NO',
  denmark: 'DK',
  switzerland: 'CH',
  belgium: 'BE',
  portugal: 'PT',
  ireland: 'IE',
  austria: 'AT',
  poland: 'PL',
  czechrepublic: 'CZ',
  turkey: 'TR',
  mexico: 'MX',
  brazil: 'BR',
  argentina: 'AR',
  chile: 'CL',
  colombia: 'CO',
  peru: 'PE'
};

const parseDelimitedTerms = (value) =>
  [...new Set(
    String(value || '')
      .split(/[\n,;]+/)
      .map((item) => item.trim())
      .filter(Boolean)
  )];

const getEnvConfig = () => getMetaAdsConfig();

const requireCanonicalMetaAdAccountId = (value, stage = 'Campaign creation') => {
  const raw = String(value || '').trim();
  if (!raw) {
    const error = new Error('Please select a Meta Ad Account before creating a campaign.');
    error.stage = stage;
    error.status = 400;
    throw error;
  }

  const collapsed = raw.replace(/^(?:act_)+/i, '');
  if (!collapsed || !/^[A-Za-z0-9]+$/i.test(collapsed)) {
    throw buildStageErrorWithDetails(
      stage,
      'Invalid Meta Ad Account ID. Please choose a valid ad account from your connected Meta accounts.',
      { value: raw },
      400
    );
  }

  return `act_${collapsed}`;
};

const resolveMetaCampaignEnvConfig = () => {
  const env = getEnvConfig();
  const accessToken = String(env.accessToken || '').trim();

  return {
    apiVersion: String(env.apiVersion || 'v23.0').trim(),
    appId: String(env.appId || '').trim(),
    appSecret: String(env.appSecret || '').trim(),
    accessToken
  };
};

const resolveMetaCampaignAuthConfig = (accessContext = {}) => {
  const adminMetaConfig = accessContext?.adminMetaConfig || {};

  return {
    appId: String(adminMetaConfig.appId || '').trim(),
    appSecret: String(adminMetaConfig.appSecret || '').trim()
  };
};

const normalizeMetaAdAccountRecord = (account = {}) => ({
  ...account,
  id: toCanonicalAdAccountId(account?.id),
  name: String(account?.name || '').trim(),
  accountStatus: account?.account_status ?? account?.accountStatus ?? null,
  currency: String(account?.currency || '').trim(),
  amountSpent: Number(account?.amount_spent || account?.amountSpent || 0) || 0
});

const resolveMetaAdAccountSelection = async ({
  userId,
  adAccountId,
  accessToken,
  apiVersion,
  stage = 'Campaign creation'
} = {}) => {
  const userAccessContext = userId ? await getAccessContextForUser(userId) : null;
  const accessContext = {
    ...(userAccessContext || {}),
    accessToken: String(accessToken || userAccessContext?.accessToken || '').trim(),
    apiVersion: String(apiVersion || userAccessContext?.apiVersion || getEnvConfig().apiVersion || 'v23.0').trim()
  };

  const resolvedAccessToken = String(accessContext.accessToken || '').trim();
  const resolvedApiVersion = String(accessContext.apiVersion || apiVersion || getEnvConfig().apiVersion || 'v23.0').trim();
  const requestedAdAccountId = toCanonicalAdAccountId(adAccountId);
  const savedAdAccountId = toCanonicalAdAccountId(accessContext?.connection?.selectedAdAccountId || '');

  const availableAdAccounts = await getUserAdAccounts({
    userId,
    accessToken: resolvedAccessToken,
    apiVersion: resolvedApiVersion
  });
  const firstAvailableAdAccountId = toCanonicalAdAccountId(availableAdAccounts[0]?.id || '');
  const selectedAdAccountId = requestedAdAccountId || savedAdAccountId || firstAvailableAdAccountId;

  if (!selectedAdAccountId) {
    const error = new Error('Please select a Meta Ad Account before creating a campaign.');
    error.stage = stage;
    error.details = { userId: String(userId || '') };
    error.status = 400;
    throw error;
  }

  const matchedAccount = availableAdAccounts.find(
    (account) => toCanonicalAdAccountId(account?.id || '') === selectedAdAccountId
  );

  if (!matchedAccount) {
    throw buildStageErrorWithDetails(
      stage,
      'The selected Meta Ad Account is not available for this connected Meta user.',
      {
        adAccountId: selectedAdAccountId,
        availableAdAccounts: availableAdAccounts.map((account) => ({
          id: account.id,
          name: account.name
        }))
      },
      400
    );
  }

  return {
    accessToken: resolvedAccessToken,
    apiVersion: resolvedApiVersion,
    selectedAdAccountId,
    availableAdAccounts,
    accessContext
  };
};

const META_GRAPH_CACHE_TTL_MS = 10 * 60 * 1000;
const META_AUTO_SYNC_COOLDOWN_MS = 10 * 60 * 1000;
const META_MANUAL_SYNC_DEBOUNCE_MS = 60 * 1000;
const META_GRAPH_MAX_CONCURRENCY = 2;
const META_GRAPH_BASE_BACKOFF_MS = 60 * 1000;
const metaGraphRequestCache = new Map();
const metaGraphRequestInflight = new Map();
const metaGraphRequestQueue = [];
const metaGraphRateLimits = new Map();
let metaGraphRequestActiveCount = 0;
let metaGraphRequestSequence = 0;
const metaGraphRequestStats = {
  requestCount: 0,
  cacheHits: 0,
  duplicateSkips: 0,
  cooldownSkips: 0,
  rateLimitEvents: 0
};

const cloneMetaValue = (value) => {
  if (value === null || value === undefined) {
    return value;
  }

  try {
    if (typeof structuredClone === 'function') {
      return structuredClone(value);
    }
  } catch {
    // Fall through to JSON cloning.
  }

  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
};

const stableSerialize = (value) => {
  if (value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map((item) => stableSerialize(item));
  if (value instanceof Date) return value.toISOString();
  if (typeof value !== 'object') return value;

  return Object.keys(value)
    .sort()
    .reduce((acc, key) => {
      const item = value[key];
      if (item === undefined) return acc;
      acc[key] = stableSerialize(item);
      return acc;
    }, {});
};

const buildMetaGraphRequestKey = ({ method, path, params, data, accessToken, apiVersion }) =>
  JSON.stringify({
    method: String(method || 'GET').trim().toUpperCase(),
    path: String(path || '').trim().replace(/^\/+/, ''),
    params: stableSerialize(params || {}),
    data: stableSerialize(data || {}),
    accessToken: String(accessToken || '').trim(),
    apiVersion: String(apiVersion || '').trim()
  });

const getMetaTokenKey = ({ accessToken, apiVersion }) =>
  `${String(apiVersion || '').trim()}::${String(accessToken || '').trim()}`;

const cloneMetaResponse = (response) => {
  if (!response) return null;

  return {
    data: cloneMetaValue(response.data),
    status: response.status,
    statusText: response.statusText,
    headers: cloneMetaValue(response.headers || {}),
    config: cloneMetaValue(response.config || {})
  };
};

const normalizeMetaGraphCachePath = (path) =>
  String(path || '')
    .trim()
    .replace(/^\/+/, '')
    .replace(/\/+$/, '');

const getCachedMetaGraphResponse = (key) => {
  const entry = metaGraphRequestCache.get(key);
  if (!entry) return null;

  if (entry.expiresAt <= Date.now()) {
    metaGraphRequestCache.delete(key);
    return null;
  }

  return cloneMetaValue(entry.response);
};

const setCachedMetaGraphResponse = (key, response, ttlMs = META_GRAPH_CACHE_TTL_MS) => {
  metaGraphRequestCache.set(key, {
    expiresAt: Date.now() + ttlMs,
    response: cloneMetaResponse(response)
  });
};

const invalidateMetaGraphCacheEntries = ({
  accessToken,
  apiVersion,
  paths = []
} = {}) => {
  const resolvedApiVersion = String(apiVersion || getEnvConfig().apiVersion || 'v23.0').trim();
  const tokenKey = String(accessToken || '').trim()
    ? getMetaTokenKey({ accessToken, apiVersion: resolvedApiVersion })
    : '';
  const normalizedPaths = [...new Set(
    (Array.isArray(paths) ? paths : [])
      .map((path) => normalizeMetaGraphCachePath(path))
      .filter(Boolean)
  )];

  if (!normalizedPaths.length) {
    return 0;
  }

  const shouldInvalidate = (requestKey) => {
    let parsed;
    try {
      parsed = JSON.parse(requestKey);
    } catch {
      return false;
    }

    if (String(parsed?.method || '').trim().toUpperCase() !== 'GET') {
      return false;
    }

    if (tokenKey && getMetaTokenKey(parsed) !== tokenKey) {
      return false;
    }

    const requestPath = normalizeMetaGraphCachePath(parsed?.path);
    return normalizedPaths.some((path) => {
      if (!path) return false;
      return (
        requestPath === path ||
        requestPath.startsWith(`${path}/`) ||
        path.startsWith(`${requestPath}/`)
      );
    });
  };

  let removed = 0;
  for (const key of [...metaGraphRequestCache.keys()]) {
    if (shouldInvalidate(key)) {
      metaGraphRequestCache.delete(key);
      removed += 1;
    }
  }

  for (const key of [...metaGraphRequestInflight.keys()]) {
    if (shouldInvalidate(key)) {
      metaGraphRequestInflight.delete(key);
    }
  }

  return removed;
};

const resolveMetaCampaignCacheContext = async ({ campaignId, adSetId, adId, adAccountId } = {}) => {
  const resolvedCampaignId = String(campaignId || '').trim();
  const resolvedAdSetId = String(adSetId || '').trim();
  const resolvedAdId = String(adId || '').trim();
  const resolvedAdAccountId = toCanonicalAdAccountId(adAccountId || '');

  if (resolvedAdAccountId) {
    return {
      campaignId: resolvedCampaignId,
      adSetId: resolvedAdSetId,
      adId: resolvedAdId,
      adAccountId: resolvedAdAccountId
    };
  }

  const campaign = await Campaign.findOne({
    $or: [
      resolvedCampaignId ? { metaCampaignId: resolvedCampaignId } : null,
      resolvedAdSetId ? { metaAdSetId: resolvedAdSetId } : null,
      resolvedAdId ? { metaAdId: resolvedAdId } : null
    ].filter(Boolean)
  })
    .select('metaCampaignId metaAdSetId metaAdId adAccountId metaAdAccountId')
    .lean();

  return {
    campaignId: String(campaign?.metaCampaignId || resolvedCampaignId || '').trim(),
    adSetId: String(campaign?.metaAdSetId || resolvedAdSetId || '').trim(),
    adId: String(campaign?.metaAdId || resolvedAdId || '').trim(),
    adAccountId: toCanonicalAdAccountId(campaign?.adAccountId || campaign?.metaAdAccountId || '')
  };
};

const invalidateMetaCampaignCache = async ({
  accessToken,
  apiVersion,
  campaignId,
  adSetId,
  adId,
  adAccountId
} = {}) => {
  const cacheContext = await resolveMetaCampaignCacheContext({
    campaignId,
    adSetId,
    adId,
    adAccountId
  });

  const paths = [
    cacheContext.campaignId,
    cacheContext.adSetId,
    cacheContext.adId
  ];

  if (cacheContext.adAccountId) {
    paths.push(
      buildAdAccountPath(cacheContext.adAccountId, 'campaigns')
    );
  }

  return invalidateMetaGraphCacheEntries({
    accessToken,
    apiVersion: String(apiVersion || getEnvConfig().apiVersion || 'v23.0').trim(),
    paths
  });
};

const parseRetryAfterMs = (error) => {
  const rawHeader =
    error?.response?.headers?.['retry-after'] ||
    error?.response?.headers?.['Retry-After'] ||
    error?.response?.headers?.RetryAfter ||
    error?.response?.headers?.retryAfter;
  if (rawHeader === undefined || rawHeader === null || rawHeader === '') {
    return null;
  }

  const numeric = Number(rawHeader);
  if (Number.isFinite(numeric) && numeric > 0) {
    return numeric * 1000;
  }

  const parsedDate = Date.parse(String(rawHeader));
  if (Number.isFinite(parsedDate)) {
    return Math.max(0, parsedDate - Date.now());
  }

  return null;
};

const normalizeMetaRateLimitError = (error) => {
  const metaError = error?.response?.data?.error || error?.response?.data || {};
  const code = Number(metaError?.code);
  const errorSubcode = Number(metaError?.error_subcode);

  return {
    code: Number.isFinite(code) ? code : null,
    errorSubcode: Number.isFinite(errorSubcode) ? errorSubcode : null,
    message: String(metaError?.message || error?.message || '').trim(),
    retryAfterMs: parseRetryAfterMs(error),
    fbtrace_id: String(metaError?.fbtrace_id || '').trim()
  };
};

const isMetaRateLimitError = (error) => {
  const metaError = normalizeMetaRateLimitError(error);
  return metaError.code === 17 || metaError.errorSubcode === 2446079 || /too many api calls/i.test(metaError.message);
};

const getMetaRateLimitState = (tokenKey) => metaGraphRateLimits.get(tokenKey) || null;

const setMetaRateLimitState = ({ tokenKey, error, requestKey }) => {
  const current = getMetaRateLimitState(tokenKey) || {};
  const rateLimitError = normalizeMetaRateLimitError(error);
  const retryAfterMs = rateLimitError.retryAfterMs;
  const retryCount = Number(current.retryCount || 0) + 1;
  const backoffMs = Math.min(
    10 * 60 * 1000,
    META_GRAPH_BASE_BACKOFF_MS * Math.pow(2, Math.max(0, retryCount - 1))
  );
  const blockedForMs = retryAfterMs || backoffMs;
  const blockedUntil = new Date(Date.now() + blockedForMs);

  metaGraphRateLimits.set(tokenKey, {
    retryCount,
    blockedUntil,
    lastErrorAt: new Date(),
    lastRequestKey: requestKey || current.lastRequestKey || '',
    lastError: rateLimitError.message,
    code: rateLimitError.code,
    errorSubcode: rateLimitError.errorSubcode
  });

  metaGraphRequestStats.rateLimitEvents += 1;
  console.warn(
    '[Meta API Rate Limit]',
    JSON.stringify({
      tokenKey: tokenKey ? `${tokenKey.slice(0, 12)}...` : '',
      blockedUntil: blockedUntil.toISOString(),
      retryAfterMs: retryAfterMs || null,
      backoffMs,
      retryCount,
      code: rateLimitError.code,
      errorSubcode: rateLimitError.errorSubcode,
      requestKey: requestKey ? requestKey.slice(0, 120) : ''
    })
  );

  return metaGraphRateLimits.get(tokenKey);
};

const clearMetaRateLimitState = (tokenKey) => {
  if (metaGraphRateLimits.has(tokenKey)) {
    metaGraphRateLimits.delete(tokenKey);
  }
};

const getMetaQueueStatus = () => ({
  active: metaGraphRequestActiveCount,
  queued: metaGraphRequestQueue.length,
  inFlight: metaGraphRequestInflight.size
});

const logMetaGraphEvent = (event, payload = {}) => {
  console.log(
    '[Meta Graph]',
    JSON.stringify({
      event,
      ...payload,
      stats: { ...metaGraphRequestStats },
      queue: getMetaQueueStatus()
    })
  );
};

const drainMetaGraphRequestQueue = () => {
  while (metaGraphRequestActiveCount < META_GRAPH_MAX_CONCURRENCY && metaGraphRequestQueue.length) {
    const nextRequest = metaGraphRequestQueue.shift();
    metaGraphRequestActiveCount += 1;

    Promise.resolve()
      .then(nextRequest.run)
      .then(nextRequest.resolve, nextRequest.reject)
      .finally(() => {
        metaGraphRequestActiveCount = Math.max(0, metaGraphRequestActiveCount - 1);
        drainMetaGraphRequestQueue();
      });
  }
};

const enqueueMetaGraphRequest = (run) =>
  new Promise((resolve, reject) => {
    metaGraphRequestQueue.push({ run, resolve, reject });
    drainMetaGraphRequestQueue();
  });

const graphRequest = async ({
  method = 'GET',
  path,
  params,
  data,
  headers,
  accessToken: overrideToken,
  apiVersion: overrideApiVersion,
  returnResponse = false
}) => {
  const { apiVersion } = getEnvConfig();
  const resolvedAccessToken = String(overrideToken || '').trim();
  if (!resolvedAccessToken) {
    throw buildStageErrorWithDetails(
      'Meta access',
      'A user or admin Meta access token is required for this request.',
      { path: String(path || '').trim() },
      400
    );
  }

  const resolvedApiVersion = String(overrideApiVersion || apiVersion || 'v23.0').trim();
  const normalizedPath = String(path || '').replace(/^\/+/, '');
  const requestMethod = String(method || 'GET').trim().toUpperCase();
  const requestKey = buildMetaGraphRequestKey({
    method: requestMethod,
    path: normalizedPath,
    params,
    data,
    accessToken: resolvedAccessToken,
    apiVersion: resolvedApiVersion
  });
  const tokenKey = getMetaTokenKey({ accessToken: resolvedAccessToken, apiVersion: resolvedApiVersion });
  const cacheable = requestMethod === 'GET';
  const cachedResponse = cacheable ? getCachedMetaGraphResponse(requestKey) : null;
  const blockedState = getMetaRateLimitState(tokenKey);
  const blockedUntil = blockedState?.blockedUntil ? new Date(blockedState.blockedUntil) : null;
  const isBlocked = Boolean(blockedUntil && blockedUntil.getTime() > Date.now());

  if (cacheable && cachedResponse) {
    metaGraphRequestStats.cacheHits += 1;
    logMetaGraphEvent('cache-hit', {
      method: requestMethod,
      path: normalizedPath,
      tokenKey: tokenKey ? `${tokenKey.slice(0, 12)}...` : ''
    });
    return returnResponse ? cachedResponse : cachedResponse.data;
  }

  if (isBlocked) {
    metaGraphRequestStats.cooldownSkips += 1;
    logMetaGraphEvent('cooldown-skip', {
      method: requestMethod,
      path: normalizedPath,
      tokenKey: tokenKey ? `${tokenKey.slice(0, 12)}...` : '',
      blockedUntil: blockedUntil?.toISOString() || null
    });

    if (cacheable && cachedResponse) {
      return returnResponse ? cachedResponse : cachedResponse.data;
    }

    const blockedError = buildStageErrorWithDetails(
      'Meta rate limit',
      'Meta API requests are temporarily rate-limited. Please retry after the cooldown expires.',
      {
        code: blockedState?.code || 17,
        errorSubcode: blockedState?.errorSubcode || 2446079,
        blockedUntil: blockedUntil?.toISOString() || null,
        requestKey: requestKey.slice(0, 120)
      },
      429
    );
    blockedError.metaRateLimited = true;
    blockedError.metaRateLimit = {
      blockedUntil: blockedUntil?.toISOString() || null,
      code: blockedState?.code || 17,
      errorSubcode: blockedState?.errorSubcode || 2446079
    };
    throw blockedError;
  }

  if (metaGraphRequestInflight.has(requestKey)) {
    metaGraphRequestStats.duplicateSkips += 1;
    logMetaGraphEvent('duplicate-skip', {
      method: requestMethod,
      path: normalizedPath,
      tokenKey: tokenKey ? `${tokenKey.slice(0, 12)}...` : ''
    });
    return metaGraphRequestInflight.get(requestKey);
  }

  const url = `${GRAPH_BASE_URL}/${resolvedApiVersion}/${normalizedPath}`;
  const requestConfig = {
    url,
    method: requestMethod,
    params: {
      access_token: resolvedAccessToken,
      ...(params || {})
    },
    data,
    headers
  };

  const requestPromise = enqueueMetaGraphRequest(async () => {
    const currentBlockedState = getMetaRateLimitState(tokenKey);
    const currentBlockedUntil = currentBlockedState?.blockedUntil ? new Date(currentBlockedState.blockedUntil) : null;
    const stillBlocked = Boolean(currentBlockedUntil && currentBlockedUntil.getTime() > Date.now());

    if (stillBlocked) {
      if (cacheable) {
        const staleCachedResponse = getCachedMetaGraphResponse(requestKey);
        if (staleCachedResponse) {
          metaGraphRequestStats.cacheHits += 1;
          logMetaGraphEvent('cache-hit-stale', {
            method: requestMethod,
            path: normalizedPath,
            tokenKey: tokenKey ? `${tokenKey.slice(0, 12)}...` : '',
            blockedUntil: currentBlockedUntil.toISOString()
          });
          return returnResponse ? staleCachedResponse : staleCachedResponse.data;
        }
      }

      const blockedError = buildStageErrorWithDetails(
        'Meta rate limit',
        'Meta API requests are temporarily rate-limited. Please retry after the cooldown expires.',
        {
          code: currentBlockedState?.code || 17,
          errorSubcode: currentBlockedState?.errorSubcode || 2446079,
          blockedUntil: currentBlockedUntil?.toISOString() || null,
          requestKey: requestKey.slice(0, 120)
        },
        429
      );
      blockedError.metaRateLimited = true;
      blockedError.metaRateLimit = {
        blockedUntil: currentBlockedUntil?.toISOString() || null,
        code: currentBlockedState?.code || 17,
        errorSubcode: currentBlockedState?.errorSubcode || 2446079
      };
      throw blockedError;
    }

    metaGraphRequestStats.requestCount += 1;
    const requestId = ++metaGraphRequestSequence;
    logMetaGraphEvent('request-start', {
      requestId,
      method: requestMethod,
      path: normalizedPath,
      hasToken: Boolean(requestConfig.params.access_token),
      params: Object.keys(params || {})
    });

    try {
      const response = await axios(requestConfig);
      clearMetaRateLimitState(tokenKey);

      if (cacheable) {
        setCachedMetaGraphResponse(requestKey, response);
      }

      logMetaGraphEvent('request-success', {
        requestId,
        method: requestMethod,
        path: normalizedPath,
        status: response?.status || null
      });

      return returnResponse ? cloneMetaResponse(response) : cloneMetaValue(response.data);
    } catch (error) {
      const metaRateLimited = isMetaRateLimitError(error);
      const errorInfo = normalizeMetaRateLimitError(error);
      console.error(
        '[Meta API Error]',
        JSON.stringify({
          method: requestMethod,
          path: normalizedPath,
          message: errorInfo.message || error?.message || 'Meta API request failed',
          status: error?.response?.status || null,
          details: error?.response?.data || null
        })
      );

      if (metaRateLimited) {
        const rateLimitState = setMetaRateLimitState({
          tokenKey,
          error,
          requestKey
        });

        if (cacheable) {
          const staleCachedResponse = getCachedMetaGraphResponse(requestKey);
          if (staleCachedResponse) {
            logMetaGraphEvent('rate-limit-cache-hit', {
              requestId,
              method: requestMethod,
              path: normalizedPath,
              blockedUntil: rateLimitState?.blockedUntil?.toISOString() || null
            });
            return returnResponse ? staleCachedResponse : staleCachedResponse.data;
          }
        }

        const rateLimitError = buildStageErrorWithDetails(
          'Meta rate limit',
          'Meta API requests are temporarily rate-limited. Please retry after the cooldown expires.',
          {
            code: errorInfo.code || 17,
            errorSubcode: errorInfo.errorSubcode || 2446079,
            retryAfterMs: errorInfo.retryAfterMs,
            blockedUntil: rateLimitState?.blockedUntil?.toISOString() || null,
            requestKey: requestKey.slice(0, 120)
          },
          429
        );
        rateLimitError.metaRateLimited = true;
        rateLimitError.metaRateLimit = {
          blockedUntil: rateLimitState?.blockedUntil?.toISOString() || null,
          code: errorInfo.code || 17,
          errorSubcode: errorInfo.errorSubcode || 2446079,
          retryAfterMs: errorInfo.retryAfterMs || null
        };
        throw rateLimitError;
      }

      throw error;
    } finally {
      metaGraphRequestInflight.delete(requestKey);
    }
  });

  metaGraphRequestInflight.set(requestKey, requestPromise);
  return requestPromise;
};

const normalizePreviewPlacements = (placements = []) => {
  const requested = Array.isArray(placements)
    ? placements
    : String(placements || '')
        .split(',')
        .map((item) => String(item || '').trim())
        .filter(Boolean);

  const placementMap = new Map([
    ['facebook_feed', { key: 'facebook_feed', label: 'Facebook Feed', adFormat: 'MOBILE_FEED_STANDARD' }],
    ['facebook', { key: 'facebook_feed', label: 'Facebook Feed', adFormat: 'MOBILE_FEED_STANDARD' }],
    ['feed', { key: 'facebook_feed', label: 'Facebook Feed', adFormat: 'MOBILE_FEED_STANDARD' }],
    ['instagram_feed', { key: 'instagram_feed', label: 'Instagram Feed', adFormat: 'INSTAGRAM_STANDARD' }],
    ['instagram', { key: 'instagram_feed', label: 'Instagram Feed', adFormat: 'INSTAGRAM_STANDARD' }],
    ['story', { key: 'story', label: 'Story', adFormat: 'INSTAGRAM_STORY' }],
    ['instagram_story', { key: 'story', label: 'Story', adFormat: 'INSTAGRAM_STORY' }]
  ]);

  const resolved = requested
    .map((placement) => placementMap.get(String(placement).trim().toLowerCase()))
    .filter(Boolean);

  const uniqueResolved = resolved.filter(
    (placement, index, list) =>
      index === list.findIndex((item) => item.key === placement.key)
  );

  return uniqueResolved.length
    ? uniqueResolved
    : [
        placementMap.get('facebook_feed'),
        placementMap.get('instagram_feed'),
        placementMap.get('story')
      ];
};

const extractPreviewHtml = (responseData) => {
  const candidates = [];

  const pushCandidate = (value) => {
    if (typeof value === 'string' && value.trim()) {
      candidates.push(value.trim());
    }
  };

  pushCandidate(responseData?.body);
  pushCandidate(responseData?.html);
  pushCandidate(responseData?.iframe);
  pushCandidate(responseData?.preview);

  if (Array.isArray(responseData?.data)) {
    responseData.data.forEach((item) => {
      pushCandidate(item?.body);
      pushCandidate(item?.html);
      pushCandidate(item?.iframe);
      pushCandidate(item?.preview);
    });
  }

  if (typeof responseData === 'string') {
    pushCandidate(responseData);
  }

  for (const candidate of candidates) {
    if (/<iframe[\s>]/i.test(candidate) || /<body[\s>]/i.test(candidate) || /<html[\s>]/i.test(candidate)) {
      return candidate;
    }
  }

  return candidates[0] || '';
};

const extractApiErrorMessage = (error) => {
  return (
    error?.response?.data?.error?.message ||
    error?.response?.data?.message ||
    error?.message ||
    'Meta API request failed'
  );
};

const normalizeMetaApiError = (error) => {
  const metaError = error?.response?.data?.error || error?.response?.data || {};
  const code = Number(metaError?.code);
  const subcode = Number(metaError?.error_subcode);

  return {
    message: String(metaError?.message || error?.message || 'Meta API request failed'),
    type: String(metaError?.type || ''),
    code: Number.isFinite(code) ? code : null,
    error_subcode: Number.isFinite(subcode) ? subcode : null,
    fbtrace_id: String(metaError?.fbtrace_id || '')
  };
};

const logMetaApiRequest = ({
  stage,
  endpoint,
  adAccountId,
  payload,
  response,
  error
} = {}) => {
  const metaError = normalizeMetaApiError(error);
  console.log(
    '[Meta API Request]',
    JSON.stringify({
      stage: String(stage || '').trim(),
      endpoint: String(endpoint || '').trim(),
      normalizedAdAccountId: normalizeAdAccountId(adAccountId),
      requestPayload: payload || null,
      httpStatus: response?.status ?? error?.response?.status ?? null,
      metaResponse: response?.data ?? null,
      metaErrorCode: metaError.code,
      metaErrorSubcode: metaError.error_subcode,
      metaErrorMessage: metaError.message,
      fbtrace_id: metaError.fbtrace_id
    })
  );
};

const mapCrudObjectiveToMetaObjective = (objective) => {
  const normalizedObjective = String(objective || '').trim().toLowerCase();

  switch (normalizedObjective) {
    case 'traffic':
      return 'OUTCOME_TRAFFIC';
    case 'engagement':
      return 'OUTCOME_ENGAGEMENT';
    case 'leads':
      return 'OUTCOME_LEADS';
    case 'sales':
    case 'catalog':
      return 'OUTCOME_SALES';
    case 'awareness':
    default:
      return 'OUTCOME_AWARENESS';
  }
};

const validateCrudObjective = (objective) => {
  const normalizedObjective = String(objective || '').trim().toLowerCase();
  if (!['awareness', 'traffic', 'engagement', 'leads', 'sales', 'catalog'].includes(normalizedObjective)) {
    throw buildStageErrorWithDetails(
      'Campaign validation',
      'Invalid campaign objective. Allowed values: awareness, traffic, engagement, leads, sales, catalog.',
      { objective: String(objective || '') },
      400
    );
  }

  return normalizedObjective;
};

const getAllowedOptimizationGoalsForCrudObjective = (objective) => {
  const normalizedObjective = String(objective || '').trim().toLowerCase();

  switch (normalizedObjective) {
    case 'traffic':
      return ['LINK_CLICKS', 'LANDING_PAGE_VIEWS', 'REACH', 'IMPRESSIONS'];
    case 'engagement':
      return ['POST_ENGAGEMENT', 'REACH', 'IMPRESSIONS'];
    case 'leads':
      return ['LEAD_GENERATION', 'QUALITY_LEAD', 'CONVERSATIONS'];
    case 'sales':
      return ['OFFSITE_CONVERSIONS', 'VALUE', 'LINK_CLICKS'];
    case 'awareness':
    default:
      return ['REACH', 'IMPRESSIONS'];
  }
};

const normalizeOptimizationGoalForCrudObjective = (objective, optimizationGoal) => {
  const normalizedGoal = String(optimizationGoal || '').trim().toUpperCase();
  const aliasMap = {
    LEADS: 'LEAD_GENERATION'
  };
  const resolvedGoal = aliasMap[normalizedGoal] || normalizedGoal;
  const allowedOptimizationGoals = getAllowedOptimizationGoalsForCrudObjective(objective);

  if (allowedOptimizationGoals.includes(resolvedGoal)) {
    return resolvedGoal;
  }

  return getDefaultOptimizationGoalForCrudObjective(objective);
};

const getDefaultOptimizationGoalForCrudObjective = (objective) => {
  return getAllowedOptimizationGoalsForCrudObjective(objective)[0];
};

const buildStageError = (stage, error) => {
  const message = extractApiErrorMessage(error);
  const wrappedError = new Error(`${stage} failed: ${message}`);
  wrappedError.stage = stage;
  wrappedError.details = error?.response?.data || null;
  wrappedError.status = error?.response?.status || 500;
  wrappedError.metaError = normalizeMetaApiError(error);
  return wrappedError;
};

const buildStageErrorWithDetails = (stage, message, details, status = 400) => {
  const wrappedError = new Error(`${stage} failed: ${message}`);
  wrappedError.stage = stage;
  wrappedError.details = details || null;
  wrappedError.status = status;
  wrappedError.metaError = details?.metaError || null;
  return wrappedError;
};

const shouldUseMockMode = () => {
  const config = getEnvConfig();
  return config.forceMock;
};

const verifyMetaAdsManagementPermission = async ({ accessToken, appId, appSecret, apiVersion } = {}) => {
  const resolvedAccessToken = String(accessToken || '').trim();
  const resolvedAppId = String(appId || '').trim();
  const resolvedAppSecret = String(appSecret || '').trim();

  if (!resolvedAccessToken) {
    throw buildStageErrorWithDetails(
      'Meta token verification',
      'Meta access token is missing. Set META_ACCESS_TOKEN or FACEBOOK_ACCESS_TOKEN in the environment.',
      { envVar: 'META_ACCESS_TOKEN' },
      400
    );
  }

  if (!resolvedAppId || !resolvedAppSecret) {
    console.warn(
      '[Meta API] Skipping token permission verification because Meta app credentials are not configured.',
      JSON.stringify({
        hasAppId: Boolean(resolvedAppId),
        hasAppSecret: Boolean(resolvedAppSecret)
      })
    );

    return {
      isValid: true,
      skipped: true,
      reason: 'Meta app credentials are not configured for debug_token verification.'
    };
  }

  const appAccessToken = `${resolvedAppId}|${resolvedAppSecret}`;
  const debugResponse = await graphRequest({
    path: 'debug_token',
    params: {
      input_token: resolvedAccessToken
    },
    accessToken: appAccessToken,
    apiVersion
  });

  const debugData = debugResponse?.data || {};
  const scopes = [
    ...(Array.isArray(debugData.scopes) ? debugData.scopes : []),
    ...(Array.isArray(debugData.granular_scopes)
      ? debugData.granular_scopes.flatMap((item) => {
          if (Array.isArray(item?.scope)) return item.scope;
          if (item?.scope) return [item.scope];
          return [];
        })
      : [])
  ]
    .map((scope) => String(scope || '').trim())
    .filter(Boolean);

  if (!debugData.is_valid) {
    throw buildStageErrorWithDetails(
      'Meta token verification',
      'Meta access token is invalid or expired.',
      {
        debugToken: debugData
      },
      401
    );
  }

  if (!scopes.includes('ads_management')) {
    throw buildStageErrorWithDetails(
      'Meta token verification',
      'Meta access token is missing the required ads_management permission.',
      {
        scopes,
        debugToken: debugData
      },
      403
    );
  }

  return {
    isValid: true,
    scopes,
    debugToken: debugData
  };
};

const getAccessContextForUser = async (userId) => metaAuthService.getAccessContextForUser(userId);

const createMetaCampaignInAdsManager = async ({ name, objective, adAccountId, accessToken, apiVersion, userId } = {}) => {
  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      id: `mock-campaign-${Date.now()}`,
      name: String(name || 'Campaign').trim(),
      objective: mapCrudObjectiveToMetaObjective(objective),
      status: 'PAUSED',
      effective_status: 'PAUSED'
    };
  }

  const env = resolveMetaCampaignEnvConfig();
  const accountSelection = await resolveMetaAdAccountSelection({
    userId,
    adAccountId,
    accessToken: accessToken || env.accessToken,
    apiVersion: apiVersion || env.apiVersion,
    stage: 'Campaign creation'
  });
  const resolvedAdAccountId = accountSelection.selectedAdAccountId;
  const resolvedAccessToken = String(accountSelection.accessToken || env.accessToken || '').trim();
  const resolvedApiVersion = String(accountSelection.apiVersion || env.apiVersion || 'v23.0').trim();

  if (!resolvedAccessToken) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Missing Meta access token. Set META_ACCESS_TOKEN or FACEBOOK_ACCESS_TOKEN in the environment.',
      { envVar: 'META_ACCESS_TOKEN' },
      400
    );
  }

  validateCrudObjective(objective);
  const authConfig = resolveMetaCampaignAuthConfig(accountSelection.accessContext);
  await verifyMetaAdsManagementPermission({
    accessToken: resolvedAccessToken,
    appId: authConfig.appId,
    appSecret: authConfig.appSecret,
    apiVersion: resolvedApiVersion
  });

  try {
    const response = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(resolvedAdAccountId, 'campaigns'),
      data: {
        name: String(name || 'Campaign').trim(),
        objective: mapCrudObjectiveToMetaObjective(objective),
        status: 'PAUSED',
        special_ad_categories: [],
        is_adset_budget_sharing_enabled: false
      },
      accessToken: resolvedAccessToken,
      apiVersion: resolvedApiVersion
    });

    await invalidateMetaCampaignCache({
      accessToken: resolvedAccessToken,
      apiVersion: resolvedApiVersion,
      campaignId: response?.id,
      adAccountId: resolvedAdAccountId
    });

    return {
      apiMode: 'live',
      adAccountId: toCanonicalAdAccountId(resolvedAdAccountId),
      availableAdAccounts: accountSelection.availableAdAccounts,
      metaStatus: String(response?.status || 'PAUSED').trim().toUpperCase() || 'PAUSED',
      ...response
    };
  } catch (error) {
    throw buildStageError('Campaign creation', error);
  }
};

const fetchMetaCampaignsFromAdsManager = async ({ adAccountId, accessToken, apiVersion, limit = 100, userId } = {}) => {
  if (shouldUseMockMode()) {
    return [];
  }

  const env = resolveMetaCampaignEnvConfig();
  const accountSelection = await resolveMetaAdAccountSelection({
    userId,
    adAccountId,
    accessToken: accessToken || env.accessToken,
    apiVersion: apiVersion || env.apiVersion,
    stage: 'Campaign sync'
  });
  const resolvedAdAccountId = accountSelection.selectedAdAccountId;
  const resolvedAccessToken = String(accountSelection.accessToken || env.accessToken || '').trim();
  const resolvedApiVersion = String(accountSelection.apiVersion || env.apiVersion || 'v23.0').trim();

  if (!resolvedAccessToken) {
    throw buildStageErrorWithDetails(
      'Campaign sync',
      'Missing Meta access token. Set META_ACCESS_TOKEN or FACEBOOK_ACCESS_TOKEN in the environment.',
      { envVar: 'META_ACCESS_TOKEN' },
      400
    );
  }

  const authConfig = resolveMetaCampaignAuthConfig(accountSelection.accessContext);
  await verifyMetaAdsManagementPermission({
    accessToken: resolvedAccessToken,
    appId: authConfig.appId,
    appSecret: authConfig.appSecret,
    apiVersion: resolvedApiVersion
  });

  const response = await graphRequest({
    path: buildAdAccountPath(resolvedAdAccountId, 'campaigns'),
    params: {
      fields: 'id,name,status,effective_status,objective,created_time,updated_time',
      limit
    },
    accessToken: resolvedAccessToken,
    apiVersion: resolvedApiVersion
  });

  return Array.isArray(response?.data) ? response.data : [];
};

const deleteMetaCampaignInAdsManager = async ({ campaignId, accessToken, apiVersion } = {}) => {
  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      deleted: Boolean(campaignId)
    };
  }

  const env = resolveMetaCampaignEnvConfig();
  const resolvedCampaignId = String(campaignId || '').trim();
  const resolvedAccessToken = String(accessToken || env.accessToken || '').trim();
  const resolvedApiVersion = String(apiVersion || env.apiVersion || 'v23.0').trim();

  if (!resolvedCampaignId) {
    return {
      skipped: true,
      reason: 'No Meta campaign ID was provided.'
    };
  }

  if (!resolvedAccessToken) {
    throw buildStageErrorWithDetails(
      'Campaign rollback',
      'Missing Meta access token. Set META_ACCESS_TOKEN or FACEBOOK_ACCESS_TOKEN in the environment.',
      { envVar: 'META_ACCESS_TOKEN' },
      400
    );
  }

  try {
    await graphRequest({
      method: 'DELETE',
      path: resolvedCampaignId,
      accessToken: resolvedAccessToken,
      apiVersion: resolvedApiVersion
    });

    await invalidateMetaCampaignCache({
      accessToken: resolvedAccessToken,
      apiVersion: resolvedApiVersion,
      campaignId: resolvedCampaignId
    });

    return {
      apiMode: 'live',
      deleted: true,
      id: resolvedCampaignId
    };
  } catch (error) {
    throw buildStageError('Campaign rollback', error);
  }
};

const ensureConnectedMetaUser = async (userId, stage = 'Meta access') => {
  const accessContext = await getAccessContextForUser(userId);

  if (shouldUseMockMode()) {
    return accessContext;
  }

  if (!userId || !accessContext?.accessToken || !['user', 'admin'].includes(accessContext.source)) {
    throw buildStageErrorWithDetails(
      stage,
      'Meta access is not configured for this admin.',
      { userId: userId || '', authSource: accessContext?.source || 'none' },
      400
    );
  }

  return accessContext;
};

const buildTargeting = (targeting = {}) => {
  const env = getEnvConfig();
  const countries = normalizeArray(targeting.countries);
  const genders = normalizeArray(targeting.genders).map((value) => Number(value)).filter(Boolean);
  const interests = normalizeArray(targeting.interests)
    .map((interest) => ({
      id: String(interest.id || '').trim(),
      name: String(interest.name || '').trim()
    }))
    .filter((interest) => /^\d+$/.test(interest.id) && interest.name);
  const behaviors = normalizeArray(targeting.behaviors)
    .map((behavior) => ({
      id: String(behavior.id || '').trim(),
      name: String(behavior.name || '').trim()
    }))
    .filter((behavior) => /^\d+$/.test(behavior.id) && behavior.name);
  const customAudienceIds = normalizeArray(targeting.customAudienceIds);

  const result = {
    geo_locations: {
      countries: countries.length ? countries : ['IN']
    },
    age_min: Number(targeting.ageMin || 21),
    age_max: Number(targeting.ageMax || 45),
    targeting_automation: {
      advantage_audience:
        targeting.advantageAudience === 1 || targeting.advantageAudience === 0
          ? Number(targeting.advantageAudience)
          : env.advantageAudience
    }
  };

  if (genders.length) {
    result.genders = genders;
  }
  if (interests.length || behaviors.length) {
    const flexibleEntry = {};
    if (interests.length) flexibleEntry.interests = interests;
    if (behaviors.length) flexibleEntry.behaviors = behaviors;
    result.flexible_spec = [flexibleEntry];
  }
  if (customAudienceIds.length) {
    result.custom_audiences = customAudienceIds.map((id) => ({ id }));
  }

  return result;
};

const buildPlacement = (placement = {}, platform = 'both') => {
  const publisherPlatforms = normalizeArray(placement.publisherPlatforms);
  const facebookPositions = normalizeArray(placement.facebookPositions);
  const instagramPositions = normalizeArray(placement.instagramPositions);
  const normalizedPlatform = String(platform || 'both').trim().toLowerCase();

  if (normalizedPlatform === 'facebook') {
    return {
      publisher_platforms: ['facebook'],
      facebook_positions: facebookPositions.length ? facebookPositions : ['feed', 'marketplace', 'video_feeds']
    };
  }
  if (normalizedPlatform === 'instagram') {
    return {
      publisher_platforms: ['instagram'],
      instagram_positions: instagramPositions.length ? instagramPositions : ['stream', 'story', 'reels']
    };
  }

  return {
    publisher_platforms: publisherPlatforms.length ? publisherPlatforms : ['facebook', 'instagram'],
    facebook_positions: facebookPositions.length ? facebookPositions : ['feed', 'marketplace', 'video_feeds'],
    instagram_positions: instagramPositions.length ? instagramPositions : ['stream', 'story', 'reels']
  };
};

const findBestTargetingMatch = (entries = [], term = '') => {
  const normalizedTerm = String(term || '').trim().toLowerCase();
  if (!normalizedTerm) return null;

  const exact = entries.find(
    (entry) => String(entry?.name || '').trim().toLowerCase() === normalizedTerm
  );
  if (exact) return exact;

  return entries.find((entry) =>
    String(entry?.name || '').trim().toLowerCase().includes(normalizedTerm)
  ) || entries[0] || null;
};

const resolveMetaTargetingEntries = async ({ accessToken, terms = [], type = 'adinterest', extraParams = {} }) => {
  const results = [];
  const seenIds = new Set();

  for (const term of terms) {
    try {
      const response = await graphRequest({
        path: 'search',
        params: {
          type,
          q: term,
          limit: 10,
          ...extraParams
        },
        accessToken
      });

      const entries = Array.isArray(response?.data) ? response.data : [];
      const best = findBestTargetingMatch(entries, term);
      const id = String(best?.id || '').trim();
      const name = String(best?.name || term).trim();
      if (!id || seenIds.has(id)) continue;

      seenIds.add(id);
      results.push({ id, name });
    } catch (error) {
      console.warn(
        '[Meta Ads] Targeting lookup failed:',
        JSON.stringify({ term, type, message: extractApiErrorMessage(error) })
      );
    }
  }

  return results;
};

const sanitizeWhatsappNumber = metaCreativeService.sanitizeWhatsappNumber;
const buildCreativeDestination = metaCreativeService.buildCreativeDestination;
const getAccessiblePages = async ({ accessToken }) =>
  metaCreativeService.getAccessiblePages({ accessToken, graphRequest });
const resolveCreativePageContext = async ({ requestedPageId, accessToken }) =>
  metaCreativeService.resolveCreativePageContext({
    requestedPageId,
    accessToken,
    graphRequest,
    env: getEnvConfig(),
    buildStageErrorWithDetails
  });
const uploadCreativeAsset = async ({ fileBuffer, fileName, mediaUrl, mediaType, userId, adAccountId }) =>
  metaCreativeService.uploadCreativeAsset({
    fileBuffer,
    fileName,
    mediaUrl,
    mediaType,
    userId,
    adAccountId,
    shouldUseMockMode,
    getAccessContextForUser,
    getEnvConfig,
    graphRequest,
    buildAdAccountPath,
    buildStageErrorWithDetails,
    extractApiErrorMessage
  });

const getSetupBundle = async ({ userId } = {}) => {
  if (shouldUseMockMode()) {
    const connection = userId ? await MetaAdsConnection.findOne({ userId }).lean() : null;
    return {
      mode: 'mock',
      connected: true,
      adAccountId: connection?.selectedAdAccountId || 'act_mock_account',
      selectedAdAccountId: toCanonicalAdAccountId(connection?.selectedAdAccountId || 'act_mock_account'),
      pageId: connection?.selectedPageId || 'mock-page',
      selectedPageId: connection?.selectedPageId || 'mock-page',
      selectedWhatsappNumber: connection?.selectedWhatsappNumber || '',
      linkedWhatsappNumber: connection?.selectedWhatsappNumber || '',
      pages: [
        { id: connection?.selectedPageId || '615785750230178', name: 'Technovo Demo Page' }
      ],
      availablePages: [
        { id: connection?.selectedPageId || '615785750230178', name: 'Technovo Demo Page' }
      ],
      businesses: [
        { id: 'mock-business-1', name: 'Technovo Demo Business' }
      ],
      adAccounts: [
        { id: connection?.selectedAdAccountId || 'act_mock_account', name: 'Technovo Demo Ad Account' }
      ],
      availableAdAccounts: [
        { id: toCanonicalAdAccountId(connection?.selectedAdAccountId || 'act_mock_account'), name: 'Technovo Demo Ad Account' }
      ],
      whatsappNumbers: [
        { id: 'mock-waba-1', display_phone_number: connection?.selectedWhatsappNumber || '+91 98765 43210' }
      ],
      availableWhatsappNumbers: [
        { id: 'mock-waba-1', display_phone_number: connection?.selectedWhatsappNumber || '+91 98765 43210' }
      ]
    };
  }

  const accessContext = await getAccessContextForUser(userId);
  if (!userId || !accessContext?.accessToken || !['user', 'admin'].includes(accessContext.source)) {
    const setupError =
      accessContext?.source === 'user-token-invalid'
        ? 'Stored Meta token could not be decrypted in this backend environment. Use the same backend for OAuth + dashboard, or keep META_TOKEN_ENCRYPTION_KEY/JWT_SECRET consistent, then reconnect Meta.'
        : 'Meta access is not configured for this admin.';

    return {
      mode: 'disconnected',
      connected: false,
      adAccountId: '',
      selectedAdAccountId: '',
      pageId: '',
      selectedPageId: '',
      selectedWhatsappNumber: '',
      linkedWhatsappNumber: '',
      pages: [],
      availablePages: [],
      businesses: [],
      adAccounts: [],
      availableAdAccounts: [],
      whatsappNumbers: [],
      availableWhatsappNumbers: [],
      setupError,
      authSource: accessContext?.source || 'none',
      profileName: ''
    };
  }

  const warnings = [];
  const savedSelection = accessContext.connection || {};

  const [pagesResult, businessesResult, adAccountsResult, pageDetailsResult] = await Promise.allSettled([
    graphRequest({
      path: 'me/accounts',
      params: { fields: 'id,name,access_token,instagram_business_account{id,username}' },
      accessToken: accessContext.accessToken
    }),
    graphRequest({
      path: 'me/businesses',
      params: { fields: 'id,name' },
      accessToken: accessContext.accessToken
    }),
    graphRequest({
      path: 'me/adaccounts',
      params: { fields: 'id,name,account_status,currency,amount_spent' },
      accessToken: accessContext.accessToken
    }),
    savedSelection.selectedPageId
      ? graphRequest({
          path: savedSelection.selectedPageId,
          params: { fields: 'id,name,whatsapp_business_account{id,name,phone_numbers{display_phone_number,id}}' },
          accessToken: accessContext.accessToken
        })
      : Promise.resolve(null)
  ]);

  const pages =
    pagesResult.status === 'fulfilled' && Array.isArray(pagesResult.value?.data)
      ? pagesResult.value.data
      : [];
  if (pagesResult.status === 'rejected') {
    warnings.push(`Pages: ${extractApiErrorMessage(pagesResult.reason)}`);
  }

  const businesses =
    businessesResult.status === 'fulfilled' && Array.isArray(businessesResult.value?.data)
      ? businessesResult.value.data
      : [];
  if (businessesResult.status === 'rejected') {
    warnings.push(`Businesses: ${extractApiErrorMessage(businessesResult.reason)}`);
  }

  const adAccounts =
    adAccountsResult.status === 'fulfilled' && Array.isArray(adAccountsResult.value?.data)
      ? adAccountsResult.value.data.map(normalizeMetaAdAccountRecord)
      : [];
  if (adAccountsResult.status === 'rejected') {
    warnings.push(`Ad accounts: ${extractApiErrorMessage(adAccountsResult.reason)}`);
  }

  const fallbackAccessiblePageId = String(pages[0]?.id || '').trim();
  const requestedPageId = String(savedSelection.selectedPageId || fallbackAccessiblePageId || '').trim();
  const accessiblePageIds = new Set(pages.map((page) => String(page?.id || '').trim()).filter(Boolean));
  let selectedPageRecord =
    (requestedPageId && pages.find((page) => String(page?.id || '').trim() === requestedPageId)) ||
    pages[0] ||
    null;
  let selectedPageId =
    (requestedPageId && accessiblePageIds.has(requestedPageId) ? requestedPageId : '') ||
    fallbackAccessiblePageId ||
    '';
  const selectedAdAccountId = toCanonicalAdAccountId(savedSelection.selectedAdAccountId || '');
  let whatsappNumbers = [];

  if (pageDetailsResult.status === 'fulfilled' && pageDetailsResult.value) {
    whatsappNumbers =
      pageDetailsResult.value?.whatsapp_business_account?.phone_numbers?.data ||
      pageDetailsResult.value?.whatsapp_business_account?.phone_numbers ||
      [];
    selectedPageId = String(pageDetailsResult.value?.id || selectedPageId || '').trim();
    selectedPageRecord =
      pages.find((page) => String(page?.id || '').trim() === selectedPageId) ||
      selectedPageRecord ||
      null;
  } else if (pageDetailsResult.status === 'rejected') {
    warnings.push(`Page details: ${extractApiErrorMessage(pageDetailsResult.reason)}`);
  }

  const selectedPageName = String(
    selectedPageRecord?.name ||
      savedSelection.selectedPageName ||
      pageDetailsResult.value?.name ||
      ''
  ).trim();
  const selectedPageAccessToken = String(
    selectedPageRecord?.access_token ||
      decryptMetaToken(savedSelection.selectedPageAccessToken || '') ||
      ''
  ).trim();

  if (selectedPageId && !whatsappNumbers.length && (!pageDetailsResult.value || String(pageDetailsResult.value?.id || '').trim() !== selectedPageId)) {
    try {
      const selectedPageDetails = await graphRequest({
        path: selectedPageId,
        params: { fields: 'id,name,whatsapp_business_account{id,name,phone_numbers{display_phone_number,id}}' },
        accessToken: accessContext.accessToken
      });

      whatsappNumbers =
        selectedPageDetails?.whatsapp_business_account?.phone_numbers?.data ||
        selectedPageDetails?.whatsapp_business_account?.phone_numbers ||
        [];
    } catch (error) {
      warnings.push(`Selected page details: ${extractApiErrorMessage(error)}`);
    }
  }

  const storedSelectedPageToken = decryptMetaToken(savedSelection.selectedPageAccessToken || '');
  const shouldPersistPageSelection =
    Boolean(userId && selectedPageId) &&
    (
      selectedPageId !== String(savedSelection.selectedPageId || '').trim() ||
      selectedPageName !== String(savedSelection.selectedPageName || '').trim() ||
      (selectedPageAccessToken && selectedPageAccessToken !== storedSelectedPageToken)
    );

  if (userId && shouldPersistPageSelection) {
    await MetaAdsConnection.updateOne(
      { userId },
      {
        $set: {
          selectedPageId,
          selectedPageName,
          selectedPageAccessToken: selectedPageAccessToken ? encryptMetaToken(selectedPageAccessToken) : String(savedSelection.selectedPageAccessToken || ''),
          lastValidatedAt: new Date()
        }
      }
    );
  }

  if (!selectedPageId && requestedPageId) {
    warnings.push('Page access: Reconnect Facebook and grant page access so a valid Facebook Page can be used for ad creatives.');
  }

  if (pages.length) {
    console.log(
      '[Meta OAuth] Page assets loaded',
      JSON.stringify({
        userId: String(userId || ''),
        pagesCount: pages.length,
        pages: pages.map(summarizePage)
      })
    );
  }

  const hasConnectedAuth = Boolean(accessContext?.accessToken && ['user', 'admin'].includes(accessContext.source));
  const hasAnyLiveData = Boolean(
    pages.length || businesses.length || adAccounts.length || whatsappNumbers.length || selectedAdAccountId
  );
  if (hasAnyLiveData) {
    return {
      mode: warnings.length ? 'live-partial' : 'live',
      connected: true,
      adAccountId: selectedAdAccountId,
      selectedAdAccountId,
      pageId: selectedPageId,
      selectedPageId,
      selectedPageName,
      pageAccessReady: Boolean(selectedPageAccessToken),
      selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || whatsappNumbers[0]?.display_phone_number || '',
      linkedWhatsappNumber: savedSelection.selectedWhatsappNumber || whatsappNumbers[0]?.display_phone_number || '',
      pages: pages.map(summarizePage),
      availablePages: pages.map(summarizePage),
      businesses,
      adAccounts,
      availableAdAccounts: adAccounts,
      whatsappNumbers,
      availableWhatsappNumbers: whatsappNumbers,
      setupError: warnings.join(' | '),
      authSource: accessContext.source,
      profileName: accessContext.connection?.name || ''
    };
  }

  if (hasConnectedAuth) {
    const setupWarning =
      warnings.join(' | ') ||
      'Meta account is connected, but ad accounts/pages could not be loaded. Reconnect and grant required Meta permissions.';

    return {
      mode: 'live-partial',
      connected: true,
      adAccountId: savedSelection.selectedAdAccountId || '',
      selectedAdAccountId: toCanonicalAdAccountId(savedSelection.selectedAdAccountId || ''),
      pageId: savedSelection.selectedPageId || '',
      selectedPageId: savedSelection.selectedPageId || '',
      selectedPageName: savedSelection.selectedPageName || '',
      pageAccessReady: Boolean(decryptMetaToken(savedSelection.selectedPageAccessToken || '')),
      selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || '',
      linkedWhatsappNumber: savedSelection.selectedWhatsappNumber || '',
      pages: pages.map(summarizePage),
      availablePages: pages.map(summarizePage),
      businesses,
      adAccounts,
      availableAdAccounts: adAccounts,
      whatsappNumbers,
      availableWhatsappNumbers: whatsappNumbers,
      setupError: setupWarning,
      authSource: accessContext.source,
      profileName: accessContext.connection?.name || ''
    };
  }

  const fallback = {
    mode: 'disconnected',
    connected: false,
    adAccountId: savedSelection.selectedAdAccountId || '',
    pageId: savedSelection.selectedPageId || '',
    selectedPageName: savedSelection.selectedPageName || '',
    pageAccessReady: Boolean(decryptMetaToken(savedSelection.selectedPageAccessToken || '')),
    selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || '',
    pages: pages.map(summarizePage),
    businesses,
    adAccounts,
    whatsappNumbers,
      setupError: warnings.join(' | ') || 'Meta setup could not be loaded for this admin.',
      authSource: accessContext.source,
      profileName: accessContext.connection?.name || ''
  };

  console.warn('Meta Ads setup fallback enabled:', fallback.setupError);
  return fallback;
};

const getPageLeads = async ({ userId, formId = '', limit = 25 } = {}) => {
  const accessContext = await getAccessContextForUser(userId);
  const selectedPageId = String(accessContext?.connection?.selectedPageId || '').trim();
  const resolvedFormId = String(formId || accessContext?.adminMetaConfig?.leadFormId || '').trim();
  const pageAccessToken = String(
    accessContext?.adminMetaConfig?.pageAccessToken ||
      decryptMetaToken(accessContext?.connection?.selectedPageAccessToken || '') ||
      ''
  ).trim();

  if (!pageAccessToken) {
    const error = new Error('A Facebook Page access token is required to load leads.');
    error.status = 400;
    throw error;
  }

  if (!resolvedFormId) {
    const error = new Error('A Meta lead form ID is required to load leads.');
    error.status = 400;
    throw error;
  }

  const fetchLeadForms = async () => {
    if (!selectedPageId) return [];
    try {
      const response = await graphRequest({
        path: `${selectedPageId}/leadgen_forms`,
        params: {
          fields: 'id,name,created_time',
          limit: 100
        },
        accessToken: pageAccessToken
      });
      return Array.isArray(response?.data) ? response.data : [];
    } catch (error) {
      console.warn('[Meta Leads] Failed to load lead forms:', error?.message || error);
      return [];
    }
  };

  const isInvalidLeadFormError = (error = {}) => {
    const code = Number(error?.response?.data?.error?.code || error?.response?.data?.code || error?.code || 0);
    const subcode = Number(error?.response?.data?.error?.error_subcode || error?.response?.data?.error_subcode || 0);
    const message = String(
      error?.response?.data?.error?.message ||
        error?.response?.data?.message ||
        error?.message ||
        ''
    ).toLowerCase();
    return (
      code === 100 &&
      (
        subcode === 33 ||
        message.includes('nonexisting field (leads)') ||
        message.includes('unsupported get request') ||
        message.includes('does not exist')
      )
    );
  };

  const tryFetchLeadsForForm = async (candidateFormId) =>
    graphRequest({
      path: `${candidateFormId}/leads`,
      params: {
        fields: 'id,created_time,field_data,ad_id,form_id,campaign_id',
        limit: Math.max(1, Math.min(Number(limit) || 25, 100))
      },
      accessToken: pageAccessToken
    });

  const leadForms = await fetchLeadForms();
  const fallbackFormIds = leadForms
    .map((form) => ({
      id: String(form?.id || '').trim(),
      createdTime: String(form?.created_time || '').trim()
    }))
    .filter((form) => form.id)
    .sort((left, right) => new Date(right.createdTime || 0).getTime() - new Date(left.createdTime || 0).getTime())
    .map((form) => form.id);

  const candidateFormIds = Array.from(new Set([resolvedFormId, ...fallbackFormIds].filter(Boolean)));

  console.log('[Meta Leads] Lead form resolution context:', {
    selectedPageId,
    requestedFormId: resolvedFormId,
    candidateFormIds
  });

  let response = null;
  let effectiveFormId = '';
  let lastError = null;

  for (const candidateFormId of candidateFormIds) {
    try {
      response = await tryFetchLeadsForForm(candidateFormId);
      effectiveFormId = candidateFormId;
      break;
    } catch (error) {
      lastError = error;
      if (!isInvalidLeadFormError(error)) {
        throw error;
      }
      console.warn('[Meta Leads] Invalid lead form id, trying next candidate:', candidateFormId);
    }
  }

  if (!response) {
    throw lastError || new Error('Unable to resolve a valid Meta lead form ID.');
  }

  const leads = Array.isArray(response?.data) ? response.data : [];

  return {
    formId: resolvedFormId,
    resolvedFormId: effectiveFormId,
    leads,
    paging: response?.paging || null
  };
};

const toCheckResult = (result, mapSuccess) => {
  if (result.status === 'fulfilled') {
    const mapped = mapSuccess ? mapSuccess(result.value) : result.value;
    return {
      ok: true,
      ...mapped
    };
  }

  return {
    ok: false,
    error: extractApiErrorMessage(result.reason)
  };
};

const getConnectionDiagnostics = async ({ userId } = {}) => {
  const env = getEnvConfig();
  const accessContext = await getAccessContextForUser(userId);
  const selectedPageId = String(accessContext?.connection?.selectedPageId || '').trim();
  const selectedAdAccountId = toCanonicalAdAccountId(accessContext?.connection?.selectedAdAccountId || '');

  if (!userId || !accessContext?.accessToken || !['user', 'admin'].includes(accessContext.source)) {
    const disconnectedError =
      accessContext?.source === 'user-token-invalid'
        ? 'Stored Meta token could not be decrypted in this backend environment. Reconnect Meta after aligning META_TOKEN_ENCRYPTION_KEY/JWT_SECRET across environments.'
        : 'Meta access is not configured for this admin.';

    return {
      env: {
        apiVersion: env.apiVersion,
        hasAccessToken: false,
        hasAdAccountId: false,
        hasPageId: false,
        hasPageAccessToken: false,
        forceMock: env.forceMock,
        authSource: accessContext?.source || 'none',
        connectedProfileName: ''
      },
      checks: {
        profile: { ok: false, error: disconnectedError },
        businesses: { ok: false, error: disconnectedError },
        pages: { ok: false, error: disconnectedError },
        pageDetails: { ok: false, error: 'No page selected for this admin.' },
        adAccount: { ok: false, error: 'No ad account selected for this admin.' }
      },
      warnings: [disconnectedError],
      summary: {
        healthy: false,
        mode: 'disconnected',
        accessiblePages: 0,
        accessibleBusinesses: 0
      },
      targets: {
        pageId: '',
        adAccountId: '',
        apiVersion: env.apiVersion,
        graphBaseUrl: GRAPH_BASE_URL
      }
    };
  }

  const checks = await Promise.allSettled([
    graphRequest({
      path: 'me',
      params: { fields: 'id,name' },
      accessToken: accessContext.accessToken
    }),
    graphRequest({
      path: 'me/businesses',
      params: { fields: 'id,name' },
      accessToken: accessContext.accessToken
    }),
    graphRequest({
      path: 'me/accounts',
      params: { fields: 'id,name,instagram_business_account{id,username}' },
      accessToken: accessContext.accessToken
    }),
    selectedPageId
      ? graphRequest({
          path: selectedPageId,
          params: { fields: 'id,name,whatsapp_business_account{id,name,phone_numbers{display_phone_number,id}}' },
          accessToken: accessContext.accessToken
        })
      : Promise.reject(new Error('No Meta page selected for this user')),
    selectedAdAccountId
      ? graphRequest({
          path: buildAdAccountPath(selectedAdAccountId),
          params: { fields: 'id,name,account_status,currency,timezone_name' },
          accessToken: accessContext.accessToken
        })
      : Promise.reject(new Error('No Meta ad account selected for this user'))
  ]);

  const [meResult, businessesResult, pagesResult, pageDetailsResult, adAccountResult] = checks;

  const diagnostics = {
    env: {
      apiVersion: env.apiVersion,
      hasAccessToken: Boolean(accessContext.accessToken),
      hasAdAccountId: Boolean(selectedAdAccountId),
      hasPageId: Boolean(selectedPageId),
      hasPageAccessToken: Boolean(decryptMetaToken(accessContext?.connection?.selectedPageAccessToken || '')),
      forceMock: env.forceMock,
      authSource: accessContext.source,
      connectedProfileName: accessContext.connection?.name || ''
    },
    checks: {
      profile: toCheckResult(meResult, (value) => ({
        data: value || null
      })),
      businesses: toCheckResult(businessesResult, (value) => ({
        count: Array.isArray(value?.data) ? value.data.length : 0,
        data: value?.data || []
      })),
      pages: toCheckResult(pagesResult, (value) => ({
        count: Array.isArray(value?.data) ? value.data.length : 0,
        data: value?.data || []
      })),
      pageDetails: toCheckResult(pageDetailsResult, (value) => ({
        data: value || null
      })),
      adAccount: toCheckResult(adAccountResult, (value) => ({
        data: value || null
      }))
    }
  };

  const warnings = [];
  Object.entries(diagnostics.checks).forEach(([key, value]) => {
    if (!value.ok) {
      warnings.push(`${key}: ${value.error}`);
    }
  });

  diagnostics.warnings = warnings;
  diagnostics.summary = {
    healthy: warnings.length === 0,
    mode: warnings.length === 0 ? 'live' : 'live-diagnostics',
    accessiblePages: diagnostics.checks.pages.count || 0,
    accessibleBusinesses: diagnostics.checks.businesses.count || 0
  };

  diagnostics.targets = {
    pageId: selectedPageId,
    pageAccessReady: Boolean(decryptMetaToken(accessContext?.connection?.selectedPageAccessToken || '')),
    adAccountId: selectedAdAccountId,
    apiVersion: env.apiVersion,
    graphBaseUrl: GRAPH_BASE_URL
  };

  return diagnostics;
};

const getAdPreviews = async ({ userId, adId, placements = [] } = {}) => {
  if (shouldUseMockMode()) {
    return {
      adId: String(adId || '').trim(),
      previews: normalizePreviewPlacements(placements).map((placement) => ({
        key: placement.key,
        label: placement.label,
        adFormat: placement.adFormat,
        html:
          `<div style="font-family:Arial,sans-serif;padding:24px;border:1px solid #dbe4f0;border-radius:16px;">` +
          `<strong>${placement.label}</strong><p>Mock preview is enabled. Connect live Meta credentials to render the real iframe.</p></div>`,
        source: 'mock'
      })),
      meta: { source: 'mock' }
    };
  }

  const accessContext = await ensureConnectedMetaUser(userId, 'Ad preview');
  const resolvedAdId = String(adId || '').trim();
  if (!/^\d+$/.test(resolvedAdId)) {
    throw buildStageErrorWithDetails(
      'Ad preview',
      'A valid Meta Ad ID is required. Use the numeric ad.id returned by Meta, not a campaign ID.',
      { adId: resolvedAdId },
      400
    );
  }

  const authConfig = resolveMetaCampaignAuthConfig(accessContext);
  await verifyMetaAdsManagementPermission({
    accessToken: accessContext.accessToken,
    appId: authConfig.appId,
    appSecret: authConfig.appSecret,
    apiVersion: accessContext.apiVersion
  });

  const previewPlacements = normalizePreviewPlacements(placements);
  const previews = [];

  for (const placement of previewPlacements) {
    try {
      const response = await graphRequest({
        path: `${resolvedAdId}/previews`,
        params: {
          ad_format: placement.adFormat
        },
        accessToken: accessContext.accessToken,
        apiVersion: accessContext.apiVersion
      });

      const html = extractPreviewHtml(response) || '';
      previews.push({
        key: placement.key,
        label: placement.label,
        adFormat: placement.adFormat,
        html,
        source: 'meta-graph'
      });
    } catch (error) {
      console.warn(
        '[Meta Ad Preview] Failed to load preview:',
        JSON.stringify({
          adId: resolvedAdId,
          placement: placement.key,
          message: extractApiErrorMessage(error),
          code: error?.response?.data?.error?.code || error?.response?.data?.code || null
        })
      );
      throw buildStageErrorWithDetails(
        'Ad preview',
        `Unable to load the ${placement.label} preview from Meta.`,
        {
          adId: resolvedAdId,
          placement: placement.key,
          metaError: normalizeMetaApiError(error)
        },
        error?.response?.status || 502
      );
    }
  }

  return {
    adId: resolvedAdId,
    previews,
    meta: {
      source: 'meta-graph',
      requestedPlacements: previewPlacements.map((placement) => placement.key)
    }
  };
};

const getAdAccountBillingSummary = async ({ userId } = {}) => {
  const accessContext = await ensureConnectedMetaUser(userId, 'Meta billing');
  const selectedAdAccountId = toCanonicalAdAccountId(accessContext?.connection?.selectedAdAccountId || '');

  if (!selectedAdAccountId) {
    throw buildStageErrorWithDetails(
      'Meta billing',
      'No Meta ad account selected for this user.',
      { userId: userId || '' },
      400
    );
  }

  const adAccount = await graphRequest({
    path: buildAdAccountPath(selectedAdAccountId),
    params: {
      fields:
        'id,name,account_status,currency,amount_spent,balance,spend_cap,funding_source_details,business,owner'
    },
    accessToken: accessContext.accessToken
  });

  const normalizedCurrency = String(adAccount?.currency || 'INR').trim() || 'INR';
  const parseMoney = (value) => {
    const amount = Number(value);
    return Number.isFinite(amount) ? amount : null;
  };

  return {
    adAccount: {
      id: String(adAccount?.id || selectedAdAccountId),
      name: String(adAccount?.name || ''),
      currency: normalizedCurrency,
      accountStatus: adAccount?.account_status ?? null,
      ownerName: String(adAccount?.owner?.name || ''),
      businessName: String(adAccount?.business?.name || '')
    },
    billing: {
      amountSpent: parseMoney(adAccount?.amount_spent),
      currentBalance: parseMoney(adAccount?.balance),
      spendCap: parseMoney(adAccount?.spend_cap),
      fundingSourceType: String(adAccount?.funding_source_details?.type || ''),
      fundingSourceDisplay: String(
        adAccount?.funding_source_details?.display_string ||
          adAccount?.funding_source_details?.id ||
          ''
      )
    },
    meta: {
      source: 'meta-graph',
      note:
        'Depending on your Meta billing model, currentBalance may represent billed balance or may be unavailable.'
    }
  };
};

const exchangeCodeForAccessToken = async ({ code, redirectUri, appId, appSecret, apiVersion }) =>
  metaAuthService.exchangeCodeForAccessToken({ code, redirectUri, appId, appSecret, apiVersion });

const getUserAdAccounts = async ({ userId } = {}) => {
  if (shouldUseMockMode()) {
    const setup = await getSetupBundle({ userId });
    return (setup.adAccounts || []).map(normalizeMetaAdAccountRecord);
  }

  const accessContext = await ensureConnectedMetaUser(userId, 'Meta account selection');
  try {
    const response = await graphRequest({
      path: 'me/adaccounts',
      params: { fields: 'id,name,account_status,currency,amount_spent' },
      accessToken: accessContext.accessToken
    });

    return Array.isArray(response?.data) ? response.data.map(normalizeMetaAdAccountRecord) : [];
  } catch (error) {
    if (error?.metaRateLimited || error?.status === 429) {
      const fallbackAccountId = toCanonicalAdAccountId(accessContext.connection?.selectedAdAccountId || '');
      logMetaGraphEvent('fallback-adaccounts', {
        userId: String(userId || ''),
        fallbackAccountId
      });

      return fallbackAccountId
        ? [normalizeMetaAdAccountRecord({
            id: fallbackAccountId,
            name: accessContext.connection?.name || 'Selected Meta Ad Account'
          })]
        : [];
    }

    throw error;
  }
};

const mapMetaObjectiveToCrudObjective = (objective) => {
  const normalizedObjective = String(objective || '').trim().toUpperCase();

  switch (normalizedObjective) {
    case 'OUTCOME_TRAFFIC':
      return 'traffic';
    case 'OUTCOME_ENGAGEMENT':
      return 'engagement';
    case 'OUTCOME_LEADS':
      return 'leads';
    case 'OUTCOME_SALES':
      return 'sales';
    case 'OUTCOME_AWARENESS':
    default:
      return 'awareness';
  }
};

const mapMetaStatusToCrudStatus = (status, effectiveStatus) => {
  const normalized = String(effectiveStatus || status || '').trim().toUpperCase();

  if (['ACTIVE', 'IN_PROCESS'].includes(normalized)) {
    return 'active';
  }
  if (
    [
      'PAUSED',
      'CAMPAIGN_PAUSED',
      'ADSET_PAUSED',
      'AD_PAUSED'
    ].includes(normalized)
  ) {
    return 'paused';
  }
  if (['ARCHIVED', 'DELETED'].includes(normalized)) {
    return 'archived';
  }
  if (['COMPLETED', 'WITH_ISSUES'].includes(normalized)) {
    return 'ended';
  }

  return 'draft';
};

const toMoneyAmount = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed / 100 : 0;
};

const normalizeMetaDateValue = (value) => {
  const normalized = String(value || '').trim();
  if (!normalized) return '';
  if (/^1970-01-01T/i.test(normalized)) return '';
  return normalized;
};

const normalizeCreativeAssetUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';

  if (/^(https?:)?\/\//i.test(raw) || raw.startsWith('/') || raw.startsWith('data:image/')) {
    return raw;
  }

  return '';
};

const extractRemoteCreativeImageUrl = (creative = {}) => {
  const candidates = [
    creative?.image_url,
    creative?.thumbnail_url,
    creative?.picture,
    creative?.image_hash,
    creative?.object_story_spec?.link_data?.image_url,
    creative?.object_story_spec?.link_data?.picture,
    creative?.object_story_spec?.link_data?.image_hash,
    creative?.object_story_spec?.photo_data?.url,
    creative?.object_story_spec?.photo_data?.image_hash,
    creative?.object_story_spec?.video_data?.image_url,
    creative?.object_story_spec?.video_data?.thumbnail_url,
    creative?.asset_feed_spec?.images?.[0]?.url,
    creative?.asset_feed_spec?.images?.[0]?.image_url,
    creative?.asset_feed_spec?.images?.[0]?.hash,
    creative?.asset_feed_spec?.image_urls?.[0],
    creative?.asset_feed_spec?.bodies?.[0]?.text
  ];

  for (const candidate of candidates) {
    const normalized = normalizeCreativeAssetUrl(candidate);
    if (normalized) return normalized;
  }

  return '';
};

const extractRemoteCreativeImageHash = (creative = {}) => {
  const candidates = [
    creative?.image_hash,
    creative?.object_story_spec?.link_data?.image_hash,
    creative?.object_story_spec?.photo_data?.image_hash,
    creative?.asset_feed_spec?.images?.[0]?.hash,
    creative?.asset_feed_spec?.image_hash
  ];

  for (const candidate of candidates) {
    const raw = String(candidate || '').trim();
    if (raw) return raw;
  }

  return '';
};

const lookupMetaImageUrlByHash = async ({ adAccountId, accessToken, imageHash }) => {
  const normalizedHash = String(imageHash || '').trim();
  if (!normalizedHash) return '';

  try {
    const response = await graphRequest({
      path: buildAdAccountPath(adAccountId, 'adimages'),
      params: {
        hashes: JSON.stringify([normalizedHash]),
        fields: 'hash,url,permalink_url,original_width,original_height'
      },
      accessToken
    });

    const images = response?.images || {};
    const imageRecord = images[normalizedHash] || Object.values(images)[0] || null;
    const url = String(imageRecord?.url || imageRecord?.permalink_url || '').trim();
    return normalizeCreativeAssetUrl(url);
  } catch (error) {
    console.warn(
      '[Meta Ads] Unable to resolve image hash',
      JSON.stringify({
        adAccountId,
        imageHash: normalizedHash,
        message: extractApiErrorMessage(error)
      })
    );
    return '';
  }
};

const getCampaignInsightsFromRow = (row = {}) => {
  const actions = Array.isArray(row?.actions) ? row.actions : [];
  const leadAction = actions.find((item) => String(item?.action_type || '').toLowerCase().includes('lead'));
  const leads = Number(leadAction?.value || 0);
  const spend = Number(row?.spend || 0);

  return {
    impressions: Number(row?.impressions || 0),
    reach: Number(row?.reach || 0),
    clicks: Number(row?.clicks || 0),
    spend,
    ctr: Number(row?.ctr || 0),
    cpc: Number(row?.cpc || 0),
    leads,
    cpl: leads ? Number((spend / leads).toFixed(2)) : 0
  };
};

const fetchAccountCampaignInsightsMap = async ({
  accountId,
  campaignIds = [],
  range = 'last30days',
  tokenCandidates = []
} = {}) => {
  const uniqueCampaignIds = [...new Set(
    normalizeArray(campaignIds)
      .map((value) => String(value || '').trim())
      .filter(Boolean)
  )];

  if (!uniqueCampaignIds.length) {
    return new Map();
  }

  const datePreset = mapCrudDateRangeToMetaPreset(range);
  const response = await requestMetaAcrossTokens({
    path: buildAdAccountPath(accountId, 'insights'),
    params: {
      fields: 'campaign_id,impressions,reach,clicks,spend,ctr,cpc,actions',
      date_preset: datePreset,
      level: 'campaign',
      filtering: JSON.stringify([
        { field: 'campaign.id', operator: 'IN', value: uniqueCampaignIds }
      ]),
      limit: uniqueCampaignIds.length
    },
    tokenCandidates
  });

  const rows = Array.isArray(response?.data) ? response.data : [];
  const insightMap = new Map();

  for (const row of rows) {
    const campaignId = String(row?.campaign_id || row?.campaign?.id || row?.campaign || '').trim();
    if (!campaignId) continue;

    const current = insightMap.get(campaignId) || {
      impressions: 0,
      reach: 0,
      clicks: 0,
      spend: 0,
      ctr: 0,
      cpc: 0,
      leads: 0,
      cpl: 0
    };
    const metrics = getCampaignInsightsFromRow(row);
    const merged = {
      impressions: current.impressions + metrics.impressions,
      reach: current.reach + metrics.reach,
      clicks: current.clicks + metrics.clicks,
      spend: Number((current.spend + metrics.spend).toFixed(2)),
      ctr: 0,
      cpc: 0,
      leads: current.leads + metrics.leads,
      cpl: 0
    };
    merged.ctr = merged.impressions > 0 ? Number(((merged.clicks / merged.impressions) * 100).toFixed(2)) : 0;
    merged.cpc = merged.clicks > 0 ? Number((merged.spend / merged.clicks).toFixed(2)) : 0;
    merged.cpl = merged.leads > 0 ? Number((merged.spend / merged.leads).toFixed(2)) : 0;
    insightMap.set(campaignId, merged);
  }

  return insightMap;
};

const getMetaConnectionSyncState = (connection = {}) => ({
  lastSuccessfulAt: connection?.metaSyncLastSuccessfulAt || null,
  lastAutoAt: connection?.metaSyncLastAutoAt || null,
  lastManualAt: connection?.metaSyncLastManualAt || null,
  cooldownUntil: connection?.metaSyncCooldownUntil || null,
  rateLimitedUntil: connection?.metaSyncRateLimitedUntil || null,
  lastErrorCode: connection?.metaSyncLastErrorCode ?? null,
  lastErrorSubcode: connection?.metaSyncLastErrorSubcode ?? null
});

const persistMetaConnectionSyncState = async ({ userId, updates = {} } = {}) => {
  if (!userId) return null;

  return MetaAdsConnection.findOneAndUpdate(
    { userId },
    { $set: updates },
    { new: true }
  );
};

const getMetaSyncStatus = (connection = {}) => {
  const state = getMetaConnectionSyncState(connection);
  const cooldownUntil = state.cooldownUntil ? new Date(state.cooldownUntil) : null;
  const rateLimitedUntil = state.rateLimitedUntil ? new Date(state.rateLimitedUntil) : null;
  const now = Date.now();

  return {
    ...state,
    cooldownActive: Boolean(cooldownUntil && cooldownUntil.getTime() > now),
    rateLimitedActive: Boolean(rateLimitedUntil && rateLimitedUntil.getTime() > now)
  };
};

const resolveSyncMode = (mode) => {
  const normalized = String(mode || '').trim().toLowerCase();
  if (normalized === 'auto' || normalized === 'scheduled') return 'auto';
  return 'manual';
};

const isMetaAutoSyncAllowed = (connection = {}) => {
  const status = getMetaSyncStatus(connection);
  const lastSuccessfulAt = status.lastSuccessfulAt ? new Date(status.lastSuccessfulAt) : null;
  const cooldownUntil = status.cooldownUntil ? new Date(status.cooldownUntil) : null;
  const now = Date.now();

  if (cooldownUntil && cooldownUntil.getTime() > now) {
    return {
      allowed: false,
      reason: 'cooldown',
      cooldownUntil
    };
  }

  if (lastSuccessfulAt && now - lastSuccessfulAt.getTime() < META_AUTO_SYNC_COOLDOWN_MS) {
    return {
      allowed: false,
      reason: 'recent-success',
      cooldownUntil: new Date(lastSuccessfulAt.getTime() + META_AUTO_SYNC_COOLDOWN_MS)
    };
  }

  return { allowed: true };
};

const fetchRemoteCampaigns = async ({ userId, filters = {} } = {}) => {
  if (shouldUseMockMode()) {
    return [];
  }

  const accessContext = await getAccessContextForUser(userId);
  const tokenCandidates = [...new Set([accessContext.accessToken].filter(Boolean))];

  if (!userId || !tokenCandidates.length || !['user', 'admin'].includes(accessContext.source)) {
    return [];
  }

  const adAccountCandidates = new Map();
  const configuredAccountIds = [accessContext.connection?.selectedAdAccountId]
    .map((value) => toCanonicalAdAccountId(value))
    .filter(Boolean);

  configuredAccountIds.forEach((id) => adAccountCandidates.set(id, { id }));

  for (const accessToken of tokenCandidates) {
    try {
      const response = await graphRequest({
        path: 'me/adaccounts',
        params: { fields: 'id,name,account_status,currency,timezone_name', limit: 50 },
        accessToken
      });

      for (const account of Array.isArray(response?.data) ? response.data : []) {
        const canonicalId = toCanonicalAdAccountId(account?.id);
        if (!canonicalId) continue;
        adAccountCandidates.set(canonicalId, { ...(adAccountCandidates.get(canonicalId) || {}), ...account });
      }
    } catch (error) {
      console.warn(
        '[Meta Ads] Unable to load ad accounts while fetching remote campaigns',
        JSON.stringify({
          source: accessContext.source,
          message: extractApiErrorMessage(error)
        })
      );
    }
  }

  const remoteCampaignMap = new Map();
  const remoteCampaignImageMap = new Map();
  const creativeCampaignMap = new Map();
  const accountIds = [...adAccountCandidates.keys()];
  const remoteInsightDatePreset = mapCrudDateRangeToMetaPreset(filters.dateRange || 'last30days');

  for (const adAccountId of accountIds) {
    let adsResponse = null;
    let response = null;

    for (const accessToken of tokenCandidates) {
      if (!adsResponse) {
        try {
          adsResponse = await graphRequest({
            path: buildAdAccountPath(adAccountId, 'ads'),
            params: {
              fields: 'id,name,campaign{id},creative{image_url,thumbnail_url,object_story_spec}',
              limit: 100
            },
            accessToken
          });
        } catch (error) {
          console.warn(
            '[Meta Ads] Unable to load ads for remote campaign images',
            JSON.stringify({
              adAccountId,
              source: accessContext.source,
              message: extractApiErrorMessage(error)
            })
          );
        }
      }

      try {
        response = await graphRequest({
          path: buildAdAccountPath(adAccountId, 'campaigns'),
          params: {
            fields: 'id,name,status,effective_status,objective,daily_budget,lifetime_budget,created_time,updated_time',
            limit: 100
          },
          accessToken
        });
        console.log('Campaigns from Meta:', response?.data);
        break;
      } catch (error) {
        console.warn(
          '[Meta Ads] Unable to load campaigns for ad account',
          JSON.stringify({
            adAccountId,
            source: accessContext.source,
            message: extractApiErrorMessage(error)
          })
        );
      }
    }

    const ads = Array.isArray(adsResponse?.data) ? adsResponse.data : [];
    const unresolvedCreativeIds = new Set();
    const creativeHashMap = new Map();
    for (const ad of ads) {
      const campaignId = String(ad?.campaign?.id || ad?.campaign_id || '').trim();
      const creativeId = String(ad?.creative?.id || ad?.creative_id || '').trim();
      const creativeHash =
        extractRemoteCreativeImageHash(ad?.creative || {}) ||
        String(ad?.creative?.image_hash || '').trim();

      if (campaignId && creativeId) {
        if (!creativeCampaignMap.has(creativeId)) {
          creativeCampaignMap.set(creativeId, new Set());
        }
        creativeCampaignMap.get(creativeId).add(campaignId);
      }

      if (creativeId && creativeHash) {
        creativeHashMap.set(creativeId, creativeHash);
      }

      if (!campaignId || remoteCampaignImageMap.has(campaignId)) continue;

      const creativeUrl = extractRemoteCreativeImageUrl(ad?.creative || {});
      if (creativeUrl) {
        remoteCampaignImageMap.set(campaignId, creativeUrl);
        continue;
      }

      if (creativeId) {
        unresolvedCreativeIds.add(creativeId);
      }
    }

    for (const creativeId of unresolvedCreativeIds) {
      let creativeUrl = '';
      try {
        const creative = await graphRequest({
          path: creativeId,
          params: {
            fields:
              'id,image_url,thumbnail_url,picture,object_story_spec,asset_feed_spec,body,title'
          },
          accessToken: tokenCandidates[0]
        });

        creativeUrl = extractRemoteCreativeImageUrl(creative || {});

        if (creativeUrl) {
          const campaignIds = creativeCampaignMap.get(creativeId);
          for (const campaignId of campaignIds || []) {
            if (!remoteCampaignImageMap.has(campaignId)) {
              remoteCampaignImageMap.set(campaignId, creativeUrl);
            }
          }
          continue;
        }
      } catch (error) {
        console.warn(
          '[Meta Ads] Unable to load creative image for remote campaign',
          JSON.stringify({
            creativeId,
            adAccountId,
            source: accessContext.source,
            message: extractApiErrorMessage(error)
          })
        );
      }

      const creativeHash = creativeHashMap.get(creativeId) || '';
      if (!creativeHash) continue;

      creativeUrl = await lookupMetaImageUrlByHash({
        adAccountId,
        accessToken: tokenCandidates[0],
        imageHash: creativeHash
      });

      if (creativeUrl) {
        const campaignIds = creativeCampaignMap.get(creativeId);
        for (const campaignId of campaignIds || []) {
          if (!remoteCampaignImageMap.has(campaignId)) {
            remoteCampaignImageMap.set(campaignId, creativeUrl);
          }
        }
      }
    }

    const campaigns = Array.isArray(response?.data) ? response.data : [];
    let campaignInsights = new Map();
    try {
      campaignInsights = await fetchAccountCampaignInsightsMap({
        accountId: adAccountId,
        campaignIds: campaigns.map((campaign) => campaign?.id),
        range: filters.dateRange || 'last30days',
        tokenCandidates
      });
    } catch (error) {
      console.warn(
        '[Meta Ads] Unable to load batched campaign insights for remote campaigns',
        JSON.stringify({
          adAccountId,
          source: accessContext.source,
          message: extractApiErrorMessage(error)
        })
      );
    }

    for (const campaign of campaigns) {
      const remoteId = String(campaign?.id || '').trim();
      if (!remoteId || remoteCampaignMap.has(remoteId)) continue;

      const insight = campaignInsights.get(remoteId) || {};
      remoteCampaignMap.set(remoteId, {
        _id: `meta_${remoteId}`,
        id: `meta_${remoteId}`,
        source: 'meta',
        readOnly: true,
        syncedFromMeta: true,
        metaCampaignId: remoteId,
        metaAdAccountId: adAccountId,
        adAccountId,
        name: String(campaign?.name || `Meta Campaign ${remoteId}`),
        platform: 'both',
        objective: mapMetaObjectiveToCrudObjective(campaign?.objective),
        status: mapMetaStatusToCrudStatus(campaign?.status, campaign?.effective_status),
        metaStatus: String(campaign?.effective_status || campaign?.status || '').trim().toUpperCase(),
        localStatus: 'synced',
        dailyBudget: toMoneyAmount(campaign?.daily_budget),
        lifetimeBudget: toMoneyAmount(campaign?.lifetime_budget),
        startDate: normalizeMetaDateValue(campaign?.start_time || campaign?.created_time || ''),
        endDate: normalizeMetaDateValue(campaign?.stop_time || ''),
        targeting: 'Imported from Meta Ads',
        spent: Number(insight?.spend || 0),
        impressions: Number(insight?.impressions || 0),
        clicks: Number(insight?.clicks || 0),
        ctr: Number(insight?.ctr || 0),
        cpc: Number(insight?.cpc || 0),
        revenue: 0,
        createdAt: campaign?.created_time || null,
        updatedAt: campaign?.updated_time || null,
        imageUrl: remoteCampaignImageMap.get(remoteId) || '',
        metaResponse: campaign
      });
    }
  }

  const normalizedSearch = String(filters.search || '').trim().toLowerCase();
  const normalizedStatus = String(filters.status || 'all').trim().toLowerCase();
  const normalizedObjective = String(filters.objective || '').trim().toLowerCase();
  const normalizedPlatform = String(filters.platform || 'all').trim().toLowerCase();

  return [...remoteCampaignMap.values()].filter((campaign) => {
    if (!['', 'all'].includes(normalizedStatus) && campaign.status !== normalizedStatus) {
      return false;
    }
    if (normalizedObjective && campaign.objective !== normalizedObjective) {
      return false;
    }
    if (!['', 'all', 'both'].includes(normalizedPlatform) && campaign.platform !== normalizedPlatform) {
      return false;
    }
    if (
      normalizedSearch &&
      !`${campaign.name} ${campaign.objective} ${campaign.metaCampaignId}`.toLowerCase().includes(normalizedSearch)
    ) {
      return false;
    }
    return true;
  });
};

const getLoginDialogUrl = ({ redirectUri, state, appId, apiVersion }) =>
  metaAuthService.getLoginDialogUrl({ redirectUri, state, appId, apiVersion });

const saveUserConnection = async ({ userId, accessToken, scopes = [] }) =>
  metaAuthService.saveUserConnection({ userId, accessToken, scopes, graphRequest });

const ensureUserConnectionRecord = async ({ userId } = {}) =>
  metaAuthService.ensureUserConnectionRecord({ userId, graphRequest });

const saveUserSelections = async ({ userId, adAccountId, pageId, whatsappNumber }) => {
  const existingConnection = await ensureUserConnectionRecord({ userId });
  if (!existingConnection) {
    const error = new Error('Connect your Meta account before saving ad account, page, or WhatsApp selections.');
    error.status = 400;
    throw error;
  }

  const updates = {
    lastValidatedAt: new Date()
  };

  if (adAccountId !== undefined) {
    updates.selectedAdAccountId = toCanonicalAdAccountId(adAccountId);
  }
  if (pageId !== undefined) {
    const normalizedPageId = String(pageId || '').trim();
    let matchedPage = null;
    if (normalizedPageId) {
      const accessiblePages = await getAccessiblePages({
        accessToken: decryptMetaToken(existingConnection.accessToken)
      });
      matchedPage = accessiblePages.find((page) => String(page?.id || '').trim() === normalizedPageId);
      if (!matchedPage) {
        const error = new Error('The selected Facebook Page is not available for this Facebook login. Reconnect Facebook and grant page access, then try again.');
        error.status = 400;
        throw error;
      }

      const selectedPageAccessToken = String(matchedPage?.access_token || '').trim();
      if (!selectedPageAccessToken) {
        const error = new Error(
          'Facebook page access token is missing for this Meta connection. Reconnect Meta, approve page permissions, and select a Page that returns a Page access token.'
        );
        error.status = 400;
        throw error;
      }

      updates.selectedPageName = String(matchedPage?.name || '').trim();
      updates.selectedPageAccessToken = encryptMetaToken(selectedPageAccessToken);
      console.log(
        '[Meta OAuth] Page selection saved',
        JSON.stringify({
          userId: String(userId || ''),
          pageId: String(matchedPage?.id || ''),
          pageName: String(matchedPage?.name || '')
        })
      );
    } else {
      updates.selectedPageName = '';
      updates.selectedPageAccessToken = '';
    }

    updates.selectedPageId = normalizedPageId;
  }
  if (whatsappNumber !== undefined) {
    updates.selectedWhatsappNumber = String(whatsappNumber || '');
  }

  const connection = await MetaAdsConnection.findOneAndUpdate({ userId }, { $set: updates }, { new: true });

  return connection;
};

const saveUserAdAccountSelection = async ({ userId, adAccountId }) => {
  const selectedAdAccountId = toCanonicalAdAccountId(adAccountId);
  if (!selectedAdAccountId) {
    const error = new Error('Select a valid Meta ad account.');
    error.status = 400;
    throw error;
  }

  const availableAdAccounts = await getUserAdAccounts({ userId });
  const matchedAccount = availableAdAccounts.find(
    (account) => String(account?.id || '') === selectedAdAccountId
  );

  if (!matchedAccount) {
    const error = new Error('The selected ad account is not available for this Facebook login.');
    error.status = 400;
    throw error;
  }

  return saveUserSelections({ userId, adAccountId: selectedAdAccountId });
};

const mapObjectiveToPixelEvent = (objective) => {
  const normalized = String(objective || '').trim().toUpperCase();
  switch (normalized) {
    case 'OUTCOME_SALES':
      return 'PURCHASE';
    case 'OUTCOME_LEADS':
      return 'LEAD';
    case 'OUTCOME_TRAFFIC':
      return 'VIEW_CONTENT';
    default:
      return 'LEAD';
  }
};

const buildPromotedObject = ({ objective, destinationUrl, pageId }) => {
  const env = getEnvConfig();
  if (!env.pixelId) return null;
  if (!/^https?:\/\//i.test(String(destinationUrl || ''))) return null;

  return {
    pixel_id: env.pixelId,
    custom_event_type: mapObjectiveToPixelEvent(objective),
    page_id: pageId || undefined
  };
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const normalizeMetaScheduleTime = (value, boundary = 'start') => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  if (boundary === 'end') {
    date.setUTCHours(23, 59, 59, 999);
  } else {
    date.setUTCHours(0, 0, 0, 0);
  }

  return date.toISOString();
};

const resolveAdIdFromCreation = async ({
  createdAd,
  createdAdSetId,
  createdCreativeId,
  effectiveAdAccountId,
  resolvedAccessToken,
  campaignName
}) => {
  const directId =
    String(
      createdAd?.id ||
      createdAd?.ad_id ||
      createdAd?.data?.id ||
      createdAd?.result?.id ||
      ''
    ).trim();

  if (directId) {
    return directId;
  }

  const adName = `${campaignName} - Ad`;
  for (const waitMs of [0, 800, 1500, 2500, 4000]) {
    if (waitMs > 0) {
      await delay(waitMs);
    }

    const lookupCandidates = [];

    try {
      const adSetAdsResponse = await graphRequest({
        path: `${String(createdAdSetId).trim()}/ads`,
        params: {
          fields: 'id,name,creative{id},adset{id}',
          limit: 50
        },
        accessToken: resolvedAccessToken
      });

      lookupCandidates.push(...(Array.isArray(adSetAdsResponse?.data) ? adSetAdsResponse.data : []));
    } catch (error) {
      console.warn('[Meta Ads] Unable to resolve ad id from ad set lookup:', extractApiErrorMessage(error));
    }

    if (!lookupCandidates.length) {
      try {
        const accountAdsResponse = await graphRequest({
          path: buildAdAccountPath(effectiveAdAccountId, 'ads'),
          params: {
            fields: 'id,name,adset{id},creative{id}',
            limit: 100
          },
          accessToken: resolvedAccessToken
        });

        lookupCandidates.push(...(Array.isArray(accountAdsResponse?.data) ? accountAdsResponse.data : []));
      } catch (error) {
        console.warn('[Meta Ads] Unable to resolve ad id from ad account lookup:', extractApiErrorMessage(error));
      }
    }

    const matchedAd =
      lookupCandidates.find((item) => String(item?.creative?.id || '') === String(createdCreativeId || '')) ||
      lookupCandidates.find((item) => String(item?.name || '').trim() === adName) ||
      lookupCandidates.find((item) => String(item?.adset?.id || item?.adset_id || '') === String(createdAdSetId || ''));

    const resolvedId = String(matchedAd?.id || '').trim();
    if (resolvedId) {
      return resolvedId;
    }
  }

  return '';
};

const createFullAdStack = async ({ campaign, creativeUpload, userId, accessToken }) => {
  const env = getEnvConfig();
  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign creation');
  let resolvedAccessToken = String(accessToken || accessContext.accessToken || '').trim();
  const initialDeliveryStatus =
    String(campaign?.status || '').trim().toUpperCase() === 'ACTIVE' ? 'ACTIVE' : 'PAUSED';
  const effectiveAdAccountId =
    campaign?.adAccountId ||
    accessContext.connection?.selectedAdAccountId;

  if (shouldUseMockMode()) {
    const now = Date.now();
    return {
      apiMode: 'mock',
      adAccountId: effectiveAdAccountId || `mock-ad-account-${now}`,
      campaignId: `mock-campaign-${now}`,
      adSetId: `mock-adset-${now}`,
      creativeId: `mock-creative-${now}`,
      adId: `mock-ad-${now}`,
      mediaHash: creativeUpload?.mediaHash || `mock_${now}`,
      videoId: creativeUpload?.videoId || ''
    };
  }

  if (!effectiveAdAccountId) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Select a Meta ad account for this user before publishing campaigns.',
      { userId: userId || '' },
      400
    );
  }

  const objective = campaign.objective || 'OUTCOME_LEADS';
  const deliveryObjective = objective;
  const requestedPageId = campaign.configuredPageId || accessContext.connection?.selectedPageId;
  if (!requestedPageId) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Select a Facebook Page for this user before publishing campaigns.',
      { userId: userId || '' },
      400
    );
  }
  const creativePageContext = await resolveCreativePageContext({
    requestedPageId,
    accessToken: resolvedAccessToken
  });
  const configuredPageId = creativePageContext.pageId;
  const configuredPageAccessToken = String(
    creativePageContext.pageAccessToken ||
      accessContext.connection?.selectedPageAccessToken ||
      ''
  ).trim();
  if (!configuredPageAccessToken) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      'Facebook page access is missing for this Meta connection. Click Reconnect Meta, approve page permissions, then select a page and save the setup before publishing campaigns.',
      {
        requestedPageId,
        resolvedPageId: configuredPageId,
        accessiblePages: creativePageContext.accessiblePages
      },
      400
    );
  }
  const instagramActorId = campaign.configuredInstagramActorId || undefined;
  const { whatsappNumber: sanitizedWhatsappNumber, destinationUrl } = buildCreativeDestination({
    whatsappNumber: campaign.whatsappNumber,
    pageId: configuredPageId
  });
  const resolvedDestinationUrl = String(campaign?.metaOverrides?.destinationUrl || destinationUrl).trim();
  const targeting = buildTargeting(campaign.targeting);
  const placement = buildPlacement(campaign.placement, campaign.platform);
  const promotedObject = buildPromotedObject({
    objective,
    destinationUrl: resolvedDestinationUrl,
    pageId: configuredPageId
  });
  const rawDailyBudget = Number(campaign?.budget?.dailyBudget || 0);
  const rawLifetimeBudget = Number(campaign?.budget?.lifetimeBudget || 0);
  const hasDailyBudget = Number.isFinite(rawDailyBudget) && rawDailyBudget > 0;
  const hasLifetimeBudget = Number.isFinite(rawLifetimeBudget) && rawLifetimeBudget > 0;
  const useLifetimeBudget = hasLifetimeBudget && !hasDailyBudget;
  const resolvedBudgetAmount = useLifetimeBudget
    ? rawLifetimeBudget
    : (hasDailyBudget ? rawDailyBudget : 500);
  const budgetInMinorUnit = Math.max(1, Math.round(resolvedBudgetAmount * 100));
  const startTime = campaign.schedule?.startTime
    ? normalizeMetaScheduleTime(campaign.schedule.startTime, 'start')
    : new Date().toISOString();
  const endTime = campaign.schedule?.endTime
    ? normalizeMetaScheduleTime(campaign.schedule.endTime, 'end')
    : undefined;
  const campaignCreateUrl = `${GRAPH_BASE_URL}/${env.apiVersion.replace(/^\/+/, '')}/${buildAdAccountPath(effectiveAdAccountId, 'campaigns')}`;

  console.log('Access token exists:', !!resolvedAccessToken);
  console.log('Ad Account ID:', effectiveAdAccountId);
  console.log('Date range:', { startTime, endTime });
  console.log('Meta API URL:', campaignCreateUrl);

  if (useLifetimeBudget && !endTime) {
    throw buildStageErrorWithDetails(
      'Ad set creation',
      'Lifetime budget requires an end date.',
      { budget: resolvedBudgetAmount, campaignName: campaign?.campaignName || '' },
      400
    );
  }

  let createdCampaign;
  try {
    createdCampaign = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'campaigns'),
      data: {
        name: campaign.campaignName,
        objective: deliveryObjective,
        status: initialDeliveryStatus,
        special_ad_categories: [],
        is_adset_budget_sharing_enabled: false
      },
      accessToken: resolvedAccessToken
    });
    console.log('Meta API response:', createdCampaign);
  } catch (error) {
    throw buildStageError('Campaign creation', error);
  }

  const adSetPayload = {
    name: `${campaign.campaignName} - Ad Set`,
    campaign_id: createdCampaign.id,
    ...(useLifetimeBudget ? { lifetime_budget: budgetInMinorUnit } : { daily_budget: budgetInMinorUnit }),
    billing_event: 'IMPRESSIONS',
    optimization_goal:
      campaign?.metaOverrides?.optimizationGoal ||
      (objective === 'OUTCOME_TRAFFIC'
        ? 'LINK_CLICKS'
        : objective === 'OUTCOME_ENGAGEMENT'
          ? 'REACH'
          : 'LINK_CLICKS'),
    bid_strategy: campaign?.metaOverrides?.bidStrategy || env.bidStrategy || 'LOWEST_COST_WITH_BID_CAP',
    bid_amount: Number.isFinite(env.bidAmount) && env.bidAmount > 0 ? env.bidAmount : 5000,
    targeting,
    status: initialDeliveryStatus,
    start_time: startTime,
    end_time: endTime,
    ...placement
  };

  Object.keys(adSetPayload).forEach((key) => adSetPayload[key] === undefined && delete adSetPayload[key]);

  let createdAdSet;
  try {
    const adSetPayloadVariants = [
      {
        ...adSetPayload,
        bid_strategy: 'LOWEST_COST_WITHOUT_CAP',
        bid_amount: undefined,
        promoted_object: promotedObject || undefined
      },
      {
        ...adSetPayload,
        bid_strategy: 'LOWEST_COST_WITH_BID_CAP',
        bid_amount: Number.isFinite(env.bidAmount) && env.bidAmount > 0 ? env.bidAmount : 5000,
        promoted_object: promotedObject || undefined
      },
      {
        ...adSetPayload,
        optimization_goal: 'REACH',
        bid_strategy: 'LOWEST_COST_WITHOUT_CAP',
        bid_amount: undefined,
        promoted_object: undefined
      },
      {
        name: `${campaign.campaignName} - Ad Set`,
        campaign_id: createdCampaign.id,
        ...(useLifetimeBudget ? { lifetime_budget: budgetInMinorUnit } : { daily_budget: budgetInMinorUnit }),
        billing_event: 'IMPRESSIONS',
        optimization_goal: 'REACH',
        bid_strategy: 'LOWEST_COST_WITH_BID_CAP',
        bid_amount: Number.isFinite(env.bidAmount) && env.bidAmount > 0 ? env.bidAmount : 5000,
        targeting,
        status: initialDeliveryStatus,
        start_time: startTime,
        end_time: endTime,
        promoted_object: promotedObject || undefined
      }
    ].map((payload) => {
      const cleaned = { ...payload };
      Object.keys(cleaned).forEach((key) => cleaned[key] === undefined && delete cleaned[key]);
      return cleaned;
    });

    let lastError = null;
    const variantFailures = [];
    for (let index = 0; index < adSetPayloadVariants.length; index += 1) {
      const variant = adSetPayloadVariants[index];
      try {
        console.log(
          `[Meta Ads] Ad set variant ${index + 1}/${adSetPayloadVariants.length} for campaign "${campaign.campaignName}":`,
          JSON.stringify(variant)
        );
        createdAdSet = await graphRequest({
          method: 'POST',
          path: buildAdAccountPath(effectiveAdAccountId, 'adsets'),
          data: variant,
          accessToken: resolvedAccessToken
        });
        lastError = null;
        break;
      } catch (error) {
        lastError = error;
        const metaError = error?.response?.data?.error || {};
        variantFailures.push({
          variantIndex: index + 1,
          payload: variant,
          error: {
            message: metaError.message || error.message,
            type: metaError.type || '',
            code: metaError.code || null,
            error_subcode: metaError.error_subcode || null,
            error_user_title: metaError.error_user_title || '',
            error_user_msg: metaError.error_user_msg || '',
            fbtrace_id: metaError.fbtrace_id || ''
          }
        });
        console.error(
          `[Meta Ads] Ad set variant ${index + 1} failed:`,
          JSON.stringify(variantFailures[variantFailures.length - 1])
        );
      }
    }

    if (lastError) {
      throw buildStageErrorWithDetails(
        'Ad set creation',
        extractApiErrorMessage(lastError),
        {
          attemptedVariants: variantFailures
        },
        lastError?.response?.status || 400
      );
    }
  } catch (error) {
    if (error?.stage === 'Ad set creation') {
      throw error;
    }
    throw buildStageError('Ad set creation', error);
  }

  let createdCreative;
  try {
    createdCreative = await metaCreativeService.createCreative({
      campaignName: campaign.campaignName,
      creative: campaign.creative,
      creativeUpload,
      configuredPageId,
      pageAccessToken: configuredPageAccessToken,
      instagramActorId,
      destinationUrl: resolvedDestinationUrl,
      sanitizedWhatsappNumber,
      adAccountId: effectiveAdAccountId,
      accessToken: resolvedAccessToken,
      graphRequest,
      buildAdAccountPath,
      buildStageErrorWithDetails,
      extractApiErrorMessage,
      creativePageContext
    });
  } catch (error) {
    if (error?.stage) {
      throw error;
    }
    throw buildStageError('Creative creation', error);
  }

  let createdAd;
  try {
    createdAd = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'ads'),
      data: {
        name: `${campaign.campaignName} - Ad`,
        adset_id: createdAdSet.id,
        creative: { creative_id: createdCreative.id },
        status: campaign.status === 'ACTIVE' ? 'ACTIVE' : 'PAUSED'
      },
      accessToken: resolvedAccessToken
    });
  } catch (error) {
    throw buildStageError('Ad creation', error);
  }

  const resolvedAdId = await resolveAdIdFromCreation({
    createdAd,
    createdAdSetId: createdAdSet.id,
    createdCreativeId: createdCreative.id,
    effectiveAdAccountId,
    resolvedAccessToken,
    campaignName: campaign.campaignName
  });

  if (!resolvedAdId) {
    throw buildStageErrorWithDetails(
      'Ad creation',
      'Meta ad was created but its id could not be resolved from the API response.',
      {
        createdAd,
        adSetId: createdAdSet.id,
        creativeId: createdCreative.id,
        adAccountId: effectiveAdAccountId
      },
      400
    );
  }

  return {
    apiMode: 'live',
    adAccountId: effectiveAdAccountId,
    campaignId: createdCampaign.id,
    adSetId: createdAdSet.id,
    creativeId: createdCreative.id,
    adId: resolvedAdId,
    mediaHash: creativeUpload?.mediaHash || '',
    videoId: creativeUpload?.videoId || '',
    destinationUrl: resolvedDestinationUrl,
    pageId: configuredPageId
  };
};

const createMetaAdStack = async ({ campaign, creativeUpload, userId }) =>
  createFullAdStack({ campaign, creativeUpload, userId });

const fetchCampaignInsights = async (campaign) => {
  if (!campaign?.meta?.campaignId) {
    return null;
  }

  if (campaign.apiMode === 'mock' || shouldUseMockMode()) {
    const spend = Number(campaign.budget?.dailyBudget || 0) * 0.74;
    const clicks = Math.max(18, Math.round(spend / 6));
    const leads = Math.max(4, Math.round(clicks / 5));
    const impressions = Math.max(650, clicks * 42);
    const ctr = impressions ? Number(((clicks / impressions) * 100).toFixed(2)) : 0;
    const cpc = clicks ? Number((spend / clicks).toFixed(2)) : 0;
    const cpl = leads ? Number((spend / leads).toFixed(2)) : 0;

    return {
      impressions,
      reach: Math.round(impressions * 0.72),
      clicks,
      leads,
      spend: Number(spend.toFixed(2)),
      ctr,
      cpc,
      cpl,
      lastSyncedAt: new Date()
    };
  }

  let response;
  const accessContext = await ensureConnectedMetaUser(campaign.userId, 'Insights sync');
  const effectiveAdAccountId = campaign?.meta?.adAccountId || accessContext.connection?.selectedAdAccountId;
  if (!effectiveAdAccountId) {
    throw buildStageErrorWithDetails(
      'Insights sync',
      'Select a Meta ad account for this user before syncing analytics.',
      { campaignId: String(campaign?._id || '') },
      400
    );
  }
  try {
    const insightMap = await fetchAccountCampaignInsightsMap({
      accountId: effectiveAdAccountId,
      campaignIds: [campaign.meta.campaignId],
      range: 'last30days',
      tokenCandidates: [accessContext.accessToken]
    });
    const insight = insightMap.get(String(campaign.meta.campaignId)) || {};

    return {
      impressions: Number(insight.impressions || 0),
      reach: Number(insight.reach || 0),
      clicks: Number(insight.clicks || 0),
      leads: Number(insight.leads || 0),
      spend: Number(insight.spend || 0),
      ctr: Number(insight.ctr || 0),
      cpc: Number(insight.cpc || 0),
      cpl: Number(insight.cpl || 0),
      lastSyncedAt: new Date()
    };
  } catch (error) {
    if (error?.metaRateLimited || error?.status === 429) {
      const existingAnalytics = campaign?.analytics || {};
      return {
        impressions: Number(existingAnalytics.impressions || 0),
        reach: Number(existingAnalytics.reach || 0),
        clicks: Number(existingAnalytics.clicks || 0),
        leads: Number(existingAnalytics.leads || 0),
        spend: Number(existingAnalytics.spend || 0),
        ctr: Number(existingAnalytics.ctr || 0),
        cpc: Number(existingAnalytics.cpc || 0),
        cpl: Number(existingAnalytics.cpl || 0),
        lastSyncedAt: existingAnalytics.lastSyncedAt || campaign?.updatedAt || new Date()
      };
    }
    throw buildStageError('Insights sync', error);
  }
};

const getOrCreateWalletRecord = async (userId) => {
  let wallet = await MetaAdsWallet.findOne({ userId });
  if (!wallet) {
    wallet = await MetaAdsWallet.create({ userId, balance: 0 });
  }
  return wallet;
};

const ensureWalletBalance = async ({ userId, requiredAmount, note, campaignId }) => {
  const amount = Math.max(0, Number(requiredAmount || 0));
  const wallet = await getOrCreateWalletRecord(userId);

  if (Number(wallet.balance || 0) < amount) {
    const error = new Error(`Insufficient wallet balance. Add at least ₹${amount} before activating this campaign.`);
    error.status = 400;
    error.stage = 'Wallet balance check';
    error.details = {
      balance: Number(wallet.balance || 0),
      requiredAmount: amount
    };
    throw error;
  }

  if (amount > 0) {
    wallet.balance = Number(wallet.balance || 0) - amount;
    await wallet.save();

    await MetaAdsTransaction.create({
      userId,
      campaignId: campaignId || null,
      amount,
      type: 'debit',
      note: note || 'Campaign activation reserve'
    });
  }

  return wallet;
};

const trackPixelEvent = async ({
  eventName,
  userData = {},
  customData = {},
  eventTime,
  eventSourceUrl,
  actionSource = 'website',
  accessToken
}) => {
  const env = getEnvConfig();
  if (!env.pixelId) {
    return {
      skipped: true,
      reason: 'META_PIXEL_ID is not configured'
    };
  }

  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      eventName
    };
  }

  return graphRequest({
    method: 'POST',
    path: `${env.pixelId}/events`,
    data: {
      data: [
        {
          event_name: eventName,
          event_time: eventTime || Math.floor(Date.now() / 1000),
          action_source: actionSource,
          event_source_url: eventSourceUrl,
          user_data: userData,
          custom_data: customData
        }
      ]
    },
    accessToken
  });
};

const reconcileCampaignSpend = async (campaign, latestAnalytics) => {
  const currentSpend = Number(latestAnalytics?.spend || 0);
  const reservedBudget = Number(campaign?.accounting?.reservedBudget || 0);
  const totalDebited = Number(campaign?.accounting?.totalDebited || 0);

  const extraDebitNeeded = Math.max(0, Number((currentSpend - totalDebited).toFixed(2)));
  if (extraDebitNeeded > 0) {
    const wallet = await getOrCreateWalletRecord(campaign.userId);
    if (Number(wallet.balance || 0) < extraDebitNeeded) {
      campaign.lastError = `Wallet reconciliation pending: add ${extraDebitNeeded.toFixed(2)} INR to cover live spend.`;
    } else {
      wallet.balance = Number(wallet.balance || 0) - extraDebitNeeded;
      await wallet.save();

      await MetaAdsTransaction.create({
        userId: campaign.userId,
        campaignId: campaign._id,
        amount: extraDebitNeeded,
        type: 'debit',
        note: `Spend reconciliation for ${campaign.campaignName}`
      });

      campaign.accounting = {
        reservedBudget,
        totalDebited: Number((totalDebited + extraDebitNeeded).toFixed(2)),
        reconciledSpend: currentSpend,
        lastReconciledAt: new Date()
      };
      campaign.lastError = '';
      return;
    }
  }

  campaign.accounting = {
    reservedBudget,
    totalDebited,
    reconciledSpend: currentSpend,
    lastReconciledAt: new Date()
  };
};

const syncCampaignAnalyticsRecord = async (campaign) => {
  const latestAnalytics = await fetchCampaignInsights(campaign);
  if (!latestAnalytics) {
    return null;
  }

  campaign.analytics = latestAnalytics;
  await reconcileCampaignSpend(campaign, latestAnalytics);
  await campaign.save();
  return campaign;
};

const createMetaCampaignFromCrud = async ({ userId, name, objective, status, accessToken }) => {
  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign creation');
  const effectiveAdAccountId = accessContext.connection?.selectedAdAccountId;

  if (!effectiveAdAccountId) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Select a Meta ad account for this user before creating campaigns.',
      { configuredAdAccountId: effectiveAdAccountId },
      400
    );
  }

  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      id: `mock-campaign-${Date.now()}`,
      effective_status: String(status || 'PAUSED').toUpperCase()
    };
  }

  try {
    const response = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'campaigns'),
      data: {
        name: String(name || 'Campaign').trim(),
        objective: mapCrudObjectiveToMetaObjective(objective),
        status: String(status || 'PAUSED').toUpperCase() === 'ACTIVE' ? 'ACTIVE' : 'PAUSED',
        special_ad_categories: []
      },
      accessToken: String(accessToken || accessContext.accessToken || '').trim()
    });

    await invalidateMetaCampaignCache({
      accessToken: String(accessToken || accessContext.accessToken || '').trim(),
      apiVersion: getEnvConfig().apiVersion,
      campaignId: response?.id,
      adAccountId: effectiveAdAccountId
    });

    return {
      apiMode: 'live',
      ...response
    };
  } catch (error) {
    throw buildStageError('Campaign creation', error);
  }
};

const parseTargetingCountriesFromCrud = (targeting) => {
  const tokens = parseDelimitedTerms(targeting);
  const countries = tokens
    .map((token) => {
      const normalizedToken = String(token || '').trim().toUpperCase();
      if (/^[A-Z]{2}$/.test(normalizedToken)) {
        return normalizedToken;
      }
      return COUNTRY_NAME_TO_CODE[normalizeCountryToken(token)] || '';
    })
    .filter(Boolean);

  return countries.length ? countries : ['IN'];
};

const createMetaAdStackFromCrud = async ({
  userId,
  accessToken,
  adAccountId,
  configuredPageId,
  campaignName,
  objective,
  dailyBudget,
  lifetimeBudget,
  startDate,
  endDate,
  platform,
  targeting,
  ageMin,
  ageMax,
  gender,
  interests,
  behaviors,
  primaryText,
  headline,
  description,
  destinationUrl,
  callToAction,
  optimizationGoal,
  bidStrategy,
  mediaType,
  imageUrl,
  imageFileBuffer,
  imageFileName,
  videoUrl,
  videoFileBuffer,
  videoFileName,
  status,
  logMetaRequest
}) => {
  const env = getEnvConfig();
  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign creation');
  const resolvedAccessToken = String(accessToken || accessContext.accessToken || '').trim();
  const resolvedAdAccountId = toCanonicalAdAccountId(adAccountId || accessContext.connection?.selectedAdAccountId || '');
  const resolvedPageId = String(configuredPageId || accessContext.connection?.selectedPageId || '').trim();
  const resolvedPageAccessToken = String(
    decryptMetaToken(accessContext.connection?.selectedPageAccessToken || '') || ''
  ).trim();
  const normalizedStatus = String(status || '').trim().toUpperCase() === 'ACTIVE' ? 'ACTIVE' : 'PAUSED';
  const resolvedOptimizationGoal = normalizeOptimizationGoalForCrudObjective(objective, optimizationGoal);
  const genders = [];
  const partialData = {};

  if (String(gender || '').toLowerCase() === 'male') genders.push(1);
  if (String(gender || '').toLowerCase() === 'female') genders.push(2);

  if (!resolvedPageId) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Select a Facebook Page for this user before publishing campaigns.',
      { userId: userId || '' },
      400
    );
  }
  if (!resolvedAdAccountId) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Select a Meta ad account for this user before publishing campaigns.',
      { userId: userId || '' },
      400
    );
  }

  const normalizedMediaType = String(mediaType || '').trim().toLowerCase() === 'video' ? 'video' : 'image';
  const parsedDailyBudget = Number(dailyBudget || 0);
  const parsedLifetimeBudget = Number(lifetimeBudget || 0);
  const hasDailyBudget = Number.isFinite(parsedDailyBudget) && parsedDailyBudget > 0;
  const hasLifetimeBudget = Number.isFinite(parsedLifetimeBudget) && parsedLifetimeBudget > 0;
  const resolvedDailyBudget = hasDailyBudget ? parsedDailyBudget : (!hasLifetimeBudget ? 50 : 0);
  const resolvedLifetimeBudget = !hasDailyBudget && hasLifetimeBudget ? parsedLifetimeBudget : 0;
  if (resolvedLifetimeBudget > 0 && !endDate) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Lifetime budget campaigns require an end date.',
      { lifetimeBudget: resolvedLifetimeBudget },
      400
    );
  }

  const interestTerms = parseDelimitedTerms(interests);
  const behaviorTerms = parseDelimitedTerms(behaviors);
  const [resolvedInterests, resolvedBehaviors] = await Promise.all([
    interestTerms.length
      ? resolveMetaTargetingEntries({
          accessToken: resolvedAccessToken,
          terms: interestTerms,
          type: 'adinterest'
        })
      : Promise.resolve([]),
    behaviorTerms.length
      ? resolveMetaTargetingEntries({
          accessToken: resolvedAccessToken,
          terms: behaviorTerms,
          type: 'adTargetingCategory',
          extraParams: { class: 'behaviors' }
        })
      : Promise.resolve([])
  ]);

  const creativeUpload = await uploadCreativeAsset({
    fileBuffer: normalizedMediaType === 'video' ? videoFileBuffer : imageFileBuffer,
    fileName: normalizedMediaType === 'video' ? videoFileName : imageFileName,
    mediaUrl: normalizedMediaType === 'video' ? videoUrl : imageUrl,
    mediaType: normalizedMediaType,
    userId,
    adAccountId: resolvedAdAccountId,
    accessContext
  });

  if (normalizedMediaType === 'video' && !creativeUpload?.videoId) {
    throw buildStageErrorWithDetails(
      'Creative upload',
      'Ad video is required. Upload a video or provide a valid video URL.',
      { videoUrl: videoUrl || '', fileName: videoFileName || '' },
      400
    );
  }

  if (normalizedMediaType === 'image' && !creativeUpload?.mediaHash && !creativeUpload?.mediaUrl) {
    throw buildStageErrorWithDetails(
      'Creative upload',
      'Ad image is required. Upload an image or provide a valid image URL.',
      { imageUrl: imageUrl || '', fileName: imageFileName || '' },
      400
    );
  }

  const campaignPayload = {
    name: String(campaignName || 'Campaign').trim(),
    objective: mapCrudObjectiveToMetaObjective(objective),
    status: normalizedStatus,
    special_ad_categories: [],
    is_adset_budget_sharing_enabled: false
  };
  const campaignPath = buildAdAccountPath(resolvedAdAccountId, 'campaigns');

  let createdCampaignResponse;
  try {
    createdCampaignResponse = await graphRequest({
      method: 'POST',
      path: campaignPath,
      data: campaignPayload,
      accessToken: resolvedAccessToken,
      returnResponse: true
    });
    logMetaRequest?.({
      stage: 'campaign',
      endpoint: campaignPath,
      adAccountId: resolvedAdAccountId,
      payload: campaignPayload,
      response: createdCampaignResponse
    });
  } catch (error) {
    logMetaRequest?.({
      stage: 'campaign',
      endpoint: campaignPath,
      adAccountId: resolvedAdAccountId,
      payload: campaignPayload,
      error
    });
    const wrappedError = buildStageError('Campaign creation', error);
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  const createdCampaign = createdCampaignResponse?.data || {};
  partialData.metaCampaignId = String(createdCampaign.id || '').trim();
  partialData.campaignStatus = String(createdCampaign.status || createdCampaign.effective_status || normalizedStatus || 'PAUSED')
    .trim()
    .toUpperCase() || 'PAUSED';
  if (!partialData.metaCampaignId) {
    const wrappedError = buildStageErrorWithDetails(
      'Campaign creation',
      'Meta campaign creation did not return a campaign ID.',
      { partialData },
      502
    );
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  const simpleTargeting = {
    geo_locations: {
      countries: parseTargetingCountriesFromCrud(targeting)
    },
    age_min: Math.max(13, Number(ageMin || 18)),
    age_max: Math.min(65, Number(ageMax || 65)),
    targeting_automation: {
      advantage_audience: env.advantageAudience === 1 ? 1 : 0
    }
  };

  if (genders.length) {
    simpleTargeting.genders = genders;
  }

  if (resolvedInterests.length || resolvedBehaviors.length) {
    const flexibleSpec = {};
    if (resolvedInterests.length) {
      flexibleSpec.interests = resolvedInterests;
    }
    if (resolvedBehaviors.length) {
      flexibleSpec.behaviors = resolvedBehaviors;
    }
    simpleTargeting.flexible_spec = [flexibleSpec];
  }

  const adSetPayload = {
    name: `${String(campaignName || 'Campaign').trim()} - Ad Set`,
    campaign_id: partialData.metaCampaignId,
    ...(resolvedLifetimeBudget > 0 ? { lifetime_budget: Math.max(1, Math.round(resolvedLifetimeBudget * 100)) } : { daily_budget: Math.max(1, Math.round(resolvedDailyBudget * 100)) }),
    billing_event: 'IMPRESSIONS',
    optimization_goal: resolvedOptimizationGoal,
    bid_strategy: String(bidStrategy || env.bidStrategy || 'LOWEST_COST_WITHOUT_CAP').trim().toUpperCase(),
    targeting: simpleTargeting,
    status: normalizedStatus,
    start_time: startDate ? normalizeMetaScheduleTime(startDate, 'start') : new Date().toISOString(),
    ...(endDate ? { end_time: normalizeMetaScheduleTime(endDate, 'end') } : {})
  };

  const normalizedObjective = String(objective || '').trim().toLowerCase();
  if (normalizedObjective === 'awareness') {
    adSetPayload.optimization_goal = 'REACH';
    adSetPayload.bid_strategy = 'LOWEST_COST_WITHOUT_CAP';
  } else {
    if (!['LOWEST_COST_WITHOUT_CAP', 'LOWEST_COST_WITH_BID_CAP'].includes(adSetPayload.bid_strategy)) {
      adSetPayload.bid_strategy = 'LOWEST_COST_WITHOUT_CAP';
    }

    if (adSetPayload.bid_strategy === 'LOWEST_COST_WITH_BID_CAP') {
      const rawBidAmount = Number(env.bidAmount || 0);
      if (Number.isFinite(rawBidAmount) && rawBidAmount > 0) {
        adSetPayload.bid_amount = Math.round(rawBidAmount);
      }
    }

    if (/^https?:\/\//i.test(String(destinationUrl || ''))) {
      adSetPayload.promoted_object = { page_id: resolvedPageId };
    }
  }

  Object.keys(adSetPayload).forEach((key) => adSetPayload[key] === undefined && delete adSetPayload[key]);

  const adSetPath = buildAdAccountPath(resolvedAdAccountId, 'adsets');
  let createdAdSetResponse;
  try {
    createdAdSetResponse = await graphRequest({
      method: 'POST',
      path: adSetPath,
      data: adSetPayload,
      accessToken: resolvedAccessToken,
      returnResponse: true
    });
    logMetaRequest?.({
      stage: 'adset',
      endpoint: adSetPath,
      adAccountId: resolvedAdAccountId,
      payload: adSetPayload,
      response: createdAdSetResponse
    });
  } catch (error) {
    logMetaRequest?.({
      stage: 'adset',
      endpoint: adSetPath,
      adAccountId: resolvedAdAccountId,
      payload: adSetPayload,
      error
    });
    const wrappedError = buildStageError('Ad set creation', error);
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  const createdAdSet = createdAdSetResponse?.data || {};
  partialData.metaAdSetId = String(createdAdSet.id || '').trim();
  partialData.adSetStatus = String(createdAdSet.status || createdAdSet.effective_status || normalizedStatus || 'PAUSED')
    .trim()
    .toUpperCase() || 'PAUSED';
  if (!partialData.metaAdSetId) {
    const wrappedError = buildStageErrorWithDetails(
      'Ad set creation',
      'Meta ad set creation did not return an ad set ID.',
      { partialData },
      502
    );
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  let createdCreative;
  try {
    createdCreative = await metaCreativeService.createCreative({
      campaignName: String(campaignName || 'Campaign').trim(),
      creative: {
        primaryText: String(primaryText || campaignName || '').trim(),
        headline: String(headline || campaignName || '').trim(),
        description: String(description || '').trim(),
        callToAction: String(callToAction || 'LEARN_MORE').trim().toUpperCase(),
        mediaType: normalizedMediaType
      },
      creativeUpload,
      configuredPageId: resolvedPageId,
      pageAccessToken: resolvedPageAccessToken || resolvedAccessToken,
      instagramActorId: undefined,
      destinationUrl:
        normalizedObjective === 'awareness'
          ? `https://www.facebook.com/${resolvedPageId}`
          : (String(destinationUrl || '').trim() || `https://www.facebook.com/${resolvedPageId}`),
      sanitizedWhatsappNumber: '',
      adAccountId: resolvedAdAccountId,
      accessToken: resolvedAccessToken,
      graphRequest,
      buildAdAccountPath,
      buildStageErrorWithDetails,
      extractApiErrorMessage,
      logMetaRequest
    });
  } catch (error) {
    logMetaRequest?.({
      stage: 'creative',
      endpoint: buildAdAccountPath(resolvedAdAccountId, 'adcreatives'),
      adAccountId: resolvedAdAccountId,
      payload: {
        name: `${String(campaignName || 'Campaign').trim()} - Creative`
      },
      error
    });
    const wrappedError = error?.stage ? error : buildStageError('Creative creation', error);
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  if (!createdCreative?.id) {
    const wrappedError = buildStageErrorWithDetails(
      'Creative creation',
      'Meta creative creation did not return a creative ID.',
      { partialData },
      400
    );
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  partialData.metaCreativeId = String(createdCreative.id || '').trim();

  const adPayload = {
    name: `${String(campaignName || 'Campaign').trim()} - Ad`,
    adset_id: partialData.metaAdSetId,
    creative: { creative_id: partialData.metaCreativeId },
    status: normalizedStatus
  };
  const adPath = buildAdAccountPath(resolvedAdAccountId, 'ads');

  let createdAdResponse;
  try {
    createdAdResponse = await graphRequest({
      method: 'POST',
      path: adPath,
      data: adPayload,
      accessToken: resolvedAccessToken,
      returnResponse: true
    });
    logMetaRequest?.({
      stage: 'ad',
      endpoint: adPath,
      adAccountId: resolvedAdAccountId,
      payload: adPayload,
      response: createdAdResponse
    });
  } catch (error) {
    logMetaRequest?.({
      stage: 'ad',
      endpoint: adPath,
      adAccountId: resolvedAdAccountId,
      payload: adPayload,
      error
    });
    const wrappedError = buildStageError('Ad creation', error);
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  const createdAd = createdAdResponse?.data || {};
  partialData.metaAdId = String(createdAd.id || '').trim();
  partialData.adStatus = String(createdAd.status || createdAd.effective_status || normalizedStatus || 'PAUSED')
    .trim()
    .toUpperCase() || 'PAUSED';

  if (!partialData.metaAdId) {
    const wrappedError = buildStageErrorWithDetails(
      'Ad creation',
      'Meta ad was created but its id could not be resolved from the API response.',
      { partialData },
      400
    );
    wrappedError.partialData = partialData;
    throw wrappedError;
  }

  await invalidateMetaCampaignCache({
    accessToken: resolvedAccessToken,
    apiVersion: getEnvConfig().apiVersion,
    campaignId: partialData.metaCampaignId,
    adSetId: partialData.metaAdSetId,
    adId: partialData.metaAdId,
    adAccountId: resolvedAdAccountId
  });

  return {
    apiMode: 'live',
    adAccountId: resolvedAdAccountId,
    campaignId: partialData.metaCampaignId,
    adSetId: partialData.metaAdSetId,
    creativeId: partialData.metaCreativeId,
    adId: partialData.metaAdId,
    campaignStatus: partialData.campaignStatus,
    adSetStatus: partialData.adSetStatus,
    adStatus: partialData.adStatus,
    mediaHash: creativeUpload?.mediaHash || '',
    videoId: creativeUpload?.videoId || '',
    destinationUrl: String(destinationUrl || '').trim() || `https://www.facebook.com/${resolvedPageId}`,
    pageId: resolvedPageId
  };
};

const updateMetaCrudDeliveryStatus = async ({ userId, campaignId, adSetId, adId, status }) => {
  const normalizedStatus = String(status || '').trim().toUpperCase();

  if (!['ACTIVE', 'PAUSED'].includes(normalizedStatus)) {
    return { skipped: true };
  }

  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      status: normalizedStatus
    };
  }

  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign status update');
  const updates = [
    campaignId ? graphRequest({ method: 'POST', path: campaignId, data: { status: normalizedStatus }, accessToken: accessContext.accessToken }) : null,
    adSetId ? graphRequest({ method: 'POST', path: adSetId, data: { status: normalizedStatus }, accessToken: accessContext.accessToken }) : null,
    adId ? graphRequest({ method: 'POST', path: adId, data: { status: normalizedStatus }, accessToken: accessContext.accessToken }) : null
  ].filter(Boolean);

  try {
    await Promise.all(updates);
    await invalidateMetaCampaignCache({
      accessToken: accessContext.accessToken,
      apiVersion: getEnvConfig().apiVersion,
      campaignId,
      adSetId,
      adId
    });
    return {
      apiMode: 'live',
      status: normalizedStatus
    };
  } catch (error) {
    throw buildStageError('Campaign status update', error);
  }
};

const archiveMetaCrudAssets = async ({ userId, campaignId, adSetId, adId }) => {
  const assetIds = [
    { id: adId, label: 'ad' },
    { id: adSetId, label: 'ad set' },
    { id: campaignId, label: 'campaign' }
  ].filter((asset) => String(asset.id || '').trim());

  if (!assetIds.length) {
    return {
      skipped: true,
      reason: 'No Meta assets linked to this campaign.'
    };
  }

  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      archived: assetIds.map((asset) => ({
        id: asset.id,
        type: asset.label,
        status: 'ARCHIVED'
      }))
    };
  }

  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign deletion');
  const archived = [];

  for (const asset of assetIds) {
    try {
      await graphRequest({
        method: 'POST',
        path: String(asset.id).trim(),
        data: { status: 'ARCHIVED' },
        accessToken: accessContext.accessToken
      });

      archived.push({
        id: asset.id,
        type: asset.label,
        status: 'ARCHIVED'
      });
    } catch (error) {
      throw buildStageErrorWithDetails(
        'Campaign deletion',
        `Unable to archive the Meta ${asset.label} before deleting the local campaign.`,
        {
          assetType: asset.label,
          assetId: asset.id,
          archived,
          metaError: error?.response?.data || { message: extractApiErrorMessage(error) }
        },
        error?.response?.status || 400
      );
    }
  }

  await invalidateMetaCampaignCache({
    accessToken: accessContext.accessToken,
    apiVersion: getEnvConfig().apiVersion,
    campaignId,
    adSetId,
    adId
  });

  return {
    apiMode: 'live',
    archived
  };
};

const updateMetaCampaignFromCrud = async ({ userId, campaignId, name, status }) => {
  if (!campaignId) {
    return { apiMode: shouldUseMockMode() ? 'mock' : 'skipped' };
  }

  if (shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      id: campaignId,
      name,
      status
    };
  }

  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign update');
  const payload = {};

  if (String(name || '').trim()) {
    payload.name = String(name).trim();
  }

  const normalizedStatus = String(status || '').trim().toUpperCase();
  if (['ACTIVE', 'PAUSED'].includes(normalizedStatus)) {
    payload.status = normalizedStatus;
  }

  if (!Object.keys(payload).length) {
    return {
      apiMode: 'live',
      id: campaignId,
      skipped: true
    };
  }

  try {
    const response = await graphRequest({
      method: 'POST',
      path: campaignId,
      data: payload,
      accessToken: accessContext.accessToken
    });

    await invalidateMetaCampaignCache({
      accessToken: accessContext.accessToken,
      apiVersion: getEnvConfig().apiVersion,
      campaignId
    });

    return {
      apiMode: 'live',
      ...response
    };
  } catch (error) {
    throw buildStageError('Campaign update', error);
  }
};

const updateCampaign = async ({ userId, campaignId, name, status }) =>
  updateMetaCampaignFromCrud({ userId, campaignId, name, status });

const pauseCampaign = async ({ userId, campaignId, adSetId, adId }) =>
  updateMetaCrudDeliveryStatus({ userId, campaignId, adSetId, adId, status: 'PAUSED' });

const resumeCampaign = async ({ userId, campaignId, adSetId, adId }) =>
  updateMetaCrudDeliveryStatus({ userId, campaignId, adSetId, adId, status: 'ACTIVE' });

const fetchInsights = async (campaign) => fetchCampaignInsights(campaign);

const syncAllCampaignAnalytics = async () => {
  const campaigns = await MetaAdCampaign.find({
    status: { $in: ['ACTIVE', 'PAUSED'] }
  });

  const results = {
    synced: 0,
    warnings: []
  };

  for (const campaign of campaigns) {
    try {
      await syncCampaignAnalyticsRecord(campaign);
      results.synced += 1;
    } catch (error) {
      campaign.lastError = error.message || 'Analytics sync failed';
      await campaign.save();
      results.warnings.push({
        campaignId: String(campaign._id),
        campaignName: campaign.campaignName,
        error: error.message
      });
    }
  }

  return results;
};

const mapCrudDateRangeToMetaPreset = (range = 'last30days') => {
  switch (String(range || '').trim().toLowerCase()) {
    case 'today':
      return 'today';
    case 'yesterday':
      return 'yesterday';
    case 'last7days':
      return 'last_7d';
    case 'thismonth':
      return 'this_month';
    case 'lastmonth':
      return 'last_month';
    case 'last30days':
    default:
      return 'last_30d';
  }
};

const fetchCrudCampaignInsights = async ({ campaign, userId, range = 'last30days' }) => {
  if (!campaign?.metaCampaignId) return null;

  if (shouldUseMockMode()) {
    const budgetReference = Number(campaign?.dailyBudget || campaign?.lifetimeBudget || 0);
    const spend = Number((budgetReference * 0.74).toFixed(2));
    const clicks = Math.max(1, Math.round(spend / 6));
    const impressions = Math.max(100, clicks * 42);
    const ctr = impressions ? Number(((clicks / impressions) * 100).toFixed(2)) : 0;
    const cpc = clicks ? Number((spend / clicks).toFixed(2)) : 0;

    return {
      impressions,
      reach: Math.round(impressions * 0.72),
      clicks,
      spend,
      ctr,
      cpc,
      leads: 0,
      cpl: 0,
      lastSyncedAt: new Date()
    };
  }

  const ownerUserId = String(userId || campaign?.createdBy || '').trim();
  const accessContext = await ensureConnectedMetaUser(ownerUserId, 'Insights sync');
  const effectiveAdAccountId =
    campaign?.metaResponse?.adAccountId ||
    campaign?.metaAdAccountId ||
    accessContext.connection?.selectedAdAccountId;
  if (!effectiveAdAccountId) {
    throw buildStageErrorWithDetails(
      'Insights sync',
      'Select a Meta ad account before syncing campaign analytics.',
      { campaignId: String(campaign?._id || ''), userId: ownerUserId },
      400
    );
  }

  let response;
  try {
    const insightMap = await fetchAccountCampaignInsightsMap({
      accountId: effectiveAdAccountId,
      campaignIds: [campaign.metaCampaignId],
      range,
      tokenCandidates: [accessContext.accessToken]
    });
    const insight = insightMap.get(String(campaign.metaCampaignId)) || {};

    return {
      impressions: Number(insight.impressions || 0),
      reach: Number(insight.reach || 0),
      clicks: Number(insight.clicks || 0),
      spend: Number(insight.spend || 0),
      ctr: Number(insight.ctr || 0),
      cpc: Number(insight.cpc || 0),
      leads: Number(insight.leads || 0),
      cpl: Number(insight.cpl || 0),
      lastSyncedAt: new Date()
    };
  } catch (error) {
    if (error?.metaRateLimited || error?.status === 429) {
      const existingAnalytics = campaign?.analytics || {};
      return {
        impressions: Number(existingAnalytics.impressions || 0),
        reach: Number(existingAnalytics.reach || 0),
        clicks: Number(existingAnalytics.clicks || 0),
        spend: Number(existingAnalytics.spend || 0),
        ctr: Number(existingAnalytics.ctr || 0),
        cpc: Number(existingAnalytics.cpc || 0),
        leads: Number(existingAnalytics.leads || 0),
        cpl: Number(existingAnalytics.cpl || 0),
        lastSyncedAt: existingAnalytics.lastSyncedAt || campaign?.updatedAt || new Date()
      };
    }
    throw buildStageError('Insights sync', error);
  }
};

const syncCrudCampaignAnalyticsRecord = async ({ campaign, userId, range = 'last30days', syncMode = 'manual' }) => {
  const latestInsights = await fetchCrudCampaignInsights({ campaign, userId, range });
  if (!latestInsights) return null;

  const persistedCampaign = campaign;
  persistedCampaign.spent = Number(latestInsights.spend || 0);
  persistedCampaign.impressions = Number(latestInsights.impressions || 0);
  persistedCampaign.clicks = Number(latestInsights.clicks || 0);
  persistedCampaign.ctr = Number(latestInsights.ctr || 0);
  persistedCampaign.cpc = Number(latestInsights.cpc || 0);

  const existingMetaResponse =
    persistedCampaign.metaResponse && typeof persistedCampaign.metaResponse === 'object'
      ? persistedCampaign.metaResponse
      : {};
  persistedCampaign.metaResponse = {
    ...existingMetaResponse,
    latestInsights,
    analyticsLastSyncedAt: new Date().toISOString()
  };
  persistedCampaign.markModified('metaResponse');
  await persistedCampaign.save();

  const normalizedUserId = String(userId || campaign?.createdBy || '').trim();
  if (normalizedUserId) {
    const now = new Date();
    const updates = {
      metaSyncLastSuccessfulAt: now,
      metaSyncLastErrorCode: null,
      metaSyncLastErrorSubcode: null,
      metaSyncRateLimitedUntil: null
    };

    if (resolveSyncMode(syncMode) === 'auto') {
      updates.metaSyncLastAutoAt = now;
      updates.metaSyncCooldownUntil = new Date(now.getTime() + META_AUTO_SYNC_COOLDOWN_MS);
    } else {
      updates.metaSyncLastManualAt = now;
    }

    await persistMetaConnectionSyncState({ userId: normalizedUserId, updates });
  }

  return {
    campaign: persistedCampaign,
    insights: latestInsights
  };
};

const syncAllCrudCampaignAnalytics = async ({ userId, mode = 'manual', range = 'last30days' } = {}) => {
  const syncMode = resolveSyncMode(mode);
  const query = {
    metaCampaignId: { $exists: true, $ne: '' },
    status: { $in: ['active', 'paused'] }
  };
  if (userId) {
    query.createdBy = userId;
  }

  const campaigns = await Campaign.find(query);
  const results = {
    synced: 0,
    skipped: 0,
    rateLimited: false,
    warnings: []
  };

  const groupedCampaigns = new Map();
  for (const campaign of campaigns) {
    const ownerUserId = String(campaign.createdBy || userId || '').trim();
    const adAccountId = toCanonicalAdAccountId(
      campaign?.metaAdAccountId ||
      campaign?.metaResponse?.adAccountId ||
      campaign?.setupSnapshot?.selectedAdAccountId ||
      ''
    );

    if (!ownerUserId || !adAccountId) {
      results.skipped += 1;
      continue;
    }

    const groupKey = `${ownerUserId}::${adAccountId}`;
    if (!groupedCampaigns.has(groupKey)) {
      groupedCampaigns.set(groupKey, {
        userId: ownerUserId,
        adAccountId,
        campaigns: []
      });
    }

    groupedCampaigns.get(groupKey).campaigns.push(campaign);
  }

  for (const [groupKey, group] of groupedCampaigns.entries()) {
    let accessContext;
    try {
      accessContext = await ensureConnectedMetaUser(group.userId, 'Insights sync');
    } catch (error) {
      results.warnings.push({
        group: groupKey,
        error: error.message || 'Unable to load Meta access context'
      });
      continue;
    }

    const syncStatus = getMetaSyncStatus(accessContext.connection || {});
    if (syncMode === 'auto') {
      const autoAllowed = isMetaAutoSyncAllowed(accessContext.connection || {});
      if (!autoAllowed.allowed) {
        results.skipped += group.campaigns.length;
        logMetaGraphEvent('sync-cooldown-skip', {
          groupKey,
          userId: group.userId,
          adAccountId: group.adAccountId,
          reason: autoAllowed.reason,
          cooldownUntil: autoAllowed.cooldownUntil ? autoAllowed.cooldownUntil.toISOString() : null
        });
        continue;
      }
    } else {
      const lastManualAt = syncStatus.lastManualAt ? new Date(syncStatus.lastManualAt) : null;
      if (lastManualAt && Date.now() - lastManualAt.getTime() < META_MANUAL_SYNC_DEBOUNCE_MS) {
        results.skipped += group.campaigns.length;
        logMetaGraphEvent('sync-manual-debounce', {
          groupKey,
          userId: group.userId,
          adAccountId: group.adAccountId,
          lastManualAt: lastManualAt.toISOString()
        });
        continue;
      }
    }

    try {
      const campaignIds = group.campaigns.map((campaign) => campaign.metaCampaignId);
      const insightMap = await fetchAccountCampaignInsightsMap({
        accountId: group.adAccountId,
        campaignIds,
        range,
        tokenCandidates: [accessContext.accessToken]
      });

      for (const campaign of group.campaigns) {
        const insight = insightMap.get(String(campaign.metaCampaignId || '').trim()) || getCampaignInsightsFromRow({});
        const persistedCampaign = campaign;
        persistedCampaign.spent = Number(insight.spend || 0);
        persistedCampaign.impressions = Number(insight.impressions || 0);
        persistedCampaign.clicks = Number(insight.clicks || 0);
        persistedCampaign.ctr = Number(insight.ctr || 0);
        persistedCampaign.cpc = Number(insight.cpc || 0);

        const existingMetaResponse =
          persistedCampaign.metaResponse && typeof persistedCampaign.metaResponse === 'object'
            ? persistedCampaign.metaResponse
            : {};
        persistedCampaign.metaResponse = {
          ...existingMetaResponse,
          latestInsights: {
            ...insight,
            lastSyncedAt: new Date()
          },
          analyticsLastSyncedAt: new Date().toISOString()
        };
        persistedCampaign.markModified('metaResponse');
        await persistedCampaign.save();
        results.synced += 1;
      }

      const now = new Date();
      await persistMetaConnectionSyncState({
        userId: group.userId,
        updates: {
          metaSyncLastSuccessfulAt: now,
          metaSyncLastAutoAt: syncMode === 'auto' ? now : undefined,
          metaSyncLastManualAt: syncMode === 'manual' ? now : undefined,
          metaSyncCooldownUntil: syncMode === 'auto' ? new Date(now.getTime() + META_AUTO_SYNC_COOLDOWN_MS) : null,
          metaSyncRateLimitedUntil: null,
          metaSyncLastErrorCode: null,
          metaSyncLastErrorSubcode: null
        }
      });
    } catch (error) {
      if (error?.metaRateLimited || error?.status === 429) {
        const blockedUntil = error?.metaRateLimit?.blockedUntil ? new Date(error.metaRateLimit.blockedUntil) : null;
        await persistMetaConnectionSyncState({
          userId: group.userId,
          updates: {
            metaSyncRateLimitedUntil: blockedUntil || new Date(Date.now() + META_AUTO_SYNC_COOLDOWN_MS),
            metaSyncLastErrorCode: error?.metaRateLimit?.code || 17,
            metaSyncLastErrorSubcode: error?.metaRateLimit?.errorSubcode || 2446079
          }
        });
        results.rateLimited = true;
        results.warnings.push({
          group: groupKey,
          error: error.message || 'Meta API rate limit reached'
        });
        logMetaGraphEvent('sync-rate-limited', {
          groupKey,
          userId: group.userId,
          adAccountId: group.adAccountId,
          blockedUntil: blockedUntil ? blockedUntil.toISOString() : null
        });
        break;
      }

      results.warnings.push({
        group: groupKey,
        error: error.message || 'Campaign analytics sync failed'
      });
    }
  }

  return results;
};

const refreshCrudCampaignAnalytics = async ({ campaignId, userId, range = 'last30days' }) => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) {
    const error = new Error('Campaign not found');
    error.status = 404;
    throw error;
  }

  if (userId && String(campaign.createdBy || '') !== String(userId)) {
    const error = new Error('Not authorized to sync this campaign');
    error.status = 403;
    throw error;
  }

  return syncCrudCampaignAnalyticsRecord({
    campaign,
    userId: String(campaign.createdBy || ''),
    range,
    syncMode: 'manual'
  });
};

const updateCampaignDeliveryStatus = async ({ campaign, userId, status }) => {
  const normalizedStatus = String(status || '').toUpperCase();
  if (!['ACTIVE', 'PAUSED'].includes(normalizedStatus)) {
    const error = new Error('Only ACTIVE and PAUSED statuses are supported.');
    error.status = 400;
    throw error;
  }

  if (campaign?.apiMode === 'mock' || shouldUseMockMode()) {
    return {
      apiMode: 'mock',
      status: normalizedStatus
    };
  }

  const accessContext = await ensureConnectedMetaUser(userId || campaign?.userId, 'Campaign status update');

  try {
    if (campaign?.meta?.campaignId) {
      await graphRequest({
        method: 'POST',
        path: campaign.meta.campaignId,
        data: { status: normalizedStatus },
        accessToken: accessContext.accessToken
      });
    }

    if (campaign?.meta?.adSetId) {
      await graphRequest({
        method: 'POST',
        path: campaign.meta.adSetId,
        data: { status: normalizedStatus },
        accessToken: accessContext.accessToken
      });
    }

    if (campaign?.meta?.adId) {
      await graphRequest({
        method: 'POST',
        path: campaign.meta.adId,
        data: { status: normalizedStatus },
        accessToken: accessContext.accessToken
      });
    }
  } catch (error) {
    throw buildStageError('Campaign status update', error);
  }

  return {
    apiMode: 'live',
    status: normalizedStatus
  };
};

const getInsightsDatePreset = (range) => {
  const normalizedRange = String(range || '30d').trim().toLowerCase();
  switch (normalizedRange) {
    case '7d':
      return 'last_7d';
    case '90d':
      return 'last_90d';
    case '30d':
    default:
      return 'last_30d';
  }
};

const dedupeById = (items = []) => {
  const seen = new Set();
  return items.filter((item) => {
    const id = String(item?.id || '').trim();
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
};

const buildInsightsFilteringParam = ({ campaignId, adSetId } = {}) => {
  const normalizedAdSetId = String(adSetId || '').trim();
  const normalizedCampaignId = String(campaignId || '').trim();

  if (normalizedAdSetId) {
    return JSON.stringify([{ field: 'adset.id', operator: 'IN', value: [normalizedAdSetId] }]);
  }

  if (normalizedCampaignId) {
    return JSON.stringify([{ field: 'campaign.id', operator: 'IN', value: [normalizedCampaignId] }]);
  }

  return '';
};

const resolveInsightsAccess = async ({ userId } = {}) => {
  const accessContext = await getAccessContextForUser(userId);
  if (!userId || !accessContext?.accessToken || !['user', 'admin'].includes(accessContext.source)) {
    return {
      accessContext,
      tokenCandidates: [],
      adAccounts: []
    };
  }
  const tokenCandidates = [...new Set([accessContext.accessToken].filter(Boolean))];
  const adAccounts = new Map();

  [accessContext.connection?.selectedAdAccountId]
    .map((value) => toCanonicalAdAccountId(value))
    .filter(Boolean)
    .forEach((id) => {
      adAccounts.set(id, { id, source: 'configured' });
    });

  for (const accessToken of tokenCandidates) {
    try {
      const response = await graphRequest({
        path: 'me/adaccounts',
        params: { fields: 'id,name,account_status,currency,timezone_name', limit: 100 },
        accessToken
      });

      for (const account of Array.isArray(response?.data) ? response.data : []) {
        const id = toCanonicalAdAccountId(account?.id);
        if (!id) continue;
        adAccounts.set(id, {
          ...(adAccounts.get(id) || {}),
          ...account,
          id
        });
      }
    } catch (error) {
      console.warn(
        '[Meta Insights] Unable to load ad accounts',
        JSON.stringify({
          source: accessContext.source,
          message: extractApiErrorMessage(error)
        })
      );
    }
  }

  return {
    accessContext,
    tokenCandidates,
    adAccounts: [...adAccounts.values()]
  };
};

const requestMetaAcrossTokens = async ({ path, params, tokenCandidates }) => {
  let lastError = null;

  for (const accessToken of tokenCandidates) {
    try {
      return await graphRequest({
        path,
        params,
        accessToken
      });
    } catch (error) {
      lastError = error;
    }
  }

  if (lastError) {
    throw lastError;
  }

  return { data: [] };
};

const aggregateInsightRows = (rows = []) => {
  const timeseriesMap = new Map();
  const summary = {
    reach: 0,
    impressions: 0,
    spend: 0,
    clicks: 0
  };

  for (const row of rows) {
    const date = String(row?.date_start || row?.date || '').trim();
    const reach = Number(row?.reach || 0);
    const impressions = Number(row?.impressions || 0);
    const spend = Number(row?.spend || 0);
    const clicks = Number(row?.clicks || 0);

    summary.reach += reach;
    summary.impressions += impressions;
    summary.spend += spend;
    summary.clicks += clicks;

    if (date) {
      const existing = timeseriesMap.get(date) || { date, reach: 0, spend: 0 };
      existing.reach += reach;
      existing.spend = Number((existing.spend + spend).toFixed(2));
      timeseriesMap.set(date, existing);
    }
  }

  const timeseries = [...timeseriesMap.values()].sort((left, right) => left.date.localeCompare(right.date));

  return {
    summary: {
      reach: Math.round(summary.reach),
      impressions: Math.round(summary.impressions),
      spend: Number(summary.spend.toFixed(2)),
      ctr: summary.impressions > 0 ? Number(((summary.clicks / summary.impressions) * 100).toFixed(2)) : 0
    },
    timeseries
  };
};

const aggregateDemographicsRows = (rows = []) => {
  const demographicsMap = new Map();

  for (const row of rows) {
    const age = String(row?.age || '').trim();
    const gender = String(row?.gender || '').trim().toLowerCase();
    const reach = Number(row?.reach || 0);
    if (!age) continue;

    const existing = demographicsMap.get(age) || { age, male: 0, female: 0 };
    if (gender === 'male') {
      existing.male += Math.round(reach);
    } else if (gender === 'female') {
      existing.female += Math.round(reach);
    }
    demographicsMap.set(age, existing);
  }

  const ageOrder = ['13-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+'];
  return [...demographicsMap.values()].sort(
    (left, right) => ageOrder.indexOf(left.age) - ageOrder.indexOf(right.age)
  );
};

const fetchInsightsFilters = async ({ userId } = {}) => {
  const { tokenCandidates, adAccounts } = await resolveInsightsAccess({ userId });
  if (!tokenCandidates.length || !adAccounts.length) {
    return {
      campaigns: [{ id: 'all', name: 'All Campaigns', adSets: [{ id: 'all', name: 'All Ad Sets' }] }]
    };
  }

  const campaignMap = new Map();
  const adSetMap = new Map();

  for (const account of adAccounts) {
    try {
      const campaignsResponse = await requestMetaAcrossTokens({
        path: buildAdAccountPath(account.id, 'campaigns'),
        params: {
          fields: 'id,name,effective_status,objective',
          limit: 100
        },
        tokenCandidates
      });

      for (const campaign of Array.isArray(campaignsResponse?.data) ? campaignsResponse.data : []) {
        const campaignId = String(campaign?.id || '').trim();
        if (!campaignId) continue;
        campaignMap.set(campaignId, {
          id: campaignId,
          name: String(campaign?.name || campaignId),
          objective: String(campaign?.objective || ''),
          status: String(campaign?.effective_status || ''),
          adSets: [{ id: 'all', name: 'All Ad Sets' }]
        });
      }
    } catch (error) {
      console.warn('[Meta Insights] Campaign filter load failed:', extractApiErrorMessage(error));
    }

    try {
      const adSetsResponse = await requestMetaAcrossTokens({
        path: buildAdAccountPath(account.id, 'adsets'),
        params: {
          fields: 'id,name,campaign_id',
          limit: 200
        },
        tokenCandidates
      });

      for (const adSet of Array.isArray(adSetsResponse?.data) ? adSetsResponse.data : []) {
        const adSetId = String(adSet?.id || '').trim();
        const campaignId = String(adSet?.campaign_id || '').trim();
        if (!adSetId || !campaignId) continue;

        const normalizedAdSet = {
          id: adSetId,
          name: String(adSet?.name || adSetId)
        };
        adSetMap.set(adSetId, normalizedAdSet);

        const existingCampaign = campaignMap.get(campaignId);
        if (existingCampaign) {
          existingCampaign.adSets = dedupeById([...(existingCampaign.adSets || []), normalizedAdSet]);
          campaignMap.set(campaignId, existingCampaign);
        }
      }
    } catch (error) {
      console.warn('[Meta Insights] Ad set filter load failed:', extractApiErrorMessage(error));
    }
  }

  return {
    campaigns: [
      { id: 'all', name: 'All Campaigns', adSets: [{ id: 'all', name: 'All Ad Sets' }] },
      ...[...campaignMap.values()].sort((left, right) => left.name.localeCompare(right.name))
    ]
  };
};

const fetchInsightsDashboard = async ({ userId, range = '30d', campaignId, adSetId } = {}) => {
  const datePreset = getInsightsDatePreset(range);

  if (shouldUseMockMode()) {
    const totalDays = range === '7d' ? 7 : range === '90d' ? 90 : 30;
    const timeseries = Array.from({ length: totalDays }, (_, index) => {
      const date = new Date();
      date.setDate(date.getDate() - (totalDays - index - 1));
      const reach = Math.round(4200 + index * 110 + Math.sin(index / 3) * 950);
      const spend = Number((reach * 0.0042).toFixed(2));
      return {
        date: date.toISOString().slice(0, 10),
        reach,
        spend
      };
    });

    const aggregated = aggregateInsightRows(
      timeseries.map((entry) => ({
        date_start: entry.date,
        reach: entry.reach,
        impressions: Math.round(entry.reach * 1.33),
        spend: entry.spend,
        clicks: Math.round(entry.reach * 0.022)
      }))
    );

    return {
      summary: aggregated.summary,
      timeseries,
      demographics: [
        { age: '13-17', male: 4200, female: 3900 },
        { age: '18-24', male: 15800, female: 12800 },
        { age: '25-34', male: 22100, female: 18400 },
        { age: '35-44', male: 12600, female: 10800 },
        { age: '45-54', male: 7200, female: 6400 },
        { age: '55-64', male: 3900, female: 3500 },
        { age: '65+', male: 1700, female: 1600 }
      ]
    };
  }

  const { tokenCandidates, adAccounts } = await resolveInsightsAccess({ userId });
  if (!tokenCandidates.length) {
    throw buildStageErrorWithDetails(
      'Insights',
      'Meta access token is not configured for insights.',
      { range, campaignId: campaignId || '', adSetId: adSetId || '' },
      400
    );
  }

  const insightRows = [];
  const demographicRows = [];
  const timeseriesErrors = [];
  const demographicErrors = [];
  const filtering = buildInsightsFilteringParam({ campaignId, adSetId });

  for (const account of adAccounts) {
    const insightsPath = buildAdAccountPath(account.id, 'insights');
    try {
      const timeseriesResponse = await requestMetaAcrossTokens({
        path: insightsPath,
        params: {
          fields: 'date_start,reach,impressions,spend,clicks',
          date_preset: datePreset,
          time_increment: 1,
          level: 'campaign',
          filtering: filtering || undefined,
          limit: 500
        },
        tokenCandidates
      });
      insightRows.push(...(Array.isArray(timeseriesResponse?.data) ? timeseriesResponse.data : []));
    } catch (error) {
      const message = extractApiErrorMessage(error);
      timeseriesErrors.push({ accountId: account.id, message });
      console.warn('[Meta Insights] Timeseries fetch failed:', message);
    }

    try {
      const demographicsResponse = await requestMetaAcrossTokens({
        path: insightsPath,
        params: {
          fields: 'reach',
          date_preset: datePreset,
          breakdowns: 'age,gender',
          level: 'campaign',
          filtering: filtering || undefined,
          limit: 500
        },
        tokenCandidates
      });
      demographicRows.push(...(Array.isArray(demographicsResponse?.data) ? demographicsResponse.data : []));
    } catch (error) {
      const message = extractApiErrorMessage(error);
      demographicErrors.push({ accountId: account.id, message });
      console.warn('[Meta Insights] Demographics fetch failed:', message);
    }
  }

  if (!insightRows.length && timeseriesErrors.length > 0) {
    throw buildStageErrorWithDetails(
      'Insights',
      'Meta insights request failed. Verify ad-account access, campaign permissions, and selected date range.',
      {
        range,
        campaignId: campaignId || '',
        adSetId: adSetId || '',
        timeseriesErrors: timeseriesErrors.slice(0, 5),
        demographicErrors: demographicErrors.slice(0, 5)
      },
      502
    );
  }

  const aggregated = aggregateInsightRows(insightRows);

  return {
    summary: aggregated.summary,
    timeseries: aggregated.timeseries,
    demographics: aggregateDemographicsRows(demographicRows),
    meta: {
      dataSource: 'meta-graph',
      hasData: aggregated.timeseries.length > 0,
      timeseriesRows: insightRows.length,
      demographicRows: demographicRows.length,
      warningCount: timeseriesErrors.length + demographicErrors.length
    }
  };
};

module.exports = {
  extractApiErrorMessage,
  buildStageError,
  buildStageErrorWithDetails,
  getEnvConfig,
  getAccessContextForUser,
  getUserAdAccounts,
  getSetupBundle,
  getConnectionDiagnostics,
  getAdPreviews,
  getPageLeads,
  getAdAccountBillingSummary,
  verifyMetaAdsManagementPermission,
  createMetaCampaignInAdsManager,
  fetchMetaCampaignsFromAdsManager,
  deleteMetaCampaignInAdsManager,
  exchangeCodeForAccessToken,
  getLoginDialogUrl,
  saveUserConnection,
  ensureUserConnectionRecord,
  saveUserSelections,
  saveUserAdAccountSelection,
  fetchInsightsFilters,
  fetchInsightsDashboard,
  fetchRemoteCampaigns,
  uploadCreativeAsset,
  createFullAdStack,
  createMetaAdStack,
  createMetaCampaignFromCrud,
  createMetaAdStackFromCrud,
  updateCampaign,
  pauseCampaign,
  resumeCampaign,
  archiveMetaCrudAssets,
  updateMetaCampaignFromCrud,
  updateMetaCrudDeliveryStatus,
  fetchInsights,
  fetchCampaignInsights,
  fetchCrudCampaignInsights,
  syncCampaignAnalyticsRecord,
  syncAllCampaignAnalytics,
  syncCrudCampaignAnalyticsRecord,
  syncAllCrudCampaignAnalytics,
  refreshCrudCampaignAnalytics,
  mapCrudDateRangeToMetaPreset,
  updateCampaignDeliveryStatus,
  getOrCreateWalletRecord,
  ensureWalletBalance,
  trackPixelEvent,
  shouldUseMockMode,
  normalizeAdAccountId,
  toCanonicalAdAccountId
};
