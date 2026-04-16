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
const normalizeAdAccountId = (value) => String(value || '').replace(/^act_/i, '');
const buildAdAccountPath = (adAccountId, resource = '') => {
  const normalizedId = normalizeAdAccountId(adAccountId);
  const cleanResource = String(resource || '').replace(/^\/+/, '');
  return cleanResource ? `act_${normalizedId}/${cleanResource}` : `act_${normalizedId}`;
};
const toCanonicalAdAccountId = (value) => {
  const normalizedId = normalizeAdAccountId(value);
  return normalizedId ? `act_${normalizedId}` : '';
};

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

const graphRequest = async ({ method = 'GET', path, params, data, headers, accessToken: overrideToken, apiVersion: overrideApiVersion }) => {
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

  const url = `${GRAPH_BASE_URL}/${(overrideApiVersion || apiVersion).replace(/^\/+/, '')}/${path.replace(/^\/+/, '')}`;
  const requestConfig = {
    url,
    method,
    params: {
      access_token: resolvedAccessToken,
      ...(params || {})
    },
    data,
    headers
  };

  console.log(
    '[Meta API]',
    JSON.stringify({
      method: requestConfig.method,
      path,
      hasToken: Boolean(requestConfig.params.access_token),
      params: Object.keys(params || {})
    })
  );

  try {
    const response = await axios(requestConfig);
    return response.data;
  } catch (error) {
    console.error(
      '[Meta API Error]',
      JSON.stringify({
        method: requestConfig.method,
        path,
        message: error?.response?.data?.error?.message || error.message,
        status: error?.response?.status || null,
        details: error?.response?.data || null
      })
    );
    throw error;
  }
};

const extractApiErrorMessage = (error) => {
  return (
    error?.response?.data?.error?.message ||
    error?.response?.data?.message ||
    error?.message ||
    'Meta API request failed'
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

const getAllowedOptimizationGoalsForCrudObjective = (objective) => {
  const normalizedObjective = String(objective || '').trim().toLowerCase();

  switch (normalizedObjective) {
    case 'traffic':
      return ['LINK_CLICKS', 'LANDING_PAGE_VIEWS', 'REACH', 'IMPRESSIONS'];
    case 'engagement':
      return ['POST_ENGAGEMENT', 'REACH', 'IMPRESSIONS'];
    case 'leads':
      return ['LEADS', 'QUALITY_LEAD', 'CONVERSATIONS'];
    case 'sales':
      return ['OFFSITE_CONVERSIONS', 'VALUE', 'LINK_CLICKS'];
    case 'awareness':
    default:
      return ['REACH', 'IMPRESSIONS'];
  }
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
  return wrappedError;
};

const buildStageErrorWithDetails = (stage, message, details, status = 400) => {
  const wrappedError = new Error(`${stage} failed: ${message}`);
  wrappedError.stage = stage;
  wrappedError.details = details || null;
  wrappedError.status = status;
  return wrappedError;
};

const shouldUseMockMode = () => {
  const config = getEnvConfig();
  return config.forceMock;
};

const getAccessContextForUser = async (userId) => metaAuthService.getAccessContextForUser(userId);

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
      pageId: connection?.selectedPageId || 'mock-page',
      selectedWhatsappNumber: connection?.selectedWhatsappNumber || '',
      pages: [
        { id: connection?.selectedPageId || '615785750230178', name: 'Technovo Demo Page' }
      ],
      businesses: [
        { id: 'mock-business-1', name: 'Technovo Demo Business' }
      ],
      adAccounts: [
        { id: connection?.selectedAdAccountId || 'act_mock_account', name: 'Technovo Demo Ad Account' }
      ],
      whatsappNumbers: [
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
      pageId: '',
      selectedWhatsappNumber: '',
      pages: [],
      businesses: [],
      adAccounts: [],
      whatsappNumbers: [],
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
      params: { fields: 'id,name,instagram_business_account{id,username}' },
      accessToken: accessContext.accessToken
    }),
    graphRequest({
      path: 'me/businesses',
      params: { fields: 'id,name' },
      accessToken: accessContext.accessToken
    }),
    graphRequest({
      path: 'me/adaccounts',
      params: { fields: 'id,name,account_status,currency,timezone_name' },
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
      ? adAccountsResult.value.data
      : [];
  if (adAccountsResult.status === 'rejected') {
    warnings.push(`Ad accounts: ${extractApiErrorMessage(adAccountsResult.reason)}`);
  }

  const fallbackAccessiblePageId = String(pages[0]?.id || '').trim();
  const requestedPageId = String(savedSelection.selectedPageId || fallbackAccessiblePageId || '').trim();
  const accessiblePageIds = new Set(
    pages.map((page) => String(page?.id || '').trim()).filter(Boolean)
  );
  let selectedPageId =
    (requestedPageId && accessiblePageIds.has(requestedPageId) ? requestedPageId : '') ||
    fallbackAccessiblePageId ||
    '';
  const selectedAdAccountId =
    savedSelection.selectedAdAccountId ||
    accessContext?.adminMetaConfig?.adAccountId ||
    adAccounts[0]?.id ||
    '';
  let whatsappNumbers = [];

  if (pageDetailsResult.status === 'fulfilled' && pageDetailsResult.value) {
    whatsappNumbers =
      pageDetailsResult.value?.whatsapp_business_account?.phone_numbers?.data ||
      pageDetailsResult.value?.whatsapp_business_account?.phone_numbers ||
      [];
    selectedPageId = String(pageDetailsResult.value?.id || selectedPageId || '').trim();
  } else if (pageDetailsResult.status === 'rejected') {
    warnings.push(`Page details: ${extractApiErrorMessage(pageDetailsResult.reason)}`);
  }

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

  if (userId && selectedPageId && selectedPageId !== String(savedSelection.selectedPageId || '').trim()) {
    await MetaAdsConnection.updateOne(
      { userId },
      {
        $set: {
          selectedPageId,
          lastValidatedAt: new Date()
        }
      }
    );
  }

  if (!selectedPageId && requestedPageId) {
    warnings.push('Page access: Reconnect Facebook and grant page access so a valid Facebook Page can be used for ad creatives.');
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
      pageId: selectedPageId,
      selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || whatsappNumbers[0]?.display_phone_number || '',
      pages,
      businesses,
      adAccounts,
      whatsappNumbers,
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
      pageId: savedSelection.selectedPageId || '',
      selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || '',
      pages,
      businesses,
      adAccounts,
      whatsappNumbers,
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
    selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || '',
    pages,
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
    adAccountId: selectedAdAccountId,
    apiVersion: env.apiVersion,
    graphBaseUrl: GRAPH_BASE_URL
  };

  return diagnostics;
};

const exchangeCodeForAccessToken = async ({ code, redirectUri, appId, appSecret, apiVersion }) =>
  metaAuthService.exchangeCodeForAccessToken({ code, redirectUri, appId, appSecret, apiVersion });

const getUserAdAccounts = async ({ userId } = {}) => {
  if (shouldUseMockMode()) {
    const setup = await getSetupBundle({ userId });
    return setup.adAccounts || [];
  }

  const accessContext = await ensureConnectedMetaUser(userId, 'Meta account selection');
  const response = await graphRequest({
    path: 'me/adaccounts',
    params: { fields: 'id,name,account_status,currency,timezone_name' },
    accessToken: accessContext.accessToken
  });

  return Array.isArray(response?.data) ? response.data : [];
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
  const accountIds = [...adAccountCandidates.keys()];

  for (const adAccountId of accountIds) {
    let response = null;

    for (const accessToken of tokenCandidates) {
      try {
        response = await graphRequest({
          path: buildAdAccountPath(adAccountId, 'campaigns'),
          params: {
            fields:
              'id,name,status,effective_status,objective,daily_budget,lifetime_budget,start_time,stop_time,created_time,updated_time,insights.limit(1){impressions,clicks,spend,ctr,cpc}',
            limit: 100
          },
          accessToken
        });
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

    const campaigns = Array.isArray(response?.data) ? response.data : [];
    for (const campaign of campaigns) {
      const remoteId = String(campaign?.id || '').trim();
      if (!remoteId || remoteCampaignMap.has(remoteId)) continue;

      const insight = Array.isArray(campaign?.insights?.data) ? campaign.insights.data[0] || {} : {};
      remoteCampaignMap.set(remoteId, {
        _id: `meta_${remoteId}`,
        id: `meta_${remoteId}`,
        source: 'meta',
        readOnly: true,
        syncedFromMeta: true,
        metaCampaignId: remoteId,
        metaAdAccountId: adAccountId,
        name: String(campaign?.name || `Meta Campaign ${remoteId}`),
        platform: 'both',
        objective: mapMetaObjectiveToCrudObjective(campaign?.objective),
        status: mapMetaStatusToCrudStatus(campaign?.status, campaign?.effective_status),
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
    updates.selectedAdAccountId = String(adAccountId || '');
  }
  if (pageId !== undefined) {
    const normalizedPageId = String(pageId || '').trim();
    if (normalizedPageId) {
      const accessiblePages = await getAccessiblePages({ accessToken: decryptMetaToken(existingConnection.accessToken) });
      const matchedPage = accessiblePages.find((page) => String(page?.id || '').trim() === normalizedPageId);
      if (!matchedPage) {
        const error = new Error('The selected Facebook Page is not available for this Facebook login. Reconnect Facebook and grant page access, then try again.');
        error.status = 400;
        throw error;
      }
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

const createFullAdStack = async ({ campaign, creativeUpload, userId }) => {
  const env = getEnvConfig();
  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign creation');
  let resolvedAccessToken = accessContext.accessToken;
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
  const startTime = campaign.schedule?.startTime ? new Date(campaign.schedule.startTime).toISOString() : new Date().toISOString();
  const endTime = campaign.schedule?.endTime ? new Date(campaign.schedule.endTime).toISOString() : undefined;
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
        special_ad_categories: []
      },
      accessToken: resolvedAccessToken
    });
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
    response = await graphRequest({
      path: buildAdAccountPath(effectiveAdAccountId, 'insights'),
      params: {
        fields: 'impressions,reach,clicks,spend,ctr,cpc,actions',
        filtering: JSON.stringify([
          { field: 'campaign.id', operator: 'EQUAL', value: campaign.meta.campaignId }
        ]),
        limit: 1
      },
      accessToken: accessContext.accessToken
    });
  } catch (error) {
    throw buildStageError('Insights sync', error);
  }

  const row = Array.isArray(response?.data) ? response.data[0] : null;
  const actions = Array.isArray(row?.actions) ? row.actions : [];
  const leadAction = actions.find((item) => String(item.action_type || '').includes('lead'));
  const leads = Number(leadAction?.value || 0);

  return {
    impressions: Number(row?.impressions || 0),
    reach: Number(row?.reach || 0),
    clicks: Number(row?.clicks || 0),
    leads,
    spend: Number(row?.spend || 0),
    ctr: Number(row?.ctr || 0),
    cpc: Number(row?.cpc || 0),
    cpl: leads ? Number((Number(row?.spend || 0) / leads).toFixed(2)) : 0,
    lastSyncedAt: new Date()
  };
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

const createMetaCampaignFromCrud = async ({ userId, name, objective, status }) => {
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
      accessToken: accessContext.accessToken
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
  status
}) => {
  const env = getEnvConfig();
  const accessContext = await ensureConnectedMetaUser(userId, 'Campaign creation');
  const normalizedStatus = String(status || '').trim().toUpperCase() === 'ACTIVE' ? 'ACTIVE' : 'PAUSED';
  const allowedOptimizationGoals = getAllowedOptimizationGoalsForCrudObjective(objective);
  const normalizedOptimizationGoal = String(optimizationGoal || '').trim().toUpperCase();
  const resolvedOptimizationGoal = allowedOptimizationGoals.includes(normalizedOptimizationGoal)
    ? normalizedOptimizationGoal
    : getDefaultOptimizationGoalForCrudObjective(objective);
  const genders = [];

  if (String(gender || '').toLowerCase() === 'male') genders.push(1);
  if (String(gender || '').toLowerCase() === 'female') genders.push(2);

  const configuredPageId = accessContext.connection?.selectedPageId;
  const selectedAdAccountId = accessContext.connection?.selectedAdAccountId;
  if (!configuredPageId) {
    throw buildStageErrorWithDetails(
      'Campaign creation',
      'Select a Facebook Page for this user before publishing campaigns.',
      { userId: userId || '' },
      400
    );
  }
  if (!selectedAdAccountId) {
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
          accessToken: accessContext.accessToken,
          terms: interestTerms,
          type: 'adinterest'
        })
      : Promise.resolve([]),
    behaviorTerms.length
      ? resolveMetaTargetingEntries({
          accessToken: accessContext.accessToken,
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
    adAccountId: selectedAdAccountId
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

  const stack = await createFullAdStack({
    userId,
    creativeUpload,
    campaign: {
      campaignName,
      objective: mapCrudObjectiveToMetaObjective(objective),
      status: normalizedStatus,
      platform: ['facebook', 'instagram', 'both'].includes(String(platform || '').toLowerCase())
        ? String(platform || '').toLowerCase()
        : 'both',
      configuredPageId,
      budget: {
        dailyBudget: Math.max(0, Number(resolvedDailyBudget || 0)),
        lifetimeBudget: Math.max(0, Number(resolvedLifetimeBudget || 0)),
        currency: 'INR'
      },
      targeting: {
        countries: parseTargetingCountriesFromCrud(targeting),
        ageMin: Math.max(13, Number(ageMin || 18)),
        ageMax: Math.min(65, Number(ageMax || 65)),
        genders,
        interests: resolvedInterests,
        behaviors: resolvedBehaviors
      },
      creative: {
        primaryText: String(primaryText || campaignName).trim(),
        headline: String(headline || campaignName).trim(),
        description: String(description || '').trim(),
        callToAction: String(callToAction || 'LEARN_MORE').trim().toUpperCase(),
        mediaType: normalizedMediaType,
        mediaUrl:
          creativeUpload.mediaUrl ||
          (normalizedMediaType === 'video' ? videoUrl : imageUrl) ||
          '',
        mediaHash: creativeUpload.mediaHash,
        videoId: creativeUpload.videoId || ''
      },
      schedule: {
        startTime: startDate || undefined,
        endTime: endDate || undefined
      },
      metaOverrides: {
        optimizationGoal: resolvedOptimizationGoal,
        bidStrategy: String(bidStrategy || env.bidStrategy || 'LOWEST_COST_WITHOUT_CAP')
          .trim()
          .toUpperCase(),
        destinationUrl: String(destinationUrl || '').trim()
      }
    }
  });

  return {
    ...stack,
    imageHash: creativeUpload.mediaHash,
    videoId: creativeUpload.videoId || '',
    mediaType: normalizedMediaType,
    status: normalizedStatus,
    destinationUrl:
      String(destinationUrl || stack.destinationUrl || `https://www.facebook.com/${configuredPageId || ''}`).trim(),
    pageId: configuredPageId || ''
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
    response = await graphRequest({
      path: buildAdAccountPath(effectiveAdAccountId, 'insights'),
      params: {
        fields: 'impressions,reach,clicks,spend,ctr,cpc,actions',
        date_preset: mapCrudDateRangeToMetaPreset(range),
        filtering: JSON.stringify([
          { field: 'campaign.id', operator: 'EQUAL', value: String(campaign.metaCampaignId) }
        ]),
        limit: 1
      },
      accessToken: accessContext.accessToken
    });
  } catch (error) {
    throw buildStageError('Insights sync', error);
  }

  const row = Array.isArray(response?.data) ? response.data[0] : null;
  const actions = Array.isArray(row?.actions) ? row.actions : [];
  const leadAction = actions.find((item) => String(item.action_type || '').includes('lead'));
  const leads = Number(leadAction?.value || 0);

  return {
    impressions: Number(row?.impressions || 0),
    reach: Number(row?.reach || 0),
    clicks: Number(row?.clicks || 0),
    spend: Number(row?.spend || 0),
    ctr: Number(row?.ctr || 0),
    cpc: Number(row?.cpc || 0),
    leads,
    cpl: leads ? Number((Number(row?.spend || 0) / leads).toFixed(2)) : 0,
    lastSyncedAt: new Date()
  };
};

const syncCrudCampaignAnalyticsRecord = async ({ campaign, userId, range = 'last30days' }) => {
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

  return {
    campaign: persistedCampaign,
    insights: latestInsights
  };
};

const syncAllCrudCampaignAnalytics = async ({ userId } = {}) => {
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
    warnings: []
  };

  for (const campaign of campaigns) {
    try {
      await syncCrudCampaignAnalyticsRecord({
        campaign,
        userId: String(campaign.createdBy || userId || '')
      });
      results.synced += 1;
    } catch (error) {
      results.warnings.push({
        campaignId: String(campaign._id || ''),
        campaignName: String(campaign.name || ''),
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
    range
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
