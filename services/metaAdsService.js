const axios = require('axios');
const FormData = require('form-data');
const MetaAdCampaign = require('../models/MetaAdCampaign');
const MetaAdsConnection = require('../models/MetaAdsConnection');
const MetaAdsTransaction = require('../models/MetaAdsTransaction');
const MetaAdsWallet = require('../models/MetaAdsWallet');
const { decryptMetaToken, encryptMetaToken } = require('../utils/metaTokenCrypto');

const GRAPH_BASE_URL = 'https://graph.facebook.com';

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

const getEnvConfig = () => {
  const accessToken = process.env.META_ACCESS_TOKEN || '';
  const apiVersion = process.env.META_API_VERSION || 'v22.0';
  const adAccountId = process.env.META_AD_ACCOUNT_ID || '';
  const pageId = process.env.META_PAGE_ID || '';
  const pixelId = process.env.META_PIXEL_ID || '';
  const appId = process.env.META_APP_ID || '';
  const appSecret = process.env.META_APP_SECRET || '';
  const forceMock = String(process.env.META_ADS_FORCE_MOCK || 'false').toLowerCase() === 'true';
  const advantageAudience = String(process.env.META_ADVANTAGE_AUDIENCE || '0') === '1' ? 1 : 0;
  const bidStrategy = String(process.env.META_DEFAULT_BID_STRATEGY || 'LOWEST_COST_WITH_BID_CAP').trim();
  const bidAmount = Number(process.env.META_BID_AMOUNT || 5000);

  return {
    accessToken,
    apiVersion,
    adAccountId,
    pageId,
    pixelId,
    appId,
    appSecret,
    forceMock,
    advantageAudience,
    bidStrategy,
    bidAmount,
    hasCredentials: Boolean(accessToken && adAccountId)
  };
};

const graphRequest = async ({ method = 'GET', path, params, data, headers, accessToken: overrideToken, apiVersion: overrideApiVersion }) => {
  const { apiVersion, accessToken } = getEnvConfig();
  const url = `${GRAPH_BASE_URL}/${apiVersion}/${path.replace(/^\/+/, '')}`;
  const response = await axios({
    url,
    method,
    params: {
      access_token: overrideToken || accessToken,
      ...(params || {})
    },
    data,
    headers
  });
  return response.data;
};

const extractApiErrorMessage = (error) => {
  return (
    error?.response?.data?.error?.message ||
    error?.response?.data?.message ||
    error?.message ||
    'Meta API request failed'
  );
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
  return config.forceMock || !config.hasCredentials;
};

const getAccessContextForUser = async (userId) => {
  const env = getEnvConfig();
  if (!userId) {
    return {
      accessToken: env.accessToken,
      apiVersion: env.apiVersion,
      source: 'env'
    };
  }

  const connection = await MetaAdsConnection.findOne({ userId }).lean();
  if (connection?.accessToken) {
    return {
      accessToken: decryptMetaToken(connection.accessToken),
      apiVersion: env.apiVersion,
      source: 'user',
      connection
    };
  }

  return {
    accessToken: env.accessToken,
    apiVersion: env.apiVersion,
    source: 'env'
  };
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
  if (interests.length) {
    result.flexible_spec = [{ interests }];
  }
  if (customAudienceIds.length) {
    result.custom_audiences = customAudienceIds.map((id) => ({ id }));
  }

  return result;
};

const buildPlacement = (placement = {}) => {
  const publisherPlatforms = normalizeArray(placement.publisherPlatforms);
  const facebookPositions = normalizeArray(placement.facebookPositions);
  const instagramPositions = normalizeArray(placement.instagramPositions);

  return {
    publisher_platforms: publisherPlatforms.length ? publisherPlatforms : ['facebook', 'instagram'],
    facebook_positions: facebookPositions.length ? facebookPositions : ['feed', 'marketplace', 'video_feeds'],
    instagram_positions: instagramPositions.length ? instagramPositions : ['stream', 'story', 'reels']
  };
};

const sanitizeWhatsappNumber = (value) =>
  String(value || '')
    .replace(/[^\d]/g, '')
    .trim();

const buildCreativeDestination = ({ whatsappNumber, pageId }) => {
  const sanitizedWhatsapp = sanitizeWhatsappNumber(whatsappNumber);
  if (sanitizedWhatsapp) {
    return {
      whatsappNumber: sanitizedWhatsapp,
      destinationUrl: `https://wa.me/${sanitizedWhatsapp}`
    };
  }

  if (pageId) {
    return {
      whatsappNumber: '',
      destinationUrl: `https://www.facebook.com/${pageId}`
    };
  }

  return {
    whatsappNumber: '',
    destinationUrl: 'https://www.facebook.com/'
  };
};

const getAccessiblePages = async ({ accessToken }) => {
  const response = await graphRequest({
    path: 'me/accounts',
    params: { fields: 'id,name' },
    accessToken
  });

  return Array.isArray(response?.data) ? response.data : [];
};

const resolveCreativePageContext = async ({ requestedPageId, accessToken }) => {
  const normalizedRequestedPageId = String(requestedPageId || '').trim();
  const accessiblePages = await getAccessiblePages({ accessToken });

  if (!accessiblePages.length) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      'No accessible Facebook pages were found for this Meta token.',
      {
        requestedPageId: normalizedRequestedPageId,
        accessiblePages: []
      },
      400
    );
  }

  const matchedPage =
    accessiblePages.find((page) => String(page?.id || '') === normalizedRequestedPageId) ||
    accessiblePages[0];

  if (!matchedPage?.id) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      'The selected Facebook page is not available for this Meta token.',
      {
        requestedPageId: normalizedRequestedPageId,
        accessiblePages
      },
      400
    );
  }

  return {
    pageId: String(matchedPage.id),
    pageName: String(matchedPage.name || ''),
    requestedPageId: normalizedRequestedPageId,
    accessiblePages
  };
};

const uploadCreativeAsset = async ({ fileBuffer, fileName, mediaUrl, userId, adAccountId }) => {
  if (!fileBuffer && !mediaUrl) {
    return { mediaHash: '', mediaUrl: '' };
  }

  if (shouldUseMockMode()) {
    return {
      mediaHash: `mock_${Date.now()}`,
      mediaUrl: mediaUrl || `mock://${fileName || 'upload'}`
    };
  }

  const accessContext = await getAccessContextForUser(userId);
  const effectiveAdAccountId =
    adAccountId ||
    accessContext.connection?.selectedAdAccountId ||
    getEnvConfig().adAccountId;

  if (mediaUrl) {
    const response = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'adimages'),
      data: { url: mediaUrl },
      accessToken: accessContext.accessToken
    });
    const image = response?.images ? Object.values(response.images)[0] : null;
    return {
      mediaHash: image?.hash || '',
      mediaUrl
    };
  }

  const form = new FormData();
  form.append('filename', fileBuffer, { filename: fileName || `creative-${Date.now()}.jpg` });

  const response = await graphRequest({
    method: 'POST',
    path: buildAdAccountPath(effectiveAdAccountId, 'adimages'),
    data: form,
    headers: form.getHeaders(),
    accessToken: accessContext.accessToken
  });
  const image = response?.images ? Object.values(response.images)[0] : null;

  return {
    mediaHash: image?.hash || '',
    mediaUrl: ''
  };
};

const getSetupBundle = async ({ userId } = {}) => {
  if (shouldUseMockMode()) {
    const env = getEnvConfig();
    const connection = userId ? await MetaAdsConnection.findOne({ userId }).lean() : null;
    const envAdAccountId = toCanonicalAdAccountId(env.adAccountId);
    return {
      mode: 'mock',
      connected: true,
      adAccountId: connection?.selectedAdAccountId || envAdAccountId || 'mock-ad-account',
      pageId: connection?.selectedPageId || env.pageId || 'mock-page',
      selectedWhatsappNumber: connection?.selectedWhatsappNumber || '',
      pages: [
        { id: connection?.selectedPageId || env.pageId || '615785750230178', name: 'Technovo Demo Page' }
      ],
      businesses: [
        { id: 'mock-business-1', name: 'Technovo Demo Business' }
      ],
      adAccounts: [
        { id: connection?.selectedAdAccountId || envAdAccountId || 'act_mock_account', name: 'Technovo Demo Ad Account' }
      ],
      whatsappNumbers: [
        { id: 'mock-waba-1', display_phone_number: connection?.selectedWhatsappNumber || '+91 98765 43210' }
      ]
    };
  }

  const env = getEnvConfig();
  const envAdAccountId = toCanonicalAdAccountId(env.adAccountId);
  const accessContext = await getAccessContextForUser(userId);
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
    (savedSelection.selectedPageId || env.pageId)
      ? graphRequest({
          path: savedSelection.selectedPageId || env.pageId,
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

  const requestedPageId = String(savedSelection.selectedPageId || env.pageId || '').trim();
  const accessiblePageIds = new Set(
    pages.map((page) => String(page?.id || '').trim()).filter(Boolean)
  );
  let selectedPageId =
    (requestedPageId && accessiblePageIds.has(requestedPageId) ? requestedPageId : '') ||
    String(pages[0]?.id || '').trim() ||
    '';
  const selectedAdAccountId = savedSelection.selectedAdAccountId || adAccounts[0]?.id || envAdAccountId || '';
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

  if (!selectedPageId && requestedPageId) {
    warnings.push('Page access: Reconnect Facebook and grant page access so a valid Facebook Page can be used for ad creatives.');
  }

  const hasAnyLiveData = Boolean(pages.length || businesses.length || adAccounts.length || whatsappNumbers.length || selectedAdAccountId);
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

  const fallback = {
    mode: 'mock',
    connected: false,
    adAccountId: savedSelection.selectedAdAccountId || envAdAccountId || 'mock-ad-account',
    pageId: savedSelection.selectedPageId || env.pageId || 'mock-page',
    selectedWhatsappNumber: savedSelection.selectedWhatsappNumber || '',
    pages: pages.length ? pages : [],
    businesses: [{ id: 'mock-business-1', name: 'Technovo Demo Business' }],
    adAccounts: [{ id: savedSelection.selectedAdAccountId || envAdAccountId || 'act_mock_account', name: 'Technovo Demo Ad Account' }],
    whatsappNumbers: [{ id: 'mock-waba-1', display_phone_number: savedSelection.selectedWhatsappNumber || '+91 98765 43210' }],
    setupError: warnings.join(' | ') || 'Meta setup could not be loaded with the current credentials.',
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
    env.pageId
      ? graphRequest({
          path: env.pageId,
          params: { fields: 'id,name,whatsapp_business_account{id,name,phone_numbers{display_phone_number,id}}' },
          accessToken: accessContext.accessToken
        })
      : Promise.reject(new Error('META_PAGE_ID is not configured')),
    env.adAccountId
      ? graphRequest({
          path: buildAdAccountPath(env.adAccountId),
          params: { fields: 'id,name,account_status,currency,timezone_name' },
          accessToken: accessContext.accessToken
        })
      : Promise.reject(new Error('META_AD_ACCOUNT_ID is not configured'))
  ]);

  const [meResult, businessesResult, pagesResult, pageDetailsResult, adAccountResult] = checks;

  const diagnostics = {
    env: {
      apiVersion: env.apiVersion,
      hasAccessToken: Boolean(accessContext.accessToken),
      hasAdAccountId: Boolean(env.adAccountId),
      hasPageId: Boolean(env.pageId),
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
    pageId: env.pageId || '',
    adAccountId: env.adAccountId || '',
    apiVersion: env.apiVersion,
    graphBaseUrl: GRAPH_BASE_URL
  };

  return diagnostics;
};

const exchangeCodeForAccessToken = async ({ code, redirectUri }) => {
  const env = getEnvConfig();
  const response = await axios.get(`${GRAPH_BASE_URL}/${env.apiVersion}/oauth/access_token`, {
    params: {
      client_id: env.appId || process.env.META_APP_ID,
      client_secret: env.appSecret || process.env.META_APP_SECRET,
      redirect_uri: redirectUri,
      code
    }
  });
  return response.data;
};

const getUserAdAccounts = async ({ userId } = {}) => {
  if (shouldUseMockMode()) {
    const setup = await getSetupBundle({ userId });
    return setup.adAccounts || [];
  }

  const accessContext = await getAccessContextForUser(userId);
  const response = await graphRequest({
    path: 'me/adaccounts',
    params: { fields: 'id,name,account_status,currency,timezone_name' },
    accessToken: accessContext.accessToken
  });

  return Array.isArray(response?.data) ? response.data : [];
};

const getLoginDialogUrl = ({ redirectUri, state }) => {
  const env = getEnvConfig();
  const scopes = [
    'public_profile',
    'email',
    'business_management',
    'ads_management',
    'ads_read',
    'pages_show_list',
    'pages_read_engagement',
    'pages_manage_metadata'
  ].join(',');

  const params = new URLSearchParams({
    client_id: process.env.META_APP_ID,
    redirect_uri: redirectUri,
    scope: scopes,
    response_type: 'code',
    state
  });

  return `https://www.facebook.com/${env.apiVersion}/dialog/oauth?${params.toString()}`;
};

const saveUserConnection = async ({ userId, accessToken, scopes = [] }) => {
  const profile = await graphRequest({
    path: 'me',
    params: { fields: 'id,name' },
    accessToken
  });

  const connection = await MetaAdsConnection.findOneAndUpdate(
    { userId },
    {
      userId,
      platformUserId: String(profile?.id || ''),
      name: String(profile?.name || ''),
      accessToken: encryptMetaToken(accessToken),
      scopes,
      connectedAt: new Date(),
      lastValidatedAt: new Date()
    },
    {
      new: true,
      upsert: true,
      setDefaultsOnInsert: true
    }
  );

  return connection;
};

const ensureUserConnectionRecord = async ({ userId } = {}) => {
  if (!userId) return null;

  const existingConnection = await MetaAdsConnection.findOne({ userId });
  if (existingConnection) return existingConnection;

  const accessContext = await getAccessContextForUser(userId);
  if (!accessContext?.accessToken) return null;

  try {
    return await saveUserConnection({
      userId,
      accessToken: accessContext.accessToken,
      scopes: accessContext.connection?.scopes || []
    });
  } catch {
    return MetaAdsConnection.findOneAndUpdate(
      { userId },
      {
        userId,
        accessToken: encryptMetaToken(accessContext.accessToken),
        connectedAt: new Date(),
        lastValidatedAt: new Date()
      },
      {
        new: true,
        upsert: true,
        setDefaultsOnInsert: true
      }
    );
  }
};

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

const createMetaAdStack = async ({ campaign, creativeUpload, userId }) => {
  const env = getEnvConfig();
  const accessContext = await getAccessContextForUser(userId);
  const effectiveAdAccountId =
    campaign?.adAccountId ||
    accessContext.connection?.selectedAdAccountId ||
    env.adAccountId;

  if (shouldUseMockMode()) {
    const now = Date.now();
    return {
      apiMode: 'mock',
      adAccountId: effectiveAdAccountId || `mock-ad-account-${now}`,
      campaignId: `mock-campaign-${now}`,
      adSetId: `mock-adset-${now}`,
      creativeId: `mock-creative-${now}`,
      adId: `mock-ad-${now}`,
      mediaHash: creativeUpload?.mediaHash || `mock_${now}`
    };
  }

  const objective = campaign.objective || 'OUTCOME_LEADS';
  const deliveryObjective = objective === 'OUTCOME_LEADS' ? 'OUTCOME_TRAFFIC' : objective;
  const requestedPageId = campaign.configuredPageId || accessContext.connection?.selectedPageId || env.pageId;
  const creativePageContext = await resolveCreativePageContext({
    requestedPageId,
    accessToken: accessContext.accessToken
  });
  const configuredPageId = creativePageContext.pageId;
  const instagramActorId = campaign.configuredInstagramActorId || undefined;
  const { whatsappNumber: sanitizedWhatsappNumber, destinationUrl } = buildCreativeDestination({
    whatsappNumber: campaign.whatsappNumber,
    pageId: configuredPageId
  });
  const targeting = buildTargeting(campaign.targeting);
  const placement = buildPlacement(campaign.placement);
  const dailyBudget = Math.max(100, Number(campaign.budget?.dailyBudget || 500));
  const startTime = campaign.schedule?.startTime ? new Date(campaign.schedule.startTime).toISOString() : new Date().toISOString();
  const endTime = campaign.schedule?.endTime ? new Date(campaign.schedule.endTime).toISOString() : undefined;

  let createdCampaign;
  try {
    createdCampaign = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'campaigns'),
      data: {
        name: campaign.campaignName,
        objective: deliveryObjective,
        status: 'PAUSED',
        special_ad_categories: []
      },
      accessToken: accessContext.accessToken
    });
  } catch (error) {
    throw buildStageError('Campaign creation', error);
  }

  const adSetPayload = {
    name: `${campaign.campaignName} - Ad Set`,
    campaign_id: createdCampaign.id,
    daily_budget: Math.round(dailyBudget * 100),
    billing_event: 'IMPRESSIONS',
    optimization_goal:
      objective === 'OUTCOME_TRAFFIC'
        ? 'LINK_CLICKS'
        : objective === 'OUTCOME_ENGAGEMENT'
          ? 'REACH'
          : 'LINK_CLICKS',
    bid_strategy: env.bidStrategy || 'LOWEST_COST_WITH_BID_CAP',
    bid_amount: Number.isFinite(env.bidAmount) && env.bidAmount > 0 ? env.bidAmount : 5000,
    targeting,
    status: 'PAUSED',
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
        bid_amount: undefined
      },
      {
        ...adSetPayload,
        bid_strategy: 'LOWEST_COST_WITH_BID_CAP',
        bid_amount: Number.isFinite(env.bidAmount) && env.bidAmount > 0 ? env.bidAmount : 5000
      },
      {
        ...adSetPayload,
        optimization_goal: 'REACH',
        bid_strategy: 'LOWEST_COST_WITHOUT_CAP',
        bid_amount: undefined
      },
      {
        name: `${campaign.campaignName} - Ad Set`,
        campaign_id: createdCampaign.id,
        daily_budget: Math.round(dailyBudget * 100),
        billing_event: 'IMPRESSIONS',
        optimization_goal: 'REACH',
        bid_strategy: 'LOWEST_COST_WITH_BID_CAP',
        bid_amount: Number.isFinite(env.bidAmount) && env.bidAmount > 0 ? env.bidAmount : 5000,
        targeting,
        status: 'PAUSED',
        start_time: startTime,
        end_time: endTime
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
          accessToken: accessContext.accessToken
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

  const requestedCtaType = String(campaign.creative?.callToAction || 'WHATSAPP_MESSAGE').trim();
  const effectiveCtaType =
    requestedCtaType === 'WHATSAPP_MESSAGE' && !sanitizedWhatsappNumber
      ? 'LEARN_MORE'
      : requestedCtaType;

  const callToActionValue =
    effectiveCtaType === 'WHATSAPP_MESSAGE'
      ? {
          app_destination: 'WHATSAPP',
          link: destinationUrl,
          page_welcome_message: campaign.creative?.primaryText || campaign.campaignName || 'Start a conversation on WhatsApp'
        }
      : {
          link: destinationUrl
        };

  const objectStorySpec = {
    page_id: configuredPageId,
    link_data: {
      link: destinationUrl,
      message: campaign.creative?.primaryText || campaign.campaignName || 'Learn more',
      name: campaign.creative?.headline || campaign.campaignName,
      description: campaign.creative?.description || '',
      call_to_action: {
        type: effectiveCtaType,
        value: callToActionValue
      }
    }
  };

  if (creativeUpload?.mediaHash) {
    objectStorySpec.link_data.image_hash = creativeUpload.mediaHash;
  }
  if (instagramActorId) {
    objectStorySpec.instagram_actor_id = instagramActorId;
  }

  let createdCreative;
  try {
    createdCreative = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'adcreatives'),
      data: {
        name: `${campaign.campaignName} - Creative`,
        object_story_spec: objectStorySpec
      },
      accessToken: accessContext.accessToken
    });
  } catch (error) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      extractApiErrorMessage(error),
      {
        metaError: error?.response?.data || null,
        requestedPageId: creativePageContext.requestedPageId,
        resolvedPageId: creativePageContext.pageId,
        resolvedPageName: creativePageContext.pageName,
        accessiblePages: creativePageContext.accessiblePages
      },
      error?.response?.status || 400
    );
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
      accessToken: accessContext.accessToken
    });
  } catch (error) {
    throw buildStageError('Ad creation', error);
  }

  return {
    apiMode: 'live',
    adAccountId: effectiveAdAccountId,
    campaignId: createdCampaign.id,
    adSetId: createdAdSet.id,
    creativeId: createdCreative.id,
    adId: createdAd.id,
    mediaHash: creativeUpload?.mediaHash || ''
  };
};

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
  const accessContext = await getAccessContextForUser(campaign.userId);
  const effectiveAdAccountId = campaign?.meta?.adAccountId || accessContext.connection?.selectedAdAccountId || getEnvConfig().adAccountId;
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

  const accessContext = await getAccessContextForUser(userId || campaign?.userId);

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
  uploadCreativeAsset,
  createMetaAdStack,
  fetchCampaignInsights,
  syncCampaignAnalyticsRecord,
  syncAllCampaignAnalytics,
  updateCampaignDeliveryStatus,
  shouldUseMockMode,
  normalizeAdAccountId,
  toCanonicalAdAccountId
};
