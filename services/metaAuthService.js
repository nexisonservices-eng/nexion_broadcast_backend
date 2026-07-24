const axios = require('axios');
const { getMetaAdsConfig, CANONICAL_META_OAUTH_REDIRECT_URI } = require('../config/metaAdsConfig');

const MetaAdsConnection = require('../models/MetaAdsConnection');
const User = require('../models/User');
const { decryptMetaToken, encryptMetaToken } = require('../utils/metaTokenCrypto');
const { getMetaConfigByUserId } = require('./userMetaCredentialsService');

const GRAPH_BASE_URL = 'https://graph.facebook.com';
const FALLBACK_META_OAUTH_REDIRECT_URI =
  'https://nexion-broadcast-backend-t4u8.onrender.com/api/meta-ads/oauth/callback';

const normalizeAdAccountId = (value) => {
  const raw = String(value || '')
    .trim()
    .replace(/\s+/g, '')
    .replace(/^(?:act_)+/i, '');

  return raw ? `act_${raw}` : '';
};

const hasMetaAppCredentials = (metaConfig = {}) =>
  Boolean(String(metaConfig?.appId || '').trim() && String(metaConfig?.appSecret || '').trim());

const resolveMetaConfigFromHierarchy = async ({ userId, metaConfig } = {}) => {
  if (hasMetaAppCredentials(metaConfig)) {
    return metaConfig;
  }

  const visited = new Set();
  let currentUserId = String(userId || '').trim();

  while (currentUserId && !visited.has(currentUserId)) {
    visited.add(currentUserId);

    const directConfig = await getMetaConfigByUserId(currentUserId);
    if (hasMetaAppCredentials(directConfig)) {
      return directConfig;
    }

    let userDoc = null;
    try {
      userDoc = await User.findById(currentUserId).select('parentAdminId').lean();
    } catch {
      userDoc = null;
    }

    const parentAdminId = String(userDoc?.parentAdminId || '').trim();
    if (!parentAdminId || visited.has(parentAdminId)) {
      break;
    }

    currentUserId = parentAdminId;
  }

  return null;
};

const resolveMetaOAuthRedirectUri = (redirectUri) => {
  const canonicalRedirectUri =
    String(CANONICAL_META_OAUTH_REDIRECT_URI || '').trim() ||
    String(getMetaAdsConfig().redirectUri || '').trim() ||
    FALLBACK_META_OAUTH_REDIRECT_URI;
  const configuredRedirectUri = String(redirectUri || '').trim().replace(/\/+$/, '');

  if (configuredRedirectUri && configuredRedirectUri !== canonicalRedirectUri) {
    console.warn(
      '[Meta OAuth] Ignoring non-canonical redirect URI passed to auth helper.',
      JSON.stringify({
        configuredRedirectUri,
        canonicalRedirectUri
      })
    );
  }

  return canonicalRedirectUri;
};

const getAccessContextForUser = async (userId) => {
  const env = getMetaAdsConfig();
  if (!userId) {
    return {
      accessToken: null,
      apiVersion: env.apiVersion,
      source: 'none',
      connection: null
    };
  }

  const resolveConnectionAccessContext = (connection, source) => {
    const decryptedToken = decryptMetaToken(connection?.accessToken || '');
    if (!decryptedToken) return null;

    return {
      accessToken: decryptedToken,
      apiVersion: env.apiVersion,
      source,
      connection
    };
  };

  const resolveInvalidUserTokenContext = (connection) => ({
    accessToken: null,
    apiVersion: env.apiVersion,
    source: 'user-token-invalid',
    connection
  });

  const connection = await MetaAdsConnection.findOne({ userId }).lean();
  if (connection?.accessToken) {
    const directUserContext = resolveConnectionAccessContext(connection, 'user');
    if (directUserContext) {
      return directUserContext;
    }
  }

  const adminMetaConfig = await resolveMetaConfigFromHierarchy({ userId });
  const credentialOwnerUserId = String(adminMetaConfig?.credentialOwnerUserId || '').trim();

  if (credentialOwnerUserId && credentialOwnerUserId !== String(userId)) {
    const ownerConnection = await MetaAdsConnection.findOne({ userId: credentialOwnerUserId }).lean();
    if (ownerConnection?.accessToken) {
      const ownerContext = resolveConnectionAccessContext(ownerConnection, 'company-admin');
      if (ownerContext) {
        return ownerContext;
      }
    }
  }

  if (adminMetaConfig?.userAccessToken) {
    return {
      accessToken: adminMetaConfig.userAccessToken,
      apiVersion: String(adminMetaConfig.apiVersion || env.apiVersion || 'v23.0').trim(),
      source: 'admin',
      connection: {
        selectedAdAccountId: normalizeAdAccountId(adminMetaConfig.adAccountId || ''),
        selectedPageId: '',
        selectedWhatsappNumber: ''
      },
      adminMetaConfig
    };
  }

  if (connection?.accessToken) {
    return resolveInvalidUserTokenContext(connection);
  }

  return {
    accessToken: null,
    apiVersion: env.apiVersion,
    source: 'none',
    connection: null
  };
};

const exchangeCodeForAccessToken = async ({ code, redirectUri, appId, appSecret, apiVersion }) => {
  const env = getMetaAdsConfig();
  const resolvedApiVersion = String(apiVersion || env.apiVersion || 'v23.0').trim();
  const resolvedAppId = String(appId || '').trim();
  const resolvedAppSecret = String(appSecret || '').trim();
  const resolvedRedirectUri = resolveMetaOAuthRedirectUri(redirectUri);

  const response = await axios.get(`${GRAPH_BASE_URL}/${resolvedApiVersion}/oauth/access_token`, {
    params: {
      client_id: resolvedAppId,
      client_secret: resolvedAppSecret,
      redirect_uri: resolvedRedirectUri,
      code
    }
  });
  return response.data;
};

const getLoginDialogUrl = ({ redirectUri, state, appId, apiVersion }) => {
  const env = getMetaAdsConfig();
  const resolvedApiVersion = String(apiVersion || env.apiVersion || 'v23.0').trim();
  const resolvedAppId = String(appId || '').trim();
  const scopes = [
    'public_profile',
   
    'business_management',
    'ads_management',
    'ads_read',
    'pages_read_engagement',
    
  ].join(',');
  const resolvedRedirectUri = resolveMetaOAuthRedirectUri(redirectUri);

  return `https://www.facebook.com/${resolvedApiVersion}/dialog/oauth?client_id=${encodeURIComponent(
    resolvedAppId
  )}&redirect_uri=${encodeURIComponent(resolvedRedirectUri)}&scope=${encodeURIComponent(
    scopes
  )}&response_type=code&state=${encodeURIComponent(String(state || ''))}`;
};

const saveUserConnection = async ({ userId, accessToken, scopes = [], graphRequest }) => {
  const profile = await graphRequest({
    path: 'me',
    params: { fields: 'id,name' },
    accessToken
  });

  return MetaAdsConnection.findOneAndUpdate(
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
};

const ensureUserConnectionRecord = async ({ userId, graphRequest } = {}) => {
  if (!userId) return null;

  const existingConnection = await MetaAdsConnection.findOne({ userId });
  if (existingConnection) return existingConnection;

  const accessContext = await getAccessContextForUser(userId);
  if (!accessContext?.accessToken) return null;

  try {
    return await saveUserConnection({
      userId,
      accessToken: accessContext.accessToken,
      scopes: accessContext.connection?.scopes || [],
      graphRequest
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

module.exports = {
  GRAPH_BASE_URL,
  getAccessContextForUser,
  exchangeCodeForAccessToken,
  getLoginDialogUrl,
  saveUserConnection,
  ensureUserConnectionRecord,
  resolveMetaConfigFromHierarchy,
  hasMetaAppCredentials,
  encryptMetaToken,
  decryptMetaToken
};
