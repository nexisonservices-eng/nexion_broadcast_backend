const axios = require('axios');

const MetaAdsConnection = require('../models/MetaAdsConnection');
const { getMetaAdsConfig } = require('../config/metaAdsConfig');
const { decryptMetaToken, encryptMetaToken } = require('../utils/metaTokenCrypto');
const { getMetaConfigByUserId } = require('./userMetaCredentialsService');

const GRAPH_BASE_URL = 'https://graph.facebook.com';

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

  const connection = await MetaAdsConnection.findOne({ userId }).lean();
  if (connection?.accessToken) {
    return {
      accessToken: decryptMetaToken(connection.accessToken),
      apiVersion: env.apiVersion,
      source: 'user',
      connection
    };
  }

  const adminMetaConfig = await getMetaConfigByUserId(userId);
  if (adminMetaConfig?.userAccessToken) {
    return {
      accessToken: adminMetaConfig.userAccessToken,
      apiVersion: String(adminMetaConfig.apiVersion || env.apiVersion || 'v22.0').trim(),
      source: 'admin',
      connection: {
        selectedAdAccountId: adminMetaConfig.adAccountId || '',
        selectedPageId: '',
        selectedWhatsappNumber: ''
      },
      adminMetaConfig
    };
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
  const resolvedApiVersion = String(apiVersion || env.apiVersion || 'v22.0').trim();
  const resolvedAppId = String(appId || env.appId || '').trim();
  const resolvedAppSecret = String(appSecret || env.appSecret || '').trim();

  const response = await axios.get(`${GRAPH_BASE_URL}/${resolvedApiVersion}/oauth/access_token`, {
    params: {
      client_id: resolvedAppId,
      client_secret: resolvedAppSecret,
      redirect_uri: redirectUri,
      code
    }
  });
  return response.data;
};

const getLoginDialogUrl = ({ redirectUri, state, appId, apiVersion }) => {
  const env = getMetaAdsConfig();
  const resolvedApiVersion = String(apiVersion || env.apiVersion || 'v22.0').trim();
  const resolvedAppId = String(appId || env.appId || '').trim();
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
    client_id: resolvedAppId,
    redirect_uri: redirectUri,
    scope: scopes,
    response_type: 'code',
    state
  });

  return `https://www.facebook.com/${resolvedApiVersion}/dialog/oauth?${params.toString()}`;
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
  encryptMetaToken,
  decryptMetaToken
};
