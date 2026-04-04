const axios = require('axios');

const ADMIN_API_BASE_URLS = [
  process.env.ADMIN_API_BASE_URL,
  process.env.ADMIN_BACKEND_URL,
  'http://localhost:8000',
  'http://localhost:5000'
]
  .map((url) => (url || '').trim())
  .filter(Boolean)
  .filter((url, index, arr) => arr.indexOf(url) === index);

const ADMIN_USER_CREDENTIALS_ENDPOINT =
  process.env.ADMIN_USER_CREDENTIALS_ENDPOINT ||
  process.env.ADMIN_CREDENTIALS_ENDPOINT_PATH ||
  '/api/user/credentials';

const ADMIN_USER_CREDENTIALS_BY_ID_ENDPOINT =
  process.env.ADMIN_USER_CREDENTIALS_BY_ID_ENDPOINT || '/internal/user/credentials';

const ADMIN_INTERNAL_API_KEY = process.env.ADMIN_INTERNAL_API_KEY || null;

const normalizeMetaConfig = (data) => {
  if (!data) return null;

  const trim = (value) => String(value || '').trim();

  const appId = trim(data.metaAppId || data.metaappid);
  const appSecret = trim(data.metaAppSecret || data.metaappsecret);
  const redirectUri = trim(data.metaRedirectUri || data.metaredirecturi);
  const userAccessToken = trim(data.metaUserAccessToken || data.metauseraccesstoken);
  const adAccountId = trim(data.metaAdAccountId || data.metaadaccountid);
  const apiVersion = trim(data.metaApiVersion || data.metaapiversion || 'v22.0') || 'v22.0';

  return {
    appId,
    appSecret,
    redirectUri,
    userAccessToken,
    adAccountId,
    apiVersion,
    jwtSecret: trim(data.metaJwtSecret || data.metajwtsecret),
    credentialOwnerUserId: trim(data.credentialOwnerUserId || data.credentialowneruserid || data.userId || data.userid)
  };
};

const getMetaConfigForUser = async ({ authHeader = '' }) => {
  if (!authHeader.startsWith('Bearer ')) return null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const response = await axios.get(`${baseUrl}${ADMIN_USER_CREDENTIALS_ENDPOINT}`, {
        headers: {
          Authorization: authHeader
        },
        timeout: 10000
      });

      return normalizeMetaConfig(response.data?.data);
    } catch (error) {
      if ([401, 403].includes(error?.response?.status)) {
        throw error;
      }
    }
  }

  return null;
};

const getMetaConfigByUserId = async (userId) => {
  if (!userId) return null;

  const normalizedUserId = String(userId).trim();
  if (!normalizedUserId) return null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const headers = {};
      if (ADMIN_INTERNAL_API_KEY) {
        headers['x-internal-api-key'] = ADMIN_INTERNAL_API_KEY;
      }

      const response = await axios.get(
        `${baseUrl}${ADMIN_USER_CREDENTIALS_BY_ID_ENDPOINT}/${encodeURIComponent(normalizedUserId)}`,
        {
          headers,
          timeout: 10000
        }
      );

      return normalizeMetaConfig(response.data?.data);
    } catch (error) {
      continue;
    }
  }

  return null;
};

module.exports = {
  getMetaConfigForUser,
  getMetaConfigByUserId
};
