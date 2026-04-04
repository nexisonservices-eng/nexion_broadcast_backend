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

const ADMIN_WHATSAPP_USER_LOOKUP_ENDPOINT =
  process.env.ADMIN_WHATSAPP_USER_LOOKUP_ENDPOINT || '/internal/user/by-whatsapp-id';

const ADMIN_PHONE_USER_LOOKUP_ENDPOINT =
  process.env.ADMIN_PHONE_USER_LOOKUP_ENDPOINT || '/internal/user/by-phone-number';

const ADMIN_USER_CREDENTIALS_BY_ID_ENDPOINT =
  process.env.ADMIN_USER_CREDENTIALS_BY_ID_ENDPOINT || '/internal/user/credentials';

const ADMIN_USER_CREDENTIALS_UPDATE_ENDPOINT =
  process.env.ADMIN_USER_CREDENTIALS_UPDATE_ENDPOINT || '/internal/user/credentials';

const ADMIN_INTERNAL_API_KEY = process.env.ADMIN_INTERNAL_API_KEY || null;

const userCredentialCache = new Map();
const phoneNumberToUserIdCache = new Map();
const CACHE_TTL_MS = 2 * 60 * 1000;

const toCacheKey = (userId, token) => `${userId || 'unknown'}:${token || 'unknown'}`;

const normalizeCredentials = (data) => {
  if (!data) return null;

  const trimOrNull = (value) => {
    if (value === undefined || value === null) return null;
    const normalized = String(value).trim();
    return normalized.length > 0 ? normalized : null;
  };

  const accessToken = trimOrNull(data.whatsappToken || data.accessToken);
  const businessAccountId = trimOrNull(data.whatsappBusiness || data.businessAccountId);
  const phoneNumberId = trimOrNull(data.whatsappId || data.phoneNumberId);
  const username = trimOrNull(data.username);
  const email = trimOrNull(data.email);
  const twilioId = trimOrNull(data.twilioId);
  const phoneNumber = trimOrNull(data.phoneNumber);
  const missedCallWebhook = trimOrNull(data.missedCallWebhook);
  const companyId = trimOrNull(data.companyId);

  const missedCallTemplateName = trimOrNull(
    data.missedCallTemplateName || data.missedcalltemplatename
  );

  const missedCallTemplateLanguage =
    trimOrNull(data.missedCallTemplateLanguage || data.missedcalltemplatelanguage) || 'en_US';

  const missedCallTemplateVariables = Array.isArray(
    data.missedCallTemplateVariables || data.missedcalltemplatevariables
  )
    ? (data.missedCallTemplateVariables || data.missedcalltemplatevariables)
        .map((item, idx) => ({
          index: Number.isFinite(Number(item?.index)) ? Number(item.index) : idx + 1,
          source: String(item?.source || item?.sourceType || 'callerName').trim() || 'callerName',
          value: String(item?.value || item?.staticValue || '').trim()
        }))
        .sort((a, b) => a.index - b.index)
    : [];

  const missedCallDelayMinutesRaw =
    data.missedCallDelayMinutes ?? data.missedcalldelayminutes;

  const parsedDelay = Number(missedCallDelayMinutesRaw);
  const missedCallDelayMinutes =
    Number.isFinite(parsedDelay) && parsedDelay >= 0 ? parsedDelay : 5;

  const rawAutomationEnabled =
    data.missedCallAutomationEnabled ?? data.missedcallautomationenabled;

  const missedCallAutomationEnabled =
    typeof rawAutomationEnabled === 'boolean'
      ? rawAutomationEnabled
      : String(rawAutomationEnabled || '').toLowerCase() !== 'false';

  const rawMode =
    data.missedCallAutomationMode ?? data.missedcallautomationmode;
  const missedCallAutomationMode =
    String(rawMode || 'immediate').toLowerCase() === 'night_batch'
      ? 'night_batch'
      : 'immediate';

  const rawNightHour =
    data.missedCallNightHour ?? data.missedcallnighthour;
  const parsedNightHour = Number(rawNightHour);
  const missedCallNightHour =
    Number.isFinite(parsedNightHour) && parsedNightHour >= 0 && parsedNightHour <= 23
      ? parsedNightHour
      : 21;

  const rawNightMinute =
    data.missedCallNightMinute ?? data.missedcallnightminute;
  const parsedNightMinute = Number(rawNightMinute);
  const missedCallNightMinute =
    Number.isFinite(parsedNightMinute) && parsedNightMinute >= 0 && parsedNightMinute <= 59
      ? parsedNightMinute
      : 0;

  const missedCallTimezone =
    trimOrNull(data.missedCallTimezone || data.missedcalltimezone) || 'Asia/Kolkata';

  if (!accessToken || !businessAccountId || !phoneNumberId) return null;

  return {
    username,
    email,
    accessToken,
    businessAccountId,
    phoneNumberId,
    whatsappToken: accessToken,
    whatsappBusiness: businessAccountId,
    whatsappId: phoneNumberId,
    twilioId,
    phoneNumber,
    companyId,
    missedCallWebhook,
    missedCallTemplateName,
    missedCallTemplateLanguage,
    missedCallTemplateVariables,
    missedCallDelayMinutes,
    missedCallAutomationEnabled,
    missedCallAutomationMode,
    missedCallNightHour,
    missedCallNightMinute,
    missedCallTimezone
  };
};

const getCachedCredentials = (cacheKey) => {
  const cached = userCredentialCache.get(cacheKey);
  if (!cached) return null;

  if (Date.now() - cached.fetchedAt > CACHE_TTL_MS) {
    userCredentialCache.delete(cacheKey);
    return null;
  }

  return cached.value;
};

const fetchCredentialsFromAdminApi = async ({ authHeader, userId, token }) => {
  let lastError = null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    const endpoint = `${baseUrl}${ADMIN_USER_CREDENTIALS_ENDPOINT}`;

    try {
      const response = await axios.get(endpoint, {
        headers: {
          Authorization: authHeader
        },
        timeout: 10000
      });

      const normalized = normalizeCredentials(response.data?.data);
      if (!normalized) return null;

      const cacheKey = toCacheKey(userId, token);
      userCredentialCache.set(cacheKey, { value: normalized, fetchedAt: Date.now() });

      if (userId) {
        phoneNumberToUserIdCache.set(String(normalized.phoneNumberId), String(userId));
      }

      return normalized;
    } catch (error) {
      const statusCode = error?.response?.status || 502;
      const upstreamMessage =
        error?.response?.data?.error ||
        error?.response?.data?.message ||
        error?.message ||
        'Admin backend request failed';

      const wrappedError = new Error(`${upstreamMessage} (${endpoint})`);
      wrappedError.statusCode = statusCode;
      lastError = wrappedError;

      if (statusCode === 401 || statusCode === 403) {
        throw wrappedError;
      }
    }
  }

  throw lastError || new Error('Unable to reach admin backend for user credentials');
};

const getWhatsAppCredentialsForUser = async ({ authHeader = '', userId = null }) => {
  if (!authHeader.startsWith('Bearer ')) return null;

  const token = authHeader.slice(7).trim();
  if (!token) return null;

  const cacheKey = toCacheKey(userId, token);
  const cached = getCachedCredentials(cacheKey);
  if (cached) return cached;

  return fetchCredentialsFromAdminApi({ authHeader, userId, token });
};

const resolveUserIdByPhoneNumberId = async (phoneNumberId) => {
  if (!phoneNumberId) return null;

  const key = String(phoneNumberId);
  const cached = phoneNumberToUserIdCache.get(key);
  if (cached) return cached;

  if (!ADMIN_INTERNAL_API_KEY) return null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const endpoint = `${baseUrl}${ADMIN_WHATSAPP_USER_LOOKUP_ENDPOINT}/${encodeURIComponent(key)}`;
      const response = await axios.get(endpoint, {
        headers: {
          'x-internal-api-key': ADMIN_INTERNAL_API_KEY
        },
        timeout: 10000
      });

      const resolvedUserId = response.data?.data?.userId
        ? String(response.data.data.userId)
        : null;

      if (resolvedUserId) {
        phoneNumberToUserIdCache.set(key, resolvedUserId);
        return resolvedUserId;
      }
    } catch (error) {
      continue;
    }
  }

  return null;
};

const resolveUserIdByRegisteredPhone = async (phoneNumber) => {
  if (!phoneNumber || !ADMIN_INTERNAL_API_KEY) return null;

  const normalized = String(phoneNumber).replace(/[^\d+]/g, '').trim();
  if (!normalized) return null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const endpoint = `${baseUrl}${ADMIN_PHONE_USER_LOOKUP_ENDPOINT}/${encodeURIComponent(normalized)}`;
      const response = await axios.get(endpoint, {
        headers: {
          'x-internal-api-key': ADMIN_INTERNAL_API_KEY
        },
        timeout: 10000
      });

      const resolvedUserId = response.data?.data?.userId
        ? String(response.data.data.userId)
        : null;

      if (resolvedUserId) {
        return resolvedUserId;
      }
    } catch (error) {
      continue;
    }
  }

  return null;
};

const getWhatsAppCredentialsByUserId = async (userId) => {
  if (!userId || !ADMIN_INTERNAL_API_KEY) return null;

  const normalizedUserId = String(userId).trim();
  if (!normalizedUserId) return null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const endpoint = `${baseUrl}${ADMIN_USER_CREDENTIALS_BY_ID_ENDPOINT}/${encodeURIComponent(normalizedUserId)}`;
      const response = await axios.get(endpoint, {
        headers: {
          'x-internal-api-key': ADMIN_INTERNAL_API_KEY
        },
        timeout: 10000
      });

      const normalized = normalizeCredentials(response.data?.data);
      if (normalized) {
        phoneNumberToUserIdCache.set(String(normalized.phoneNumberId), normalizedUserId);
        return normalized;
      }

      return response.data?.data || null;
    } catch (error) {
      continue;
    }
  }

  return null;
};

const updateUserCredentialsByUserId = async (userId, payload = {}) => {
  if (!userId || !ADMIN_INTERNAL_API_KEY) return null;

  const normalizedUserId = String(userId).trim();
  if (!normalizedUserId) return null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const endpoint = `${baseUrl}${ADMIN_USER_CREDENTIALS_UPDATE_ENDPOINT}/${encodeURIComponent(normalizedUserId)}`;
      const response = await axios.put(endpoint, payload, {
        headers: {
          'x-internal-api-key': ADMIN_INTERNAL_API_KEY
        },
        timeout: 10000
      });

      const normalized = normalizeCredentials(response.data?.data);
      if (normalized) {
        phoneNumberToUserIdCache.set(String(normalized.phoneNumberId), normalizedUserId);
        return normalized;
      }

      return response.data?.data || null;
    } catch (error) {
      continue;
    }
  }

  return null;
};

module.exports = {
  getWhatsAppCredentialsForUser,
  resolveUserIdByPhoneNumberId,
  resolveUserIdByRegisteredPhone,
  getWhatsAppCredentialsByUserId,
  updateUserCredentialsByUserId
};
