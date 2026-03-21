const parseBoolean = (value, defaultValue = false) => {
  if (value === undefined || value === null || value === '') return defaultValue;
  return String(value).trim().toLowerCase() === 'true' || String(value).trim() === '1';
};

const parseNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const getMetaAdsConfig = () => {
  const apiVersion = String(process.env.META_API_VERSION || 'v22.0').trim();
  const pixelId = String(process.env.META_PIXEL_ID || '').trim();
  const appId = String(process.env.META_APP_ID || '').trim();
  const appSecret = String(process.env.META_APP_SECRET || '').trim();
  const tokenEncryptionKey = String(
    process.env.META_TOKEN_ENCRYPTION_KEY || process.env.JWT_SECRET || ''
  ).trim();
  const forceMock = parseBoolean(process.env.META_ADS_FORCE_MOCK, false);
  const advantageAudience = parseBoolean(process.env.META_ADVANTAGE_AUDIENCE, false) ? 1 : 0;
  const bidStrategy = String(
    process.env.META_DEFAULT_BID_STRATEGY || 'LOWEST_COST_WITH_BID_CAP'
  ).trim();
  const bidAmount = parseNumber(process.env.META_BID_AMOUNT, 5000);

  return {
    apiVersion,
    pixelId,
    appId,
    appSecret,
    tokenEncryptionKey,
    forceMock,
    advantageAudience,
    bidStrategy,
    bidAmount,
    hasOAuthConfig: Boolean(appId && appSecret)
  };
};

const validateMetaAdsEnv = ({ strict = false } = {}) => {
  const config = getMetaAdsConfig();
  const warnings = [];

  if (!config.apiVersion) warnings.push('META_API_VERSION is missing.');
  if (!config.tokenEncryptionKey) warnings.push('META_TOKEN_ENCRYPTION_KEY or JWT_SECRET is missing.');
  if (!config.pixelId) warnings.push('META_PIXEL_ID is missing. Pixel conversion tracking will be disabled.');

  if (strict && warnings.length) {
    const error = new Error(`Meta Ads configuration invalid: ${warnings.join(' ')}`);
    error.code = 'META_CONFIG_INVALID';
    error.warnings = warnings;
    throw error;
  }

  return {
    config,
    warnings,
    valid: warnings.length === 0
  };
};

module.exports = {
  getMetaAdsConfig,
  validateMetaAdsEnv
};
