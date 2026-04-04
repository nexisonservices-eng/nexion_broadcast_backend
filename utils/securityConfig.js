const toCleanString = (value) => String(value || '').trim();

const DEBUG_TRUE_VALUES = new Set(['1', 'true', 'yes', 'on']);

const isDebugLoggingEnabled = () =>
  DEBUG_TRUE_VALUES.has(toCleanString(process.env.ENABLE_DEBUG_LOGS).toLowerCase());

const validateSecurityEnv = () => {
  const errors = [];
  const warnings = [];

  const jwtSecret = toCleanString(process.env.JWT_SECRET);
  const googleTokenKey = toCleanString(process.env.GOOGLE_TOKEN_ENCRYPTION_KEY);
  const metaTokenKey = toCleanString(process.env.META_TOKEN_ENCRYPTION_KEY);

  if (!jwtSecret) {
    errors.push('JWT_SECRET is required.');
  }

  if (!googleTokenKey && !metaTokenKey) {
    warnings.push(
      'GOOGLE_TOKEN_ENCRYPTION_KEY and META_TOKEN_ENCRYPTION_KEY are not set. Falling back to JWT_SECRET for token encryption.'
    );
  }

  return { errors, warnings };
};

const requireJwtSecret = (context = 'JWT operations') => {
  const secret = toCleanString(process.env.JWT_SECRET);
  if (!secret) {
    throw new Error(`JWT_SECRET is required for ${context}. Set JWT_SECRET in backend env.`);
  }
  return secret;
};

const requireTokenEncryptionSecret = (context = 'token encryption') => {
  const secret =
    toCleanString(process.env.GOOGLE_TOKEN_ENCRYPTION_KEY) ||
    toCleanString(process.env.META_TOKEN_ENCRYPTION_KEY) ||
    toCleanString(process.env.JWT_SECRET);

  if (!secret) {
    throw new Error(
      `GOOGLE_TOKEN_ENCRYPTION_KEY or META_TOKEN_ENCRYPTION_KEY is required for ${context}.`
    );
  }
  return secret;
};

module.exports = {
  isDebugLoggingEnabled,
  validateSecurityEnv,
  requireJwtSecret,
  requireTokenEncryptionSecret
};
