const crypto = require('crypto');
const { requireJwtSecret } = require('./securityConfig');

const ENCRYPTED_PREFIX = 'enc:';
const IV_LENGTH = 16;

const getMetaSecretCandidates = () =>
  [
    process.env.META_TOKEN_ENCRYPTION_KEY,
    process.env.JWT_SECRET,
    process.env.GOOGLE_TOKEN_ENCRYPTION_KEY
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean)
    .filter((value, index, arr) => arr.indexOf(value) === index);

const getPrimaryMetaSecret = () =>
  getMetaSecretCandidates()[0] || requireJwtSecret('Meta token encryption');

const toKey = (secret) => crypto.createHash('sha256').update(String(secret || '')).digest();

const decryptWithSecret = (token, secret) => {
  const [, payload] = token.split(ENCRYPTED_PREFIX);
  const [ivHex, encryptedHex] = String(payload || '').split(':');
  if (!ivHex || !encryptedHex) return '';

  const decipher = crypto.createDecipheriv(
    'aes-256-cbc',
    toKey(secret),
    Buffer.from(ivHex, 'hex')
  );
  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(encryptedHex, 'hex')),
    decipher.final()
  ]);
  return decrypted.toString('utf8');
};

const encryptMetaToken = (value) => {
  const token = String(value || '');
  if (!token) return '';
  if (token.startsWith(ENCRYPTED_PREFIX)) return token;

  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', toKey(getPrimaryMetaSecret()), iv);
  const encrypted = Buffer.concat([cipher.update(token, 'utf8'), cipher.final()]);
  return `${ENCRYPTED_PREFIX}${iv.toString('hex')}:${encrypted.toString('hex')}`;
};

const decryptMetaToken = (value) => {
  const token = String(value || '');
  if (!token) return '';
  if (!token.startsWith(ENCRYPTED_PREFIX)) return token;

  try {
    const candidates = getMetaSecretCandidates();
    for (const secret of candidates) {
      try {
        const decrypted = decryptWithSecret(token, secret);
        if (decrypted) return decrypted;
      } catch {
        continue;
      }
    }

    return '';
  } catch {
    return '';
  }
};

module.exports = {
  encryptMetaToken,
  decryptMetaToken
};
