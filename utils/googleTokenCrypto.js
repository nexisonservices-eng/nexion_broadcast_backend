const crypto = require('crypto');
const { requireTokenEncryptionSecret } = require('./securityConfig');

const ENCRYPTED_PREFIX = 'enc:';
const IV_LENGTH = 16;

const getKey = () => {
  const baseSecret = requireTokenEncryptionSecret('Google token encryption');

  return crypto.createHash('sha256').update(String(baseSecret)).digest();
};

const encryptGoogleToken = (value) => {
  const token = String(value || '');
  if (!token) return '';
  if (token.startsWith(ENCRYPTED_PREFIX)) return token;

  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', getKey(), iv);
  const encrypted = Buffer.concat([cipher.update(token, 'utf8'), cipher.final()]);
  return `${ENCRYPTED_PREFIX}${iv.toString('hex')}:${encrypted.toString('hex')}`;
};

const decryptGoogleToken = (value) => {
  const token = String(value || '');
  if (!token) return '';
  if (!token.startsWith(ENCRYPTED_PREFIX)) return token;

  const [, payload] = token.split(ENCRYPTED_PREFIX);
  const [ivHex, encryptedHex] = String(payload || '').split(':');
  if (!ivHex || !encryptedHex) return '';
  try {
    const decipher = crypto.createDecipheriv(
      'aes-256-cbc',
      getKey(),
      Buffer.from(ivHex, 'hex')
    );
    const decrypted = Buffer.concat([
      decipher.update(Buffer.from(encryptedHex, 'hex')),
      decipher.final()
    ]);
    return decrypted.toString('utf8');
  } catch {
    return '';
  }
};

module.exports = {
  encryptGoogleToken,
  decryptGoogleToken
};
