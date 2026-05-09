const axios = require('axios');
const path = require('path');
const crypto = require('crypto');
const { v2: cloudinary } = require('cloudinary');
const {
  sanitizeStorageSegment,
  resolveCompanyRoot,
  resolveInboxFolderPath: resolveCompanyInboxFolderPath
} = require('./cloudinaryCompanyFolders');

const GRAPH_API_BASE_URL = 'https://graph.facebook.com/v20.0';
const DEFAULT_FOLDER_ROOT = 'inbox';

const DEFAULT_ALLOWED_IMAGE_MIME = [
  'image/png',
  'image/jpeg',
  'image/jpg',
  'image/webp'
];

const DEFAULT_ALLOWED_DOCUMENT_MIME = [
  'application/pdf',
  'application/msword',
  'application/vnd.ms-excel',
  'application/vnd.ms-powerpoint',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  'text/plain',
  'text/csv',
  'application/zip',
  'application/x-zip-compressed'
];

const DEFAULT_ALLOWED_VIDEO_MIME = [
  'video/mp4',
  'video/3gpp',
  'video/quicktime',
  'video/webm'
];

const DEFAULT_ALLOWED_AUDIO_MIME = [
  'audio/aac',
  'audio/amr',
  'audio/mp3',
  'audio/mp4',
  'audio/mpeg',
  'audio/ogg',
  'audio/opus',
  'audio/webm',
  'audio/x-m4a',
  'audio/wav',
  'audio/3gpp',
  'audio/3gpp2'
];

const DEFAULT_MAX_IMAGE_MB = 10;
const DEFAULT_MAX_DOCUMENT_MB = 25;
const DEFAULT_MAX_AUDIO_MB = 16;
const DEFAULT_MAX_VIDEO_MB = 16;
const DEFAULT_SIGNED_URL_TTL_SECONDS = 300;

let cloudinaryConfigured = false;

const parseListFromEnv = (value, fallback = []) => {
  const raw = String(value || '')
    .split(',')
    .map((item) => item.trim().toLowerCase().split(';')[0].trim())
    .filter(Boolean);
  return raw.length > 0 ? raw : fallback;
};

const normalizeMimeType = (value = '') =>
  String(value || '')
    .trim()
    .toLowerCase()
    .split(';')[0]
    .trim();

const ensureCloudinaryConfig = () => {
  if (cloudinaryConfigured) return;
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
  });
  cloudinaryConfigured = true;
};

const hasCloudinaryCredentials = () =>
  Boolean(
    process.env.CLOUDINARY_CLOUD_NAME &&
      process.env.CLOUDINARY_API_KEY &&
      process.env.CLOUDINARY_API_SECRET
  );

const resolveInboxStorageUsername = ({ username, email, userId }) => {
  const preferred = String(username || '').trim();
  if (preferred) return sanitizeStorageSegment(preferred, sanitizeStorageSegment(userId, 'user'));

  const emailValue = String(email || '').trim().toLowerCase();
  if (emailValue) {
    const localPart = emailValue.includes('@') ? emailValue.split('@')[0] : emailValue;
    return sanitizeStorageSegment(localPart, sanitizeStorageSegment(userId, 'user'));
  }

  return sanitizeStorageSegment(userId, 'user');
};

const resolveFolderRoot = () => {
  const envValue = String(process.env.INBOX_STORAGE_PREFIX || '').trim();
  const normalized = sanitizeStorageSegment(envValue.replace(/\//g, ''), DEFAULT_FOLDER_ROOT);
  return normalized || DEFAULT_FOLDER_ROOT;
};

const resolveInboxFolderPath = ({ username, direction = 'sent' }) => {
  const safeDirection = String(direction || '').trim().toLowerCase() === 'received' ? 'received' : 'sent';
  const root = resolveFolderRoot();
  const safeUsername = sanitizeStorageSegment(username, 'user');
  return `${root}/${safeUsername}/${safeDirection}`;
};

const ensureFolderPath = async (folderPath) => {
  ensureCloudinaryConfig();
  const normalized = String(folderPath || '').trim().replace(/\\/g, '/').replace(/\/+/g, '/');
  const folders = [];
  const segments = normalized.split('/').filter(Boolean);
  for (let index = 0; index < segments.length; index += 1) {
    folders.push(segments.slice(0, index + 1).join('/'));
  }

  for (const folder of folders) {
    try {
      // Cloudinary create_folder is idempotent if we ignore existing-folder errors.
      // eslint-disable-next-line no-await-in-loop
      await cloudinary.api.create_folder(folder);
    } catch (error) {
      const message = String(error?.message || '').toLowerCase();
      const alreadyExists =
        message.includes('already exists') ||
        message.includes('existing folder') ||
        Number(error?.http_code || 0) === 409;
      if (!alreadyExists) {
        throw error;
      }
    }
  }
};

const ensureInboxFolders = async ({ username }) => {
  const root = resolveFolderRoot();
  const safeUsername = sanitizeStorageSegment(username, 'user');
  await ensureFolderPath(`${root}/${safeUsername}/sent`);
  await ensureFolderPath(`${root}/${safeUsername}/received`);
};

const allowedImageMimeTypes = () =>
  parseListFromEnv(process.env.ALLOWED_IMAGE_MIME, DEFAULT_ALLOWED_IMAGE_MIME);

const allowedDocumentMimeTypes = () =>
  parseListFromEnv(process.env.ALLOWED_DOC_MIME, DEFAULT_ALLOWED_DOCUMENT_MIME);

const allowedAudioMimeTypes = () =>
  parseListFromEnv(process.env.ALLOWED_AUDIO_MIME, DEFAULT_ALLOWED_AUDIO_MIME);

const allowedVideoMimeTypes = () =>
  parseListFromEnv(process.env.ALLOWED_VIDEO_MIME, DEFAULT_ALLOWED_VIDEO_MIME);

const toBytes = (megabytes, fallbackMb) => {
  const parsed = Number(megabytes);
  const safeMb = Number.isFinite(parsed) && parsed > 0 ? parsed : fallbackMb;
  return Math.floor(safeMb * 1024 * 1024);
};

const resolveMaxImageBytes = () => toBytes(process.env.MAX_IMAGE_SIZE_MB, DEFAULT_MAX_IMAGE_MB);
const resolveMaxDocumentBytes = () => toBytes(process.env.MAX_DOC_SIZE_MB, DEFAULT_MAX_DOCUMENT_MB);
const resolveMaxAudioBytes = () => toBytes(process.env.MAX_AUDIO_SIZE_MB, DEFAULT_MAX_AUDIO_MB);
const resolveMaxVideoBytes = () => toBytes(process.env.MAX_VIDEO_SIZE_MB, DEFAULT_MAX_VIDEO_MB);

const inferMimeTypeFromFileName = (name = '') => {
  const extension = String(path.extname(String(name || '') || '').toLowerCase() || '').replace('.', '');
  const map = {
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    png: 'image/png',
    webp: 'image/webp',
    gif: 'image/gif',
    mp3: 'audio/mpeg',
    m4a: 'audio/mp4',
    aac: 'audio/aac',
    amr: 'audio/amr',
    ogg: 'audio/ogg',
    opus: 'audio/opus',
    wav: 'audio/wav',
    mp4: 'video/mp4',
    mov: 'video/quicktime',
    webm: 'video/webm',
    '3gp': 'video/3gpp',
    '3g2': 'video/3gpp2',
    pdf: 'application/pdf',
    doc: 'application/msword',
    docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    xls: 'application/vnd.ms-excel',
    xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    ppt: 'application/vnd.ms-powerpoint',
    pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    txt: 'text/plain',
    csv: 'text/csv',
    zip: 'application/zip'
  };
  return map[extension] || '';
};

const resolveFileCategory = (mimeType = '') => {
  const normalizedMime = normalizeMimeType(mimeType);
  if (allowedImageMimeTypes().includes(normalizedMime)) return 'image';
  if (allowedAudioMimeTypes().includes(normalizedMime)) return 'audio';
  if (allowedVideoMimeTypes().includes(normalizedMime)) return 'video';
  if (allowedDocumentMimeTypes().includes(normalizedMime)) return 'document';
  return '';
};

const validateAttachmentFile = (file = {}) => {
  if (!file?.buffer || !Number.isFinite(Number(file?.size || file?.buffer?.length || 0))) {
    const error = new Error('Attachment file buffer is required.');
    error.status = 400;
    throw error;
  }

  const fileSize = Number(file.size || file.buffer.length || 0);
  const originalName = String(file.originalname || '').trim();
  const mimeTypeFallback = inferMimeTypeFromFileName(originalName);
  const mimeType = normalizeMimeType(file.mimetype || mimeTypeFallback || '');
  const fileCategory = resolveFileCategory(mimeType);

  if (!fileCategory) {
    const error = new Error(
      `Unsupported file type "${mimeType || 'unknown'}". Allowed: images (${allowedImageMimeTypes().join(', ')}), audio (${allowedAudioMimeTypes().join(', ')}), video (${allowedVideoMimeTypes().join(', ')}), and documents (${allowedDocumentMimeTypes().join(', ')})`
    );
    error.status = 415;
    throw error;
  }

  const maxBytes =
    fileCategory === 'image'
      ? resolveMaxImageBytes()
      : fileCategory === 'audio'
        ? resolveMaxAudioBytes()
        : fileCategory === 'video'
          ? resolveMaxVideoBytes()
        : resolveMaxDocumentBytes();
  if (fileSize > maxBytes) {
    const maxMb = (maxBytes / (1024 * 1024)).toFixed(0);
    const error = new Error(`File exceeds max size limit (${maxMb} MB) for ${fileCategory}.`);
    error.status = 413;
    throw error;
  }

  const extension = String(path.extname(originalName || '').toLowerCase() || '').replace('.', '');

  return {
    fileSize,
    originalName,
    mimeType,
    fileCategory,
    extension
  };
};

const buildPublicIdBase = ({ originalName, extension, userId }) => {
  const baseName = String(path.basename(String(originalName || ''), path.extname(String(originalName || '')) || '')).trim();
  const safeBase = sanitizeStorageSegment(baseName, 'file').slice(0, 50);
  const timestamp = Date.now();
  const randomSuffix = crypto.randomBytes(4).toString('hex');
  const userSegment = sanitizeStorageSegment(userId, 'user');
  const extSuffix = extension ? `.${extension}` : '';
  return `${safeBase}-${userSegment}-${timestamp}-${randomSuffix}${extSuffix}`;
};

const uploadInboxAttachment = async ({
  file,
  username,
  direction = 'sent',
  folderOverride = '',
  companyContext = null,
  userId,
  sender,
  recipient
}) => {
  if (!hasCloudinaryCredentials()) {
    const error = new Error(
      'Cloudinary is not configured. Add CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, and CLOUDINARY_API_SECRET.'
    );
    error.status = 500;
    throw error;
  }

  ensureCloudinaryConfig();

  const { fileSize, originalName, mimeType, fileCategory, extension } = validateAttachmentFile(file);
  const safeUsername = sanitizeStorageSegment(username, sanitizeStorageSegment(userId, 'user'));
  const normalizedFolderOverride = String(folderOverride || '').trim().replace(/\\/g, '/').replace(/\/+/g, '/');
  const folder =
    normalizedFolderOverride ||
    (companyContext
      ? resolveCompanyInboxFolderPath({ companyContext, direction })
      : resolveInboxFolderPath({ username: safeUsername, direction }));
  await ensureFolderPath(folder);
  const resourceType =
    fileCategory === 'image'
      ? 'image'
      : fileCategory === 'video'
        ? 'video'
        : 'raw';
  const publicIdBase = buildPublicIdBase({ originalName, extension, userId });
  const dataUri = `data:${mimeType || 'application/octet-stream'};base64,${file.buffer.toString('base64')}`;

  const uploadResult = await cloudinary.uploader.upload(dataUri, {
    folder,
    resource_type: resourceType,
    use_filename: false,
    unique_filename: false,
    public_id: publicIdBase,
    overwrite: false
  });

  return {
    storageProvider: 'cloudinary',
    direction: String(direction || '').toLowerCase() === 'received' ? 'received' : 'sent',
    username: safeUsername,
    folder,
    publicId: uploadResult?.public_id || '',
    resourceType,
    fileCategory,
    mimeType,
    originalFileName: originalName,
    extension: extension || String(uploadResult?.format || '').trim().toLowerCase(),
    bytes: Number(uploadResult?.bytes || fileSize || 0),
    width: Number(uploadResult?.width || 0) || null,
    height: Number(uploadResult?.height || 0) || null,
    pages: Number(uploadResult?.pages || 0) || null,
    secureUrl: String(uploadResult?.secure_url || uploadResult?.url || '').trim(),
    sender: String(sender || '').trim(),
    recipient: String(recipient || '').trim(),
    uploadedAt: new Date(),
    deletedAt: null,
    deletedBy: null
  };
};

const isAttachmentPathOwned = ({ publicId, username, companyContext = null }) => {
  const normalizedPublicId = String(publicId || '').trim();
  if (companyContext) {
    try {
      const expectedCompanyPrefix = `${resolveCompanyRoot(companyContext)}/`;
      if (normalizedPublicId.startsWith(expectedCompanyPrefix)) return true;
    } catch (_error) {
      // Fall through to legacy username-based path validation.
    }
  }

  const safeUsername = sanitizeStorageSegment(username, 'user');
  const root = resolveFolderRoot();
  const expectedPrefix = `${root}/${safeUsername}/`;
  return normalizedPublicId.startsWith(expectedPrefix);
};

const generateSignedAttachmentUrl = ({
  attachment = {},
  mode = 'view',
  expiresInSeconds = DEFAULT_SIGNED_URL_TTL_SECONDS
}) => {
  ensureCloudinaryConfig();

  const publicId = String(attachment?.publicId || '').trim();
  if (!publicId) {
    return { url: '', expiresAt: null };
  }

  const resourceType = String(attachment?.resourceType || '').trim() || 'image';
  const safeTtl = Number.isFinite(Number(expiresInSeconds))
    ? Math.max(60, Math.min(Number(expiresInSeconds), 3600))
    : DEFAULT_SIGNED_URL_TTL_SECONDS;
  const expiresAt = Math.floor(Date.now() / 1000) + safeTtl;

  const isDownload = String(mode || '').trim().toLowerCase() === 'download';
  const signOptions = {
    resource_type: resourceType,
    secure: true,
    sign_url: true,
    type: 'upload',
    expires_at: expiresAt
  };

  if (isDownload) {
    signOptions.flags = 'attachment';
  }

  let signedUrl = '';
  try {
    signedUrl = cloudinary.url(publicId, signOptions);
  } catch (_error) {
    signedUrl = '';
  }

  return {
    url: signedUrl || String(attachment?.secureUrl || '').trim(),
    expiresAt: new Date(expiresAt * 1000).toISOString()
  };
};

const generateAttachmentDownloadUrl = ({
  attachment = {},
  expiresInSeconds = DEFAULT_SIGNED_URL_TTL_SECONDS
}) => {
  ensureCloudinaryConfig();

  const publicId = String(attachment?.publicId || '').trim();
  if (!publicId) {
    return { url: '', expiresAt: null };
  }

  const resourceType = String(attachment?.resourceType || '').trim() || 'image';
  const safeTtl = Number.isFinite(Number(expiresInSeconds))
    ? Math.max(60, Math.min(Number(expiresInSeconds), 3600))
    : DEFAULT_SIGNED_URL_TTL_SECONDS;
  const expiresAt = Math.floor(Date.now() / 1000) + safeTtl;
  const extension =
    String(attachment?.extension || '').trim().toLowerCase() ||
    String(path.extname(String(attachment?.originalFileName || '')) || '')
      .replace('.', '')
      .trim()
      .toLowerCase();

  let downloadUrl = '';
  try {
    const isRawDocument =
      resourceType === 'raw' ||
      String(attachment?.fileCategory || '').trim().toLowerCase() === 'document';

    if (isRawDocument && extension) {
      downloadUrl = cloudinary.utils.private_download_url(publicId, extension, {
        resource_type: resourceType,
        type: 'upload',
        attachment: true,
        expires_at: expiresAt
      });
    } else {
      const signed = generateSignedAttachmentUrl({
        attachment,
        mode: 'download',
        expiresInSeconds: safeTtl
      });
      downloadUrl = String(signed?.url || '').trim();
    }
  } catch (_error) {
    downloadUrl = '';
  }

  return {
    url: downloadUrl || String(attachment?.secureUrl || '').trim(),
    expiresAt: new Date(expiresAt * 1000).toISOString()
  };
};

const deleteInboxAttachment = async ({ attachment = {} }) => {
  ensureCloudinaryConfig();
  const publicId = String(attachment?.publicId || '').trim();
  if (!publicId) return { result: 'not_found' };

  const resourceType = String(attachment?.resourceType || '').trim() || 'image';
  try {
    const response = await cloudinary.uploader.destroy(publicId, {
      resource_type: resourceType,
      type: 'upload',
      invalidate: true
    });
    return response || { result: 'ok' };
  } catch (error) {
    const message = String(error?.message || '').toLowerCase();
    if (message.includes('not found')) {
      return { result: 'not_found' };
    }
    throw error;
  }
};

const fetchWhatsAppMediaBinary = async ({ mediaId, accessToken }) => {
  const normalizedMediaId = String(mediaId || '').trim();
  const normalizedToken = String(accessToken || '').trim();
  if (!normalizedMediaId || !normalizedToken) {
    const error = new Error('mediaId and accessToken are required to fetch WhatsApp media.');
    error.status = 400;
    throw error;
  }

  const metaResponse = await axios.get(`${GRAPH_API_BASE_URL}/${encodeURIComponent(normalizedMediaId)}`, {
    headers: {
      Authorization: `Bearer ${normalizedToken}`
    },
    timeout: 15000
  });

  const downloadUrl = String(metaResponse?.data?.url || '').trim();
  if (!downloadUrl) {
    const error = new Error('Unable to resolve WhatsApp media download URL.');
    error.status = 502;
    throw error;
  }

  const binaryResponse = await axios.get(downloadUrl, {
    headers: {
      Authorization: `Bearer ${normalizedToken}`
    },
    responseType: 'arraybuffer',
    timeout: 30000
  });

  return {
    buffer: Buffer.from(binaryResponse.data),
    mimeType:
      String(metaResponse?.data?.mime_type || '').trim().toLowerCase() ||
      String(binaryResponse?.headers?.['content-type'] || '').trim().toLowerCase(),
    fileSize: Number(binaryResponse?.headers?.['content-length'] || 0) || Buffer.byteLength(binaryResponse.data || []),
    sourceMeta: metaResponse?.data || {}
  };
};

const downloadAndStoreIncomingWhatsAppMedia = async ({
  mediaId,
  credentials,
  username,
  companyContext = null,
  userId,
  sender,
  recipient,
  fallbackMimeType = '',
  fallbackFileName = ''
}) => {
  const accessToken = String(credentials?.accessToken || credentials?.whatsappToken || '').trim();
  const binary = await fetchWhatsAppMediaBinary({ mediaId, accessToken });
  const mimeType = String(binary.mimeType || fallbackMimeType || '').trim().toLowerCase();
  const extension = String(path.extname(String(fallbackFileName || '')).toLowerCase() || '').replace('.', '');
  const inferredExtension = mimeType.includes('/')
    ? sanitizeStorageSegment(mimeType.split('/')[1], '')
    : '';
  const safeExtension = extension || inferredExtension || 'bin';
  const fileName = String(fallbackFileName || `incoming-${Date.now()}.${safeExtension}`).trim();

  return uploadInboxAttachment({
    file: {
      buffer: binary.buffer,
      mimetype: mimeType || 'application/octet-stream',
      originalname: fileName,
      size: Number(binary.fileSize || binary.buffer.length || 0)
    },
    username,
    companyContext,
    direction: 'received',
    userId,
    sender,
    recipient
  });
};

module.exports = {
  hasCloudinaryCredentials,
  sanitizeStorageSegment,
  resolveInboxStorageUsername,
  resolveInboxFolderPath,
  ensureInboxFolders,
  validateAttachmentFile,
  uploadInboxAttachment,
  isAttachmentPathOwned,
  generateSignedAttachmentUrl,
  generateAttachmentDownloadUrl,
  deleteInboxAttachment,
  fetchWhatsAppMediaBinary,
  downloadAndStoreIncomingWhatsAppMedia
};
