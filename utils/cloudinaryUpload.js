const { v2: cloudinary } = require('cloudinary');

let configured = false;

const ensureCloudinaryConfig = () => {
  if (configured) return;

  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
  });

  configured = true;
};

const hasCloudinaryCredentials = () =>
  Boolean(
    process.env.CLOUDINARY_CLOUD_NAME &&
    process.env.CLOUDINARY_API_KEY &&
    process.env.CLOUDINARY_API_SECRET
  );

const resolveResourceType = (file, preferredType = '') => {
  const normalizedPreferred = String(preferredType || '').trim().toLowerCase();
  if (['image', 'video', 'raw', 'auto'].includes(normalizedPreferred)) {
    return normalizedPreferred === 'auto' ? 'auto' : normalizedPreferred;
  }

  const mimeType = String(file?.mimetype || '').trim().toLowerCase();
  if (mimeType.startsWith('video/')) return 'video';
  if (mimeType.startsWith('image/')) return 'image';
  return 'auto';
};

const uploadCampaignCreative = async (file, options = {}) => {
  if (!file?.buffer) {
    return null;
  }

  if (!hasCloudinaryCredentials()) {
    const error = new Error('Cloudinary is not configured. Add CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, and CLOUDINARY_API_SECRET.');
    error.status = 500;
    throw error;
  }

  ensureCloudinaryConfig();

  const folder = options.folder || process.env.CLOUDINARY_FOLDER || 'meta-ads';
  const resourceType = resolveResourceType(file, options.resourceType);
  const dataUri = `data:${file.mimetype || 'image/jpeg'};base64,${file.buffer.toString('base64')}`;

  const result = await cloudinary.uploader.upload(dataUri, {
    folder,
    resource_type: resourceType,
    use_filename: true,
    unique_filename: true,
    overwrite: false
  });

  return result?.secure_url || result?.url || '';
};

module.exports = {
  hasCloudinaryCredentials,
  uploadCampaignCreative
};
