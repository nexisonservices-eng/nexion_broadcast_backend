const { v2: cloudinary } = require('cloudinary');

let configured = false;

const hasCloudinaryCredentials = () =>
  Boolean(process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET);

const ensureCloudinaryConfig = () => {
  if (configured || !hasCloudinaryCredentials()) return hasCloudinaryCredentials();
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
  });
  configured = true;
  return true;
};

const extractPublicIdFromUrl = (value = '') => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const match = raw.match(/\/(?:image|video|raw)\/upload\/(?:[^/]+\/)*?(?:v\d+\/)?([^?]+?)(?:\.[a-zA-Z0-9]+)?(?:\?|$)/);
  if (!match) return raw.startsWith('http') ? '' : raw.replace(/\.(mp3|wav|m4a|ogg|pdf|docx?|xlsx?|png|jpe?g|webp|mp4|mov)$/i, '');
  return decodeURIComponent(match[1]).replace(/\.(mp3|wav|m4a|ogg|pdf|docx?|xlsx?|png|jpe?g|webp|mp4|mov)$/i, '');
};

const deleteAsset = async ({ publicId, url, resourceType = 'image' } = {}) => {
  if (!ensureCloudinaryConfig()) return { skipped: true };
  const id = extractPublicIdFromUrl(publicId || url);
  if (!id) return { skipped: true };
  const types = resourceType === 'auto' ? ['image', 'video', 'raw'] : [resourceType || 'image'];
  for (const type of types) {
    const result = await cloudinary.uploader.destroy(id, {
      resource_type: type,
      type: 'upload',
      invalidate: true
    });
    if (result?.result === 'ok' || result?.result === 'not found') {
      return { result: result.result, publicId: id, resourceType: type };
    }
  }
  return { result: 'not_found', publicId: id };
};

const deleteAssets = async (assets = []) => {
  const summary = { attempted: 0, deleted: 0, skipped: 0, warnings: [] };
  const seen = new Set();
  for (const asset of assets) {
    const id = extractPublicIdFromUrl(asset?.publicId || asset?.url);
    const key = `${asset?.resourceType || 'auto'}:${id}`;
    if (!id || seen.has(key)) {
      summary.skipped += 1;
      continue;
    }
    seen.add(key);
    summary.attempted += 1;
    try {
      const result = await deleteAsset({ ...asset, publicId: id, resourceType: asset?.resourceType || 'auto' });
      if (result?.result === 'ok') summary.deleted += 1;
    } catch (error) {
      summary.warnings.push(`Failed deleting Cloudinary asset ${id}: ${error.message}`);
    }
  }
  return summary;
};

const deleteFolderPrefix = async (prefix) => {
  if (!ensureCloudinaryConfig() || !prefix) return { skipped: true };
  const warnings = [];
  for (const resourceType of ['image', 'video', 'raw']) {
    try {
      await cloudinary.api.delete_resources_by_prefix(prefix, { resource_type: resourceType, invalidate: true });
    } catch (error) {
      warnings.push(`Failed deleting ${resourceType} resources by prefix ${prefix}: ${error.message}`);
    }
  }
  return { prefix, warnings };
};

module.exports = {
  extractPublicIdFromUrl,
  deleteAsset,
  deleteAssets,
  deleteFolderPrefix
};
