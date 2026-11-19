const mediaTypes = new Map([
  ['image', { mimeTypes: ['image/jpeg', 'image/png'], maxMB: 5, formats: 'JPEG or PNG', resourceType: 'image' }],
  ['video', { mimeTypes: ['video/mp4'], maxMB: 16, formats: 'MP4', resourceType: 'video' }],
  ['document', { mimeTypes: ['application/pdf'], maxMB: 16, formats: 'PDF', resourceType: 'raw' }]
]);

const bytesPerMB = 1024 * 1024;
const maxTemplateMediaMB = Math.max(...Array.from(mediaTypes.values(), ({ maxMB }) => maxMB));

const getTemplateHeaderMedia = (headerType) => mediaTypes.get(headerType);

const getTemplateHeaderMediaType = (mimeType) => {
  for (const [type, media] of mediaTypes) {
    if (media.mimeTypes.includes(mimeType)) return type;
  }
  return '';
};

module.exports = { bytesPerMB, maxTemplateMediaMB, getTemplateHeaderMedia, getTemplateHeaderMediaType };
