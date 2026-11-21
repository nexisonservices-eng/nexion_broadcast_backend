const getAscii = (buffer, start, end) => buffer.toString('ascii', start, end);

const hasTemplateHeaderFileSignature = (buffer, headerType) => {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return false;

  if (headerType === 'image') {
    const isJpeg = buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff;
    const isPng = buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
    return isJpeg || isPng;
  }

  if (headerType === 'video') {
    return buffer.length >= 8 && getAscii(buffer, 4, 8) === 'ftyp';
  }

  if (headerType === 'document') {
    return buffer.length >= 5 && getAscii(buffer, 0, 5) === '%PDF-';
  }

  return false;
};

const validateTemplateHeaderFile = (file, headerType, mediaDefinition) => {
  if (!mediaDefinition) {
    return {
      ok: false,
      statusCode: 400,
      error: 'A valid template header type (image, video, or document) is required'
    };
  }

  const normalizedMimeType = String(file?.mimetype || '').trim().toLowerCase();
  if (!mediaDefinition.mimeTypes.includes(normalizedMimeType)) {
    return {
      ok: false,
      statusCode: 400,
      error: `${headerType[0].toUpperCase()}${headerType.slice(1)} headers must be ${mediaDefinition.formats} files`
    };
  }

  const sizeBytes = Number(file?.size || file?.buffer?.length || 0);
  const maxBytes = mediaDefinition.maxMB * 1024 * 1024;
  if (sizeBytes > maxBytes) {
    return {
      ok: false,
      statusCode: 413,
      error: `${headerType[0].toUpperCase()}${headerType.slice(1)} headers must be ${mediaDefinition.maxMB} MB or smaller`
    };
  }

  if (!hasTemplateHeaderFileSignature(file?.buffer, headerType)) {
    return {
      ok: false,
      statusCode: 400,
      error: `The uploaded file content does not match the ${headerType} header format`
    };
  }

  return { ok: true };
};

module.exports = {
  hasTemplateHeaderFileSignature,
  validateTemplateHeaderFile
};
