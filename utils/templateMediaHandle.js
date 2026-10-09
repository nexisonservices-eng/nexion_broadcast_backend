// A template example needs one complete handle, never a newline-joined response.
const normalizeTemplateMediaHandle = (value) => {
  const candidates = Array.isArray(value) ? value : [value];
  for (const candidate of candidates) {
    if (typeof candidate !== 'string') continue;
    for (const line of candidate.split(/\r?\n/)) {
      const handle = line.trim();
      if (/^\d+:[^\s]+$/.test(handle)) return handle;
    }
  }
  return '';
};

module.exports = { normalizeTemplateMediaHandle };
