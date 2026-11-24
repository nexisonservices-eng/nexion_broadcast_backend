const HEADER_TYPES = new Set(['image', 'video', 'document', 'text', 'none']);

const findHeaderComponent = (components) =>
  (Array.isArray(components) ? components : []).find(
    (component) => String(component?.type || '').trim().toUpperCase() === 'HEADER'
  ) || null;

const getTemplateHeaderType = (template = {}) => {
  if (!template || typeof template !== 'object') return 'none';

  const header =
    findHeaderComponent(template.metaComponents) ||
    findHeaderComponent(template.components) ||
    template.content?.header ||
    template.header ||
    null;
  const rawType = header?.format || header?.type || '';
  const normalizedType = String(rawType || '').trim().toLowerCase();

  return HEADER_TYPES.has(normalizedType) ? normalizedType : 'none';
};

module.exports = { HEADER_TYPES, getTemplateHeaderType };
