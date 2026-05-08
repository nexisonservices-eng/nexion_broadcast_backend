const DEFAULT_ROOT_PREFIX = 'technova';

const sanitizeCompanySegment = (value = '', fallback = 'company') => {
  const cleaned = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_]/g, '');
  return cleaned || fallback;
};

const sanitizeStorageSegment = (value = '', fallback = 'item') => {
  const raw = String(value || '').trim().toLowerCase();
  const normalized = raw.replace(/[^a-z0-9._-]/g, '_').replace(/_+/g, '_').replace(/^_+|_+$/g, '');
  return normalized.slice(0, 64) || fallback;
};

const resolveCompanyRoot = ({
  companyId,
  companyName = '',
  companySlug = '',
  cloudinaryFolderRoot = ''
} = {}) => {
  const explicitRoot = String(cloudinaryFolderRoot || '').trim().replace(/\\/g, '/').replace(/\/+/g, '/');
  if (explicitRoot) return explicitRoot;

  const safeCompanyId = String(companyId || '').trim();
  if (!safeCompanyId) {
    const error = new Error('Company context is required for Cloudinary storage');
    error.code = 'CLOUDINARY_COMPANY_CONTEXT_MISSING';
    error.status = 400;
    throw error;
  }

  const slug = sanitizeCompanySegment(companySlug || companyName || '', 'company');
  return `${DEFAULT_ROOT_PREFIX}/${slug}_${safeCompanyId}`;
};

const resolveCompanyFolders = (context = {}) => {
  const root = resolveCompanyRoot(context);
  return {
    root,
    ivrAudioFolder: `${root}/audio/ivr-audio`,
    broadcastAudioFolder: `${root}/audio/broadcast-audio`,
    userDocumentsFolder: `${root}/user-documents`,
    metaAdsFolder: `${root}/meta-ads`,
    metaTemplateImagesFolder: `${root}/meta-template-images`,
    inboxSentFolder: `${root}/inbox/sent`,
    inboxReceivedFolder: `${root}/inbox/received`,
    crmContactsFolder: `${root}/crm/contacts`
  };
};

const resolveInboxFolderPath = ({ companyContext, direction = 'sent' }) => {
  const folders = resolveCompanyFolders(companyContext);
  return String(direction || '').trim().toLowerCase() === 'received'
    ? folders.inboxReceivedFolder
    : folders.inboxSentFolder;
};

const resolveCrmContactDocumentsFolder = ({ companyContext, contact }) => {
  const folders = resolveCompanyFolders(companyContext);
  const contactPhone = String(contact?.phone || '').replace(/\D/g, '');
  const contactName = String(contact?.name || '').trim();
  const contactId = String(contact?._id || contact?.id || '').trim();
  const contactSegment = sanitizeStorageSegment(contactPhone || contactName || contactId, contactId || 'contact');
  return `${folders.crmContactsFolder}/${contactSegment}/documents`;
};

module.exports = {
  sanitizeCompanySegment,
  sanitizeStorageSegment,
  resolveCompanyRoot,
  resolveCompanyFolders,
  resolveInboxFolderPath,
  resolveCrmContactDocumentsFolder
};
