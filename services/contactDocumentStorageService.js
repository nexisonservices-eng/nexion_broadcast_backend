const {
  sanitizeStorageSegment,
  resolveInboxStorageUsername,
  uploadInboxAttachment
} = require('./inboxMediaService');

const resolveContactDocumentStorageUsername = ({ username, email, userId }) =>
  resolveInboxStorageUsername({ username, email, userId });

const resolveContactDocumentFolder = ({ username, contact }) => {
  const safeUsername = sanitizeStorageSegment(username, 'user');
  const contactPhone = String(contact?.phone || '').replace(/\D/g, '');
  const contactName = String(contact?.name || '').trim();
  const contactId = String(contact?._id || contact?.id || '').trim();
  const contactSegment = sanitizeStorageSegment(
    contactPhone || contactName || contactId,
    contactId || 'contact'
  );

  return `crm/${safeUsername}/contacts/${contactSegment}/documents`;
};

const uploadContactDocumentAttachment = async ({
  file,
  user,
  contact,
  sender = '',
  recipient = ''
}) => {
  const username = resolveContactDocumentStorageUsername({
    username: user?.username,
    email: user?.email,
    userId: user?.id
  });
  const folder = resolveContactDocumentFolder({ username, contact });

  return uploadInboxAttachment({
    file,
    username,
    folderOverride: folder,
    direction: 'sent',
    userId: user?.id,
    sender,
    recipient
  });
};

module.exports = {
  resolveContactDocumentStorageUsername,
  resolveContactDocumentFolder,
  uploadContactDocumentAttachment
};
