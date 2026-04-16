const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const auth = require('../middleware/auth');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const LeadTask = require('../models/LeadTask');
const LeadActivity = require('../models/LeadActivity');
const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');
const {
  ContactDocument,
  CONTACT_DOCUMENT_TYPES,
  CONTACT_DOCUMENT_VERIFICATION_STATUSES
} = require('../models/ContactDocument');
const {
  uploadContactDocumentAttachment
} = require('../services/contactDocumentStorageService');
const {
  generateSignedAttachmentUrl,
  generateAttachmentDownloadUrl,
  deleteInboxAttachment
} = require('../services/inboxMediaService');

const router = express.Router();
router.use(auth);

const LEAD_STAGES = ['new', 'contacted', 'nurturing', 'qualified', 'proposal', 'won', 'lost'];
const LEAD_STATUSES = ['new', 'nurturing', 'qualified', 'unqualified', 'won', 'lost'];
const TASK_PRIORITIES = ['low', 'medium', 'high'];
const TASK_STATUSES = ['pending', 'in_progress', 'completed', 'cancelled'];
const DOCUMENT_ACCESS_MODES = ['view', 'download'];
const CRM_CONTACT_LIST_FIELDS = [
  '_id',
  'name',
  'phone',
  'email',
  'tags',
  'stage',
  'status',
  'ownerId',
  'nextFollowUpAt',
  'lastContact',
  'lastContactAt',
  'leadScore',
  'isBlocked',
  'whatsappOptInStatus',
  'whatsappOptInAt',
  'whatsappOptInSource',
  'whatsappOptInScope',
  'whatsappOptInTextSnapshot',
  'whatsappOptInProofType',
  'whatsappOptInProofId',
  'whatsappOptInProofUrl',
  'whatsappOptInCapturedBy',
  'whatsappOptInPageUrl',
  'whatsappOptInIp',
  'whatsappOptInUserAgent',
  'whatsappOptInMetadata',
  'whatsappOptOutAt',
  'lastInboundMessageAt',
  'serviceWindowClosesAt',
  'createdAt',
  'updatedAt'
].join(' ');
const CRM_TASK_CONTACT_FIELDS = 'name phone stage status leadScore';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.CRM_DOCUMENT_MAX_FILE_SIZE_BYTES || 30 * 1024 * 1024)
  }
});

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) => (mongoose.Types.ObjectId.isValid(value) ? value : null);

const buildScopedFilter = (req, extra = {}) => {
  const conditions = [];

  const normalizedUserId = req?.user?.id || null;
  const normalizedCompanyId = req?.companyId || null;

  if (normalizedCompanyId && normalizedUserId) {
    conditions.push({
      $or: [
        { companyId: normalizedCompanyId },
        { userId: normalizedUserId },
        {
          $and: [
            { userId: normalizedUserId },
            {
              $or: [{ companyId: null }, { companyId: { $exists: false } }]
            }
          ]
        }
      ]
    });
  } else if (normalizedCompanyId) {
    conditions.push({
      $or: [
        { companyId: normalizedCompanyId },
        { companyId: null },
        { companyId: { $exists: false } }
      ]
    });
  } else if (normalizedUserId) {
    conditions.push({ userId: normalizedUserId });
  }

  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }

  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const safeDate = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const toCleanStringArray = (value) => {
  if (Array.isArray(value)) {
    return value.map((item) => toCleanString(item)).filter(Boolean);
  }

  const normalized = String(value || '').trim();
  if (!normalized) return [];

  if ((normalized.startsWith('[') && normalized.endsWith(']')) || (normalized.startsWith('"') && normalized.endsWith('"'))) {
    try {
      const parsed = JSON.parse(normalized);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => toCleanString(item)).filter(Boolean);
      }
    } catch (_error) {
      // Fall through to comma splitting.
    }
  }

  return normalized
    .split(',')
    .map((item) => toCleanString(item))
    .filter(Boolean);
};

const logLeadActivity = async ({
  req,
  contactId,
  conversationId = null,
  type,
  meta = {}
}) => {
  try {
    if (!contactId) return;
    await LeadActivity.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId,
      conversationId,
      type,
      meta,
      createdBy: req.user.id || null
    });
  } catch (error) {
    console.error('CRM activity log failed:', error.message);
  }
};

const findRecentConversationIdForContact = async (req, contactId) => {
  const conversation = await Conversation.findOne(
    buildScopedFilter(req, { contactId })
  )
    .sort({ lastMessageTime: -1, updatedAt: -1 })
    .select('_id');

  return conversation?._id || null;
};

const loadAuthorizedContact = async (req, contactId) => {
  if (!toObjectIdIfValid(contactId)) {
    const error = new Error('Invalid contact id');
    error.status = 400;
    throw error;
  }

  const contact = await Contact.findOne(buildScopedFilter(req, { _id: contactId }));
  if (!contact) {
    const error = new Error('Contact not found');
    error.status = 404;
    throw error;
  }

  return contact;
};

const resolveAuthorizedContactForDocumentWrite = async (req, contactId, conversationId) => {
  try {
    return await loadAuthorizedContact(req, contactId);
  } catch (error) {
    const normalizedConversationId = String(conversationId || '').trim();
    if ((error?.status || 500) !== 404 || !toObjectIdIfValid(normalizedConversationId)) {
      throw error;
    }

    const conversation = await Conversation.findOne(
      buildScopedFilter(req, { _id: normalizedConversationId })
    )
      .select('_id contactId')
      .lean();

    if (!conversation?.contactId) {
      throw error;
    }

    return loadAuthorizedContact(req, conversation.contactId);
  }
};

const resolveDocumentConversationId = async (req, contact, conversationId) => {
  const normalizedConversationId = String(conversationId || '').trim();
  if (!normalizedConversationId) {
    return findRecentConversationIdForContact(req, contact._id);
  }

  if (!toObjectIdIfValid(normalizedConversationId)) {
    return findRecentConversationIdForContact(req, contact._id);
  }

  const conversation = await Conversation.findOne(
    buildScopedFilter(req, {
      _id: normalizedConversationId,
      contactId: contact._id
    })
  ).select('_id');

  if (!conversation) {
    return findRecentConversationIdForContact(req, contact._id);
  }

  return conversation._id;
};

const runSingleDocumentUpload = (req, res) =>
  new Promise((resolve, reject) => {
    upload.single('file')(req, res, (error) => {
      if (error) return reject(error);
      resolve(req.file);
    });
  });

const buildDocumentAccessPayload = ({ document, mode }) => {
  const attachment = document?.attachment || {};
  const directUrl = String(attachment?.secureUrl || '').trim();
  const accessPayload =
    String(mode || '').trim().toLowerCase() === 'download'
      ? generateAttachmentDownloadUrl({
          attachment: {
            ...attachment,
            secureUrl: directUrl
          }
        })
      : generateSignedAttachmentUrl({
          attachment: {
            ...attachment,
            secureUrl: directUrl
          },
          mode: 'view'
        });

  return {
    url: String(accessPayload?.url || directUrl || '').trim(),
    expiresAt: accessPayload?.expiresAt || null,
    fileName: String(attachment?.originalFileName || document?.title || 'document').trim() || 'document'
  };
};

const getCrmRouteErrorStatus = (error) => {
  if (String(error?.code || '').trim().toUpperCase() === 'LIMIT_FILE_SIZE') {
    return 413;
  }
  return error?.status || 500;
};

router.get('/contacts', async (req, res) => {
  try {
    const {
      search,
      stage,
      status,
      ownerId,
      minScore,
      maxScore,
      hasFollowUp,
      page = 1,
      limit = 50
    } = req.query;

    const extraFilter = {};
    const normalizedStage = toCleanString(stage).toLowerCase();
    const normalizedStatus = toCleanString(status).toLowerCase();
    const normalizedOwnerId = toCleanString(ownerId);

    if (normalizedStage) {
      if (!LEAD_STAGES.includes(normalizedStage)) {
        return res.status(400).json({ success: false, error: 'Invalid stage filter' });
      }
      extraFilter.stage = normalizedStage;
    }

    if (normalizedStatus) {
      if (!LEAD_STATUSES.includes(normalizedStatus)) {
        return res.status(400).json({ success: false, error: 'Invalid status filter' });
      }
      extraFilter.status = normalizedStatus;
    }

    if (normalizedOwnerId) {
      extraFilter.ownerId = normalizedOwnerId;
    }

    const minScoreNumber = Number(minScore);
    const maxScoreNumber = Number(maxScore);
    if (Number.isFinite(minScoreNumber) || Number.isFinite(maxScoreNumber)) {
      extraFilter.leadScore = {};
      if (Number.isFinite(minScoreNumber)) extraFilter.leadScore.$gte = minScoreNumber;
      if (Number.isFinite(maxScoreNumber)) extraFilter.leadScore.$lte = maxScoreNumber;
    }

    if (String(hasFollowUp || '').toLowerCase() === 'true') {
      extraFilter.nextFollowUpAt = { $ne: null };
    }
    if (String(hasFollowUp || '').toLowerCase() === 'false') {
      extraFilter.nextFollowUpAt = null;
    }

    const normalizedSearch = toCleanString(search);
    if (normalizedSearch) {
      extraFilter.$or = [
        { name: { $regex: normalizedSearch, $options: 'i' } },
        { phone: { $regex: normalizedSearch, $options: 'i' } },
        { email: { $regex: normalizedSearch, $options: 'i' } },
        { notes: { $regex: normalizedSearch, $options: 'i' } },
        { tags: { $in: [new RegExp(normalizedSearch, 'i')] } }
      ];
    }

    const pageNumber = Math.max(Number(page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const skip = (pageNumber - 1) * pageSize;
    const scopedFilter = buildScopedFilter(req, extraFilter);

    const [contacts, total] = await Promise.all([
      Contact.find(scopedFilter)
        .select(CRM_CONTACT_LIST_FIELDS)
        .sort({ nextFollowUpAt: 1, leadScore: -1, lastContact: -1, createdAt: -1 })
        .skip(skip)
        .limit(pageSize)
        .lean(),
      Contact.countDocuments(scopedFilter)
    ]);

    res.json({
      success: true,
      data: contacts,
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total,
        totalPages: Math.max(Math.ceil(total / pageSize), 1)
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/contacts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const [openTasksCount, lastActivity] = await Promise.all([
      LeadTask.countDocuments(
        buildScopedFilter(req, {
          contactId: contact._id,
          status: { $in: ['pending', 'in_progress'] }
        })
      ),
      LeadActivity.findOne(buildScopedFilter(req, { contactId: contact._id }))
        .sort({ createdAt: -1 })
    ]);

    res.json({
      success: true,
      data: {
        ...contact.toObject(),
        openTasksCount,
        lastActivity
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/contacts/:id/stage', async (req, res) => {
  try {
    const { id } = req.params;
    const stage = toCleanString(req.body?.stage).toLowerCase();

    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }
    if (!LEAD_STAGES.includes(stage)) {
      return res.status(400).json({ success: false, error: 'Invalid stage' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousStage = contact.stage || '';
    const previousStatus = contact.status || '';
    contact.stage = stage;

    const statusFromStage = {
      qualified: 'qualified',
      won: 'won',
      lost: 'lost',
      nurturing: 'nurturing',
      new: 'new'
    };
    if (statusFromStage[stage]) {
      contact.status = statusFromStage[stage];
    }

    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'stage_changed',
      meta: {
        previousStage,
        nextStage: contact.stage,
        previousStatus,
        nextStatus: contact.status
      }
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/contacts/:id/owner', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousOwner = contact.ownerId || null;
    const nextOwner = toCleanString(req.body?.ownerId) || null;
    contact.ownerId = nextOwner;
    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'owner_changed',
      meta: {
        previousOwner,
        nextOwner
      }
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/contacts/:id/notes', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    if (req.body?.note === undefined) {
      return res.status(400).json({ success: false, error: 'note is required' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: id }));
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const previousNote = String(contact.notes || '').trim();
    const nextNote = toCleanString(req.body.note);
    contact.notes = nextNote;

    const followUpDate = safeDate(req.body?.nextFollowUpAt);
    if (req.body?.nextFollowUpAt !== undefined) {
      contact.nextFollowUpAt = followUpDate;
    }

    await contact.save();

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId: await findRecentConversationIdForContact(req, contact._id),
      type: 'note_updated',
      meta: {
        previousNote,
        nextNote,
        nextFollowUpAt: contact.nextFollowUpAt
      }
    });

    res.json({ success: true, data: contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/contacts/:id/documents', async (req, res) => {
  try {
    const contact = await loadAuthorizedContact(req, req.params.id);
    const documents = await ContactDocument.find(
      buildScopedFilter(req, {
        contactId: contact._id,
        status: { $ne: 'deleted' }
      })
    ).sort({ createdAt: -1, updatedAt: -1 });

    res.json({
      success: true,
      data: documents,
      meta: {
        documentTypes: CONTACT_DOCUMENT_TYPES,
        verificationStatuses: CONTACT_DOCUMENT_VERIFICATION_STATUSES
      }
    });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.post('/contacts/:id/documents', async (req, res) => {
  try {
    await runSingleDocumentUpload(req, res);
    const contact = await resolveAuthorizedContactForDocumentWrite(
      req,
      req.params.id,
      req.body?.conversationId
    );

    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Document file is required' });
    }

    const documentType = toCleanString(req.body?.documentType).toLowerCase() || 'other';
    if (!CONTACT_DOCUMENT_TYPES.includes(documentType)) {
      return res.status(400).json({ success: false, error: 'Invalid document type' });
    }

    const verificationStatus =
      toCleanString(req.body?.verificationStatus).toLowerCase() || 'not_required';
    if (!CONTACT_DOCUMENT_VERIFICATION_STATUSES.includes(verificationStatus)) {
      return res.status(400).json({ success: false, error: 'Invalid verification status' });
    }

    const conversationId = await resolveDocumentConversationId(req, contact, req.body?.conversationId);
    const attachment = await uploadContactDocumentAttachment({
      file: req.file,
      user: req.user,
      contact,
      sender: String(req.user?.username || req.user?.email || req.user?.id || '').trim(),
      recipient: String(contact?.phone || '').trim()
    });

    const document = await ContactDocument.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId: contact._id,
      conversationId: conversationId || null,
      title: toCleanString(req.body?.title) || String(attachment?.originalFileName || '').trim(),
      description: toCleanString(req.body?.description),
      documentType,
      verificationStatus,
      tags: toCleanStringArray(req.body?.tags),
      attachment,
      metadata: {
        source: 'crm_contact_panel',
        uploadedVia: 'team_inbox'
      },
      createdBy: req.user.id || null
    });

    await logLeadActivity({
      req,
      contactId: contact._id,
      conversationId,
      type: 'document_uploaded',
      meta: {
        documentId: String(document._id),
        title: document.title,
        documentType: document.documentType,
        fileName: attachment?.originalFileName || '',
        verificationStatus: document.verificationStatus
      }
    });

    res.status(201).json({ success: true, data: document });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.post('/tasks', async (req, res) => {
  try {
    const {
      contactId,
      conversationId,
      title,
      description,
      dueAt,
      priority = 'medium',
      status = 'pending',
      assignedTo = null
    } = req.body || {};

    if (!toObjectIdIfValid(contactId)) {
      return res.status(400).json({ success: false, error: 'Valid contactId is required' });
    }

    const normalizedTitle = toCleanString(title);
    if (!normalizedTitle) {
      return res.status(400).json({ success: false, error: 'Task title is required' });
    }

    const normalizedPriority = toCleanString(priority).toLowerCase() || 'medium';
    const normalizedStatus = toCleanString(status).toLowerCase() || 'pending';

    if (!TASK_PRIORITIES.includes(normalizedPriority)) {
      return res.status(400).json({ success: false, error: 'Invalid task priority' });
    }
    if (!TASK_STATUSES.includes(normalizedStatus)) {
      return res.status(400).json({ success: false, error: 'Invalid task status' });
    }

    const contact = await Contact.findOne(buildScopedFilter(req, { _id: contactId })).select('_id');
    if (!contact) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const parsedDueAt = safeDate(dueAt);
    if (dueAt !== undefined && !parsedDueAt) {
      return res.status(400).json({ success: false, error: 'Invalid dueAt date' });
    }

    const finalConversationId = toObjectIdIfValid(conversationId)
      ? conversationId
      : await findRecentConversationIdForContact(req, contact._id);

    const task = await LeadTask.create({
      userId: req.user.id,
      companyId: req.companyId || null,
      contactId: contact._id,
      conversationId: finalConversationId,
      title: normalizedTitle,
      description: toCleanString(description),
      dueAt: parsedDueAt,
      priority: normalizedPriority,
      status: normalizedStatus,
      assignedTo: toCleanString(assignedTo) || null,
      createdBy: req.user.id || null,
      completedAt: normalizedStatus === 'completed' ? new Date() : null
    });

    await logLeadActivity({
      req,
      contactId: task.contactId,
      conversationId: task.conversationId,
      type: 'task_created',
      meta: {
        taskId: String(task._id),
        title: task.title,
        dueAt: task.dueAt,
        priority: task.priority,
        status: task.status
      }
    });

    res.status(201).json({ success: true, data: task });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/tasks', async (req, res) => {
  try {
    const {
      status,
      priority,
      assignedTo,
      contactId,
      dueFrom,
      dueTo,
      page = 1,
      limit = 50
    } = req.query;

    const extraFilter = {};
    const statusList = String(status || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);
    const priorityList = String(priority || '')
      .split(',')
      .map((item) => toCleanString(item).toLowerCase())
      .filter(Boolean);

    if (statusList.length > 0) {
      const invalidStatus = statusList.find((item) => !TASK_STATUSES.includes(item));
      if (invalidStatus) {
        return res.status(400).json({ success: false, error: `Invalid task status: ${invalidStatus}` });
      }
      extraFilter.status = { $in: statusList };
    }

    if (priorityList.length > 0) {
      const invalidPriority = priorityList.find((item) => !TASK_PRIORITIES.includes(item));
      if (invalidPriority) {
        return res.status(400).json({ success: false, error: `Invalid task priority: ${invalidPriority}` });
      }
      extraFilter.priority = { $in: priorityList };
    }

    const normalizedAssignedTo = toCleanString(assignedTo);
    if (normalizedAssignedTo) extraFilter.assignedTo = normalizedAssignedTo;

    const normalizedContactId = toCleanString(contactId);
    if (normalizedContactId) {
      if (!toObjectIdIfValid(normalizedContactId)) {
        return res.status(400).json({ success: false, error: 'Invalid contactId' });
      }
      extraFilter.contactId = normalizedContactId;
    }

    const parsedDueFrom = safeDate(dueFrom);
    const parsedDueTo = safeDate(dueTo);
    if (dueFrom && !parsedDueFrom) {
      return res.status(400).json({ success: false, error: 'Invalid dueFrom date' });
    }
    if (dueTo && !parsedDueTo) {
      return res.status(400).json({ success: false, error: 'Invalid dueTo date' });
    }
    if (parsedDueFrom || parsedDueTo) {
      extraFilter.dueAt = {};
      if (parsedDueFrom) extraFilter.dueAt.$gte = parsedDueFrom;
      if (parsedDueTo) extraFilter.dueAt.$lte = parsedDueTo;
    }

    const pageNumber = Math.max(Number(page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const skip = (pageNumber - 1) * pageSize;
    const scopedFilter = buildScopedFilter(req, extraFilter);

    const [tasks, total] = await Promise.all([
      LeadTask.find(scopedFilter)
        .populate('contactId', CRM_TASK_CONTACT_FIELDS)
        .sort({ dueAt: 1, priority: -1, createdAt: -1 })
        .skip(skip)
        .limit(pageSize)
        .lean(),
      LeadTask.countDocuments(scopedFilter)
    ]);

    res.json({
      success: true,
      data: tasks,
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total,
        totalPages: Math.max(Math.ceil(total / pageSize), 1)
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.patch('/tasks/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid task id' });
    }

    const task = await LeadTask.findOne(buildScopedFilter(req, { _id: id }));
    if (!task) {
      return res.status(404).json({ success: false, error: 'Task not found' });
    }

    const previousTask = {
      title: task.title,
      description: task.description,
      dueAt: task.dueAt,
      priority: task.priority,
      status: task.status,
      assignedTo: task.assignedTo
    };

    if (req.body?.title !== undefined) {
      const nextTitle = toCleanString(req.body.title);
      if (!nextTitle) {
        return res.status(400).json({ success: false, error: 'Task title cannot be empty' });
      }
      task.title = nextTitle;
    }

    if (req.body?.description !== undefined) {
      task.description = toCleanString(req.body.description);
    }

    if (req.body?.dueAt !== undefined) {
      if (!req.body.dueAt) {
        task.dueAt = null;
      } else {
        const parsedDueAt = safeDate(req.body.dueAt);
        if (!parsedDueAt) {
          return res.status(400).json({ success: false, error: 'Invalid dueAt date' });
        }
        task.dueAt = parsedDueAt;
      }
    }

    if (req.body?.priority !== undefined) {
      const nextPriority = toCleanString(req.body.priority).toLowerCase();
      if (!TASK_PRIORITIES.includes(nextPriority)) {
        return res.status(400).json({ success: false, error: 'Invalid task priority' });
      }
      task.priority = nextPriority;
    }

    if (req.body?.status !== undefined) {
      const nextStatus = toCleanString(req.body.status).toLowerCase();
      if (!TASK_STATUSES.includes(nextStatus)) {
        return res.status(400).json({ success: false, error: 'Invalid task status' });
      }
      task.status = nextStatus;
      task.completedAt = nextStatus === 'completed' ? new Date() : null;
    }

    if (req.body?.assignedTo !== undefined) {
      task.assignedTo = toCleanString(req.body.assignedTo) || null;
    }

    await task.save();

    await logLeadActivity({
      req,
      contactId: task.contactId,
      conversationId: task.conversationId,
      type: task.status === 'completed' ? 'task_completed' : 'task_updated',
      meta: {
        taskId: String(task._id),
        previousTask,
        nextTask: {
          title: task.title,
          description: task.description,
          dueAt: task.dueAt,
          priority: task.priority,
          status: task.status,
          assignedTo: task.assignedTo
        }
      }
    });

    res.json({ success: true, data: task });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/activities/:contactId', async (req, res) => {
  try {
    const { contactId } = req.params;
    if (!toObjectIdIfValid(contactId)) {
      return res.status(400).json({ success: false, error: 'Invalid contact id' });
    }

    const limit = Math.min(Math.max(Number(req.query.limit) || 100, 1), 300);
    const scopedFilter = buildScopedFilter(req, { contactId });
    const [activities, consentLogs] = await Promise.all([
      LeadActivity.find(scopedFilter)
        .sort({ createdAt: -1 })
        .limit(limit)
        .lean(),
      WhatsAppConsentLog.find(scopedFilter)
        .sort({ createdAt: -1 })
        .limit(limit)
        .lean()
    ]);

    const consentActivities = (consentLogs || []).map((log) => ({
      _id: log._id,
      userId: log.userId,
      companyId: log.companyId,
      contactId: log.contactId,
      conversationId: null,
      type: log.action === 'opt_out' ? 'whatsapp_opt_out' : 'whatsapp_opt_in',
      meta: {
        source: log.source,
        scope: log.scope,
        proofType: log.proofType,
        proofId: log.proofId,
        capturedBy: log.capturedBy,
        consentText: log.consentText
      },
      createdAt: log.createdAt
    }));

    const merged = [...activities, ...consentActivities]
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))
      .slice(0, limit);

    res.json({ success: true, data: merged });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/documents/:id/access', async (req, res) => {
  try {
    const { id } = req.params;
    const mode = toCleanString(req.query?.mode).toLowerCase() || 'view';

    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid document id' });
    }
    if (!DOCUMENT_ACCESS_MODES.includes(mode)) {
      return res.status(400).json({ success: false, error: 'Invalid access mode' });
    }

    const document = await ContactDocument.findOne(
      buildScopedFilter(req, {
        _id: id,
        status: { $ne: 'deleted' }
      })
    ).lean();

    if (!document) {
      return res.status(404).json({ success: false, error: 'Document not found' });
    }

    const payload = buildDocumentAccessPayload({ document, mode });
    if (!payload.url) {
      return res.status(404).json({ success: false, error: 'Document access URL is unavailable' });
    }

    res.json({
      success: true,
      data: {
        ...payload,
        mode,
        documentId: String(document._id),
        documentType: document.documentType
      }
    });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.delete('/documents/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!toObjectIdIfValid(id)) {
      return res.status(400).json({ success: false, error: 'Invalid document id' });
    }

    const document = await ContactDocument.findOne(buildScopedFilter(req, { _id: id }));
    if (!document || document.status === 'deleted') {
      return res.status(404).json({ success: false, error: 'Document not found' });
    }

    if (document.attachment?.publicId || document.attachment?.secureUrl) {
      await deleteInboxAttachment({ attachment: document.attachment || {} });
    }

    document.status = 'deleted';
    document.attachment = {
      ...(document.attachment || {}),
      deletedAt: new Date(),
      deletedBy: req.user.id || null
    };
    await document.save();

    await logLeadActivity({
      req,
      contactId: document.contactId,
      conversationId: document.conversationId,
      type: 'document_deleted',
      meta: {
        documentId: String(document._id),
        title: document.title,
        documentType: document.documentType,
        fileName: document.attachment?.originalFileName || ''
      }
    });

    res.json({
      success: true,
      data: {
        _id: document._id,
        status: document.status
      }
    });
  } catch (error) {
    res.status(getCrmRouteErrorStatus(error)).json({ success: false, error: error.message });
  }
});

router.get('/metrics', async (req, res) => {
  try {
    const contactFilter = buildScopedFilter(req);
    const now = new Date();
    const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const endOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999);

    const taskScope = buildScopedFilter(req);
    const [contactMetrics, openTasksCount, overdueTasksCount, dueTodayCount, completedTasksCount] = await Promise.all([
      Contact.aggregate([
        { $match: contactFilter },
        {
          $project: {
            stage: { $toLower: { $ifNull: ['$stage', 'new'] } },
            status: { $toLower: { $ifNull: ['$status', 'nurturing'] } },
            leadScore: { $ifNull: ['$leadScore', 0] },
            normalizedTags: {
              $map: {
                input: { $ifNull: ['$tags', []] },
                as: 'tag',
                in: {
                  $toLower: {
                    $trim: {
                      input: { $toString: '$$tag' }
                    }
                  }
                }
              }
            }
          }
        },
        {
          $project: {
            stage: 1,
            status: 1,
            leadScore: 1,
            isQualified: {
              $or: [
                { $eq: ['$status', 'qualified'] },
                { $eq: ['$stage', 'qualified'] },
                { $in: ['qualified', '$normalizedTags'] }
              ]
            }
          }
        },
        {
          $facet: {
            summary: [
              {
                $group: {
                  _id: null,
                  total: { $sum: 1 },
                  qualified: {
                    $sum: {
                      $cond: ['$isQualified', 1, 0]
                    }
                  },
                  averageLeadScore: { $avg: '$leadScore' }
                }
              }
            ],
            byStage: [
              {
                $group: {
                  _id: '$stage',
                  count: { $sum: 1 }
                }
              }
            ],
            byStatus: [
              {
                $group: {
                  _id: '$status',
                  count: { $sum: 1 }
                }
              }
            ]
          }
        }
      ]),
      LeadTask.countDocuments({
        $and: [taskScope, { status: { $in: ['pending', 'in_progress'] } }]
      }),
      LeadTask.countDocuments({
        $and: [taskScope, { status: { $in: ['pending', 'in_progress'] }, dueAt: { $lt: now } }]
      }),
      LeadTask.countDocuments({
        $and: [taskScope, { status: { $in: ['pending', 'in_progress'] }, dueAt: { $gte: startOfDay, $lte: endOfDay } }]
      }),
      LeadTask.countDocuments({
        $and: [taskScope, { status: 'completed' }]
      })
    ]);

    const contactMetricsData = Array.isArray(contactMetrics) ? contactMetrics[0] || {} : {};
    const contactSummary = contactMetricsData.summary?.[0] || {};
    const byStage = (contactMetricsData.byStage || []).reduce((acc, item) => {
      const key = toCleanString(item?._id).toLowerCase() || 'new';
      acc[key] = Number(item?.count || 0);
      return acc;
    }, {});
    const byStatus = (contactMetricsData.byStatus || []).reduce((acc, item) => {
      const key = toCleanString(item?._id).toLowerCase() || 'nurturing';
      acc[key] = Number(item?.count || 0);
      return acc;
    }, {});
    const totalContacts = Number(contactSummary.total || 0);

    res.json({
      success: true,
      data: {
        contacts: {
          total: totalContacts,
          qualified: Number(contactSummary.qualified || 0),
          byStage,
          byStatus,
          averageLeadScore: totalContacts > 0 ? Number(Number(contactSummary.averageLeadScore || 0).toFixed(2)) : 0
        },
        tasks: {
          open: openTasksCount,
          overdue: overdueTasksCount,
          dueToday: dueTodayCount,
          completed: completedTasksCount
        },
        generatedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
