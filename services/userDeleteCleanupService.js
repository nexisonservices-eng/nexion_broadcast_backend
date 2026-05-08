const mongoose = require('mongoose');
const Message = require('../models/Message');
const { ContactDocument } = require('../models/ContactDocument');
const Campaign = require('../models/campaign');
const Template = require('../models/Template');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Broadcast = require('../models/Broadcast');
const Deal = require('../models/Deal');
const CrmAutomationRun = require('../models/CrmAutomationRun');
const CrmPipelineStage = require('../models/CrmPipelineStage');
const CrmPipelineView = require('../models/CrmPipelineView');
const GoogleCalendarConnection = require('../models/GoogleCalendarConnection');
const LeadActivity = require('../models/LeadActivity');
const LeadScoringConfig = require('../models/LeadScoringConfig');
const LeadTask = require('../models/LeadTask');
const MetaAdCampaign = require('../models/MetaAdCampaign');
const MetaAdsConnection = require('../models/MetaAdsConnection');
const MetaAdsTransaction = require('../models/MetaAdsTransaction');
const MetaAdsWallet = require('../models/MetaAdsWallet');
const MissedCall = require('../models/MissedCall');
const AudienceSegment = require('../models/AudienceSegment');
const ConsentExportJob = require('../models/ConsentExportJob');
const WhatsAppConsentLog = require('../models/WhatsAppConsentLog');
const WhatsAppWorkflow = require('../models/WhatsAppWorkflow');
const { deleteAssets, deleteFolderPrefix } = require('./cloudinaryDeleteService');
const { resolveCompanyRoot } = require('./cloudinaryCompanyFolders');

const asObjectId = (value) =>
  mongoose.Types.ObjectId.isValid(String(value || '')) ? new mongoose.Types.ObjectId(String(value)) : null;

const countDelete = async (model, filter, counts, key) => {
  const result = await model.deleteMany(filter);
  counts[key] = result.deletedCount || 0;
};

const cleanupUserDelete = async ({ userId, companyId, deleteCompanyScope = false, companyName = '', companySlug = '', cloudinaryFolderRoot = '' }) => {
  const normalizedUserId = String(userId || '').trim();
  const normalizedCompanyId = String(companyId || '').trim();
  if (!normalizedUserId) {
    const error = new Error('userId is required');
    error.status = 400;
    throw error;
  }

  const userObjectId = asObjectId(normalizedUserId);
  const companyObjectId = asObjectId(normalizedCompanyId);
  const userFilter = userObjectId ? userObjectId : normalizedUserId;
  const companyFilter = companyObjectId ? companyObjectId : normalizedCompanyId;
  const counts = {};
  const warnings = [];

  const assetOwnerFilter = deleteCompanyScope && normalizedCompanyId ? { companyId: companyFilter } : { userId: userFilter };
  const messages = await Message.find(assetOwnerFilter).select('attachment mediaUrl mediaType').lean();
  const docs = await ContactDocument.find(assetOwnerFilter).select('attachment').lean();
  const campaigns = await Campaign.find({
    $or: [{ createdBy: userFilter }, { createdBy: normalizedUserId }, ...(deleteCompanyScope && normalizedCompanyId ? [{ companyId: normalizedCompanyId }] : [])]
  }).select('imageUrl videoUrl').lean();
  const templates = await Template.find(assetOwnerFilter).select('content').lean();

  const assets = [];
  messages.forEach((item) => {
    if (item?.attachment?.publicId) assets.push({ publicId: item.attachment.publicId, resourceType: item.attachment.resourceType || 'auto' });
    if (item?.mediaUrl) assets.push({ url: item.mediaUrl, resourceType: item.mediaType === 'audio' ? 'raw' : item.mediaType || 'auto' });
  });
  docs.forEach((item) => {
    if (item?.attachment?.publicId) assets.push({ publicId: item.attachment.publicId, resourceType: item.attachment.resourceType || 'auto' });
    if (item?.attachment?.secureUrl) assets.push({ url: item.attachment.secureUrl, resourceType: item.attachment.resourceType || 'auto' });
  });
  campaigns.forEach((item) => {
    if (item?.imageUrl) assets.push({ url: item.imageUrl, resourceType: 'image' });
    if (item?.videoUrl) assets.push({ url: item.videoUrl, resourceType: 'video' });
  });
  templates.forEach((item) => {
    const mediaUrl = item?.content?.header?.mediaUrl;
    if (mediaUrl) assets.push({ url: mediaUrl, resourceType: item?.content?.header?.type || 'auto' });
  });

  const cloudinary = await deleteAssets(assets);
  warnings.push(...cloudinary.warnings);

  await countDelete(Message, { userId: userFilter }, counts, 'messages');
  await countDelete(ContactDocument, { userId: userFilter }, counts, 'contactDocuments');
  await countDelete(Contact, { userId: userFilter }, counts, 'contacts');
  await countDelete(Conversation, { userId: userFilter }, counts, 'conversations');
  await countDelete(Broadcast, { $or: [{ createdById: userFilter }, { createdBy: normalizedUserId }] }, counts, 'broadcasts');
  await countDelete(Campaign, { $or: [{ createdBy: userFilter }, { createdBy: normalizedUserId }] }, counts, 'campaigns');
  await countDelete(Template, { userId: userFilter }, counts, 'templates');
  await countDelete(Deal, { userId: userFilter }, counts, 'deals');
  await countDelete(CrmAutomationRun, { userId: userFilter }, counts, 'crmAutomationRuns');
  await countDelete(CrmPipelineStage, { userId: userFilter }, counts, 'crmPipelineStages');
  await countDelete(CrmPipelineView, { userId: userFilter }, counts, 'crmPipelineViews');
  await countDelete(GoogleCalendarConnection, { userId: userFilter }, counts, 'googleCalendarConnections');
  await countDelete(LeadActivity, { userId: userFilter }, counts, 'leadActivities');
  await countDelete(LeadScoringConfig, { userId: userFilter }, counts, 'leadScoringConfigs');
  await countDelete(LeadTask, { userId: userFilter }, counts, 'leadTasks');
  await countDelete(MetaAdCampaign, { userId: normalizedUserId }, counts, 'metaAdCampaigns');
  await countDelete(MetaAdsConnection, { userId: normalizedUserId }, counts, 'metaAdsConnections');
  await countDelete(MetaAdsTransaction, { userId: normalizedUserId }, counts, 'metaAdsTransactions');
  await countDelete(MetaAdsWallet, { userId: normalizedUserId }, counts, 'metaAdsWallets');
  await countDelete(MissedCall, { userId: userFilter }, counts, 'missedCalls');
  await countDelete(AudienceSegment, { userId: userFilter }, counts, 'audienceSegments');
  await countDelete(ConsentExportJob, { userId: userFilter }, counts, 'consentExportJobs');
  await countDelete(WhatsAppConsentLog, { userId: userFilter }, counts, 'whatsAppConsentLogs');
  await countDelete(WhatsAppWorkflow, { userId: normalizedUserId }, counts, 'whatsAppWorkflows');

  if (deleteCompanyScope && normalizedCompanyId) {
    const companyQuery = { companyId: companyFilter };
    await countDelete(Message, companyQuery, counts, 'companyMessages');
    await countDelete(ContactDocument, companyQuery, counts, 'companyContactDocuments');
    await countDelete(Contact, companyQuery, counts, 'companyContacts');
    await countDelete(Conversation, companyQuery, counts, 'companyConversations');
    await countDelete(Broadcast, companyQuery, counts, 'companyBroadcasts');
    await countDelete(Template, companyQuery, counts, 'companyTemplates');
    await countDelete(Deal, companyQuery, counts, 'companyDeals');
    await countDelete(CrmAutomationRun, companyQuery, counts, 'companyCrmAutomationRuns');
    await countDelete(CrmPipelineStage, companyQuery, counts, 'companyCrmPipelineStages');
    await countDelete(CrmPipelineView, companyQuery, counts, 'companyCrmPipelineViews');
    await countDelete(GoogleCalendarConnection, companyQuery, counts, 'companyGoogleCalendarConnections');
    await countDelete(LeadActivity, companyQuery, counts, 'companyLeadActivities');
    await countDelete(LeadScoringConfig, companyQuery, counts, 'companyLeadScoringConfigs');
    await countDelete(LeadTask, companyQuery, counts, 'companyLeadTasks');
    await countDelete(MissedCall, companyQuery, counts, 'companyMissedCalls');
    await countDelete(AudienceSegment, companyQuery, counts, 'companyAudienceSegments');
    await countDelete(ConsentExportJob, companyQuery, counts, 'companyConsentExportJobs');
    await countDelete(WhatsAppConsentLog, companyQuery, counts, 'companyWhatsAppConsentLogs');
    await countDelete(WhatsAppWorkflow, { companyId: normalizedCompanyId }, counts, 'companyWhatsAppWorkflows');
    await countDelete(Campaign, { companyId: normalizedCompanyId }, counts, 'companyCampaigns');
    const root = resolveCompanyRoot({ companyId: normalizedCompanyId, companyName, companySlug, cloudinaryFolderRoot });
    const prefixResult = await deleteFolderPrefix(root);
    warnings.push(...(prefixResult.warnings || []));
  }

  return { counts, cloudinary, warnings };
};

module.exports = {
  cleanupUserDelete
};
