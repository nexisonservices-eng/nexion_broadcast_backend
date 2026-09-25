const router = require('express').Router();
const authenticate = require('../middleware/auth');
const { createAgentActivityHandler } = require('../utils/agentActivity');
const entry = (model, owner, title, label, extra = {}) => ({ model: require(`../models/${model}`), owner, title, label, ...extra });
const registry = {
  broadcasts: entry('Broadcast', 'createdById', ['name'], 'Broadcast'),
  templates: entry('Template', 'createdById', ['name'], 'Template', { fallback: 'userId' }),
  contacts: entry('Contact', 'createdBy', ['name', 'phone'], 'Contact', { fallback: 'userId' }),
  tasks: entry('LeadTask', 'createdBy', ['title'], 'Task', { fallback: 'userId' }),
  meetings: entry('LeadActivity', 'createdBy', ['meta.summary'], 'Meeting', { fallback: 'userId', where: { type: 'meeting_scheduled' } }),
  deals: entry('Deal', 'createdBy', ['title'], 'Deal', { fallback: 'userId' }),
  campaigns: entry('campaign', 'createdBy', ['name'], 'Campaign'),
  ads: entry('MetaAdCampaign', 'userId', ['campaignName'], 'Ad campaign'),
  workflows: entry('WhatsAppWorkflow', 'userId', ['name'], 'Workflow'),
  messages: entry('Message', 'senderId', ['text'], 'Message', { fallback: 'userId', where: { sender: 'agent' } }),
  missedcalls: entry('MissedCall', 'userId', ['fromNumber', 'phoneNumber'], 'Missed call')
};
router.get('/:kind', authenticate, createAgentActivityHandler(registry));
module.exports = router;
