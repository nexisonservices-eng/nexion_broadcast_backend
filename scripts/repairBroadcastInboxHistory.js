const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');
const Contact = require('../models/Contact');
const Broadcast = require('../models/Broadcast');
const BroadcastDispatch = require('../models/BroadcastDispatch');
const { bulkUpsertConversationSummaries } = require('../services/conversationSummaryService');

const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const i = t.indexOf('=');
    if (i > 0 && !process.env[t.slice(0, i).trim()]) process.env[t.slice(0, i).trim()] = t.slice(i + 1).trim();
  }
}

const arg = (names, fallback = '') => {
  const a = process.argv.slice(2);
  for (const n of names) {
    const m = a.find((x) => x.startsWith(`${n}=`));
    if (m) return m.slice(n.length + 1);
    const i = a.indexOf(n);
    if (i >= 0 && a[i + 1] && !a[i + 1].startsWith('--')) return a[i + 1];
  }
  return fallback;
};

const flag = (names) => {
  const a = process.argv.slice(2);
  return names.some((n) => a.includes(n) || a.some((x) => x.startsWith(`${n}=`)));
};

const n = (v, d) => {
  const x = Number(v);
  return Number.isFinite(x) && x > 0 ? Math.trunc(x) : d;
};

const summaryPayload = (c) => ({
  conversationId: c?._id,
  userId: c?.userId,
  companyId: c?.companyId,
  contactId: c?.contactId,
  contactPhone: c?.contactPhone,
  contactName: c?.contactName,
  channel: c?.channel || 'whatsapp',
  status: c?.status,
  assignedTo: c?.assignedTo,
  assignedToId: c?.assignedToId,
  tags: Array.isArray(c?.tags) ? c.tags : [],
  priority: c?.priority,
  lastMessageTime: c?.lastMessageTime || c?.updatedAt || c?.createdAt || null,
  lastMessage: c?.lastMessage,
  lastMessageMediaType: c?.lastMessageMediaType,
  lastMessageAttachmentName: c?.lastMessageAttachmentName,
  lastMessageAttachmentPages: c?.lastMessageAttachmentPages,
  lastMessageFrom: c?.lastMessageFrom,
  lastMessageWhatsappMessageId: c?.lastMessageWhatsappMessageId,
  lastMessageStatus: c?.lastMessageStatus,
  unreadCount: c?.unreadCount,
  notes: c?.notes,
  resolvedAt: c?.resolvedAt
});

async function repairOne(d, dryRun, stats) {
  const key = String(d.broadcastDispatchKey || '').trim();
  const phone = String(d.recipientPhone || '').trim();
  const text = String(d.messageText || '').trim();
  if (!key || !phone || !text) return false;

  const existing = (await Message.findOne({ broadcastDispatchKey: key }).lean()) ||
    (d.whatsappMessageId ? await Message.findOne({ whatsappMessageId: d.whatsappMessageId }).lean() : null);

  if (existing) {
    stats.linked += 1;
    if (!dryRun) await BroadcastDispatch.updateOne({ _id: d._id }, { $set: { messageId: existing._id, conversationId: existing.conversationId || null, whatsappMessageId: existing.whatsappMessageId || d.whatsappMessageId || '', status: 'sent', sentAt: existing.timestamp || d.sentAt || new Date(), updatedAt: new Date() } });
    return true;
  }

  const b = await Broadcast.findById(d.broadcastId).lean();
  if (!b) return false;

  let contact = await Contact.findOne({ userId: d.userId, companyId: d.companyId, phone });
  if (!contact) {
    contact = dryRun ? { _id: new mongoose.Types.ObjectId(), name: '' } : await Contact.create({ userId: d.userId, companyId: d.companyId, phone, name: '', sourceType: 'incoming_message' });
  } else if (String(contact.sourceType || '') !== 'incoming_message' && !dryRun) {
    contact.sourceType = 'incoming_message';
    await contact.save();
  }

  let conv = await Conversation.findOne({ userId: d.userId, companyId: d.companyId, contactPhone: phone, status: { $in: ['active', 'pending'] } });
  const ts = d.sentAt || d.updatedAt || d.createdAt || new Date();
  if (!conv) {
    conv = dryRun ? { _id: new mongoose.Types.ObjectId(), userId: d.userId, companyId: d.companyId, contactId: contact._id, contactPhone: phone, contactName: String(contact.name || '').trim(), channel: 'whatsapp', lastMessage: text, lastMessageTime: ts, lastMessageMediaType: '', lastMessageAttachmentName: '', lastMessageAttachmentPages: null, lastMessageFrom: 'agent', lastMessageWhatsappMessageId: d.whatsappMessageId || '', unreadCount: 0, status: 'active' } : await Conversation.create({ userId: d.userId, companyId: d.companyId, contactId: contact._id, contactPhone: phone, contactName: String(contact.name || '').trim(), channel: 'whatsapp', lastMessage: text, lastMessageTime: ts, lastMessageMediaType: '', lastMessageAttachmentName: '', lastMessageAttachmentPages: null, lastMessageFrom: 'agent', lastMessageWhatsappMessageId: d.whatsappMessageId || '' });
  } else if (!dryRun) {
    conv.lastMessage = text; conv.lastMessageTime = ts; conv.lastMessageMediaType = ''; conv.lastMessageAttachmentName = ''; conv.lastMessageAttachmentPages = null; conv.lastMessageFrom = 'agent'; conv.lastMessageWhatsappMessageId = d.whatsappMessageId || ''; await conv.save();
  }

  await bulkUpsertConversationSummaries([summaryPayload(conv)]);
  const msg = dryRun ? { _id: new mongoose.Types.ObjectId() } : await Message.create({ userId: d.userId, companyId: d.companyId, conversationId: conv._id, sender: 'agent', text, whatsappMessageId: d.whatsappMessageId || undefined, status: 'sent', timestamp: ts, ...(key ? { broadcastDispatchKey: key } : {}), ...(d.broadcastId ? { broadcastId: d.broadcastId } : {}) });
  if (!dryRun) await BroadcastDispatch.updateOne({ _id: d._id }, { $set: { messageId: msg._id, conversationId: conv._id, whatsappMessageId: d.whatsappMessageId || '', status: 'sent', sentAt: ts, updatedAt: new Date() } });
  stats.repaired += 1;
  stats.keys.push(key);
  return true;
}

(async () => {
  const batchSize = n(arg(['--batch-size', '--batchSize']) || process.env.BROADCAST_INBOX_REPAIR_BATCH_SIZE, 300);
  const rounds = n(arg(['--repair-rounds', '--repairRounds']) || process.env.BROADCAST_INBOX_REPAIR_ROUNDS, 10);
  const dryRun = flag(['--dry-run', '--dryRun']) || String(process.env.BROADCAST_INBOX_REPAIR_DRY_RUN || '').trim().toLowerCase() === 'true';
  const limit = n(arg(['--limit']), 0);
  await connectDB();
  console.log(JSON.stringify({ event: 'broadcast_inbox_history_repair_start', batchSize, rounds, dryRun, limit: limit || null }, null, 2));
  const stats = { summaries: 0, repaired: 0, linked: 0, keys: [] };
  const batch = [];
  for await (const c of Conversation.find({}).select('_id userId companyId contactId contactPhone contactName status assignedTo assignedToId tags priority lastMessageTime lastMessage lastMessageMediaType lastMessageAttachmentName lastMessageAttachmentPages lastMessageFrom lastMessageWhatsappMessageId lastMessageStatus unreadCount notes resolvedAt createdAt updatedAt').sort({ _id: 1 }).lean().cursor({ batchSize })) {
    batch.push(summaryPayload(c));
    stats.summaries += 1;
    if (limit > 0 && stats.summaries >= limit) break;
    if (batch.length >= batchSize) { if (!dryRun) await bulkUpsertConversationSummaries(batch); batch.length = 0; }
  }
  if (batch.length && !dryRun) await bulkUpsertConversationSummaries(batch);
  for (let round = 1; round <= rounds; round += 1) {
    const staleBefore = new Date(Date.now() - Math.max(120000, n(process.env.BROADCAST_DISPATCH_REPAIR_STALE_MS, 300000)));
    const dispatches = await BroadcastDispatch.find({ status: 'sent', $or: [{ messageId: { $exists: false } }, { messageId: null }], sentAt: { $lte: staleBefore } }).sort({ sentAt: 1 }).limit(batchSize).lean();
    let repairedThisRound = 0;
    for (const d of dispatches) if (await repairOne(d, dryRun, stats)) repairedThisRound += 1;
    console.log(JSON.stringify({ event: 'broadcast_inbox_history_repair_round', round, scanned: dispatches.length, repaired: repairedThisRound }, null, 2));
    if (!dispatches.length || !repairedThisRound) break;
  }
  console.log(JSON.stringify({ event: 'broadcast_inbox_history_repair_complete', ...stats, dryRun }, null, 2));
  await mongoose.connection.close();
})().catch(async (error) => {
  console.error('Broadcast inbox history repair failed:', error);
  try { await mongoose.connection.close(); } catch {}
  process.exitCode = 1;
});
