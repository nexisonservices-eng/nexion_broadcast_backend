const fs = require('fs');
const path = require('path');
const connectDB = require('../config/database');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const Message = require('../models/Message');
const Broadcast = require('../models/Broadcast');
const { buildPhoneCandidates } = require('../services/whatsappOutreach/conversationResolver');

const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const index = trimmed.indexOf('=');
    if (index > 0 && !process.env[trimmed.slice(0, index).trim()]) {
      process.env[trimmed.slice(0, index).trim()] = trimmed.slice(index + 1).trim();
    }
  }
}

const safeString = (value = '') => String(value || '').trim();

async function markMissingChannelsAsWhatsapp() {
  const result = await Conversation.updateMany(
    {
      $or: [
        { channel: { $exists: false } },
        { channel: null },
        { channel: '' }
      ]
    },
    { $set: { channel: 'whatsapp' } }
  );

  await ConversationSummary.updateMany(
    {
      $or: [
        { channel: { $exists: false } },
        { channel: null },
        { channel: '' }
      ]
    },
    { $set: { channel: 'whatsapp' } }
  );

  return result?.modifiedCount || result?.nModified || 0;
}

async function backfillBroadcastReplies() {
  const broadcasts = await Broadcast.find({
    startedAt: { $exists: true },
    status: { $in: ['sending', 'completed', 'completed_with_errors'] }
  })
    .select('_id userId companyId createdById startedAt recipients.phone name')
    .lean();

  let updated = 0;

  for (const broadcast of broadcasts) {
    const recipientPhones = Array.from(
      new Set(
        (Array.isArray(broadcast?.recipients) ? broadcast.recipients : [])
          .map((recipient) => safeString(recipient?.phone))
          .filter(Boolean)
      )
    );

    if (!recipientPhones.length || !broadcast.startedAt) continue;

    for (const phone of recipientPhones) {
      const phoneCandidates = buildPhoneCandidates(phone);
      const conversation = await Conversation.findOne({
        userId: broadcast.createdById,
        companyId: broadcast.companyId || null,
        contactPhone: { $in: phoneCandidates }
      })
        .select('_id channel lastMessageTime')
        .lean();

      if (!conversation) continue;

      const inboundAfterBroadcast = await Message.exists({
        conversationId: conversation._id,
        sender: 'contact',
        timestamp: { $gte: broadcast.startedAt }
      });

      if (!inboundAfterBroadcast) continue;

      const currentChannel = safeString(conversation.channel).toLowerCase();
      if (currentChannel === 'broadcast_reply') continue;

      await Conversation.updateOne(
        { _id: conversation._id },
        { $set: { channel: 'broadcast_reply' } }
      );
      await ConversationSummary.updateOne(
        { conversationId: conversation._id },
        { $set: { channel: 'broadcast_reply' } }
      );
      updated += 1;
    }
  }

  return updated;
}

async function main() {
  await connectDB();
  const whatsappUpdated = await markMissingChannelsAsWhatsapp();
  const broadcastReplyUpdated = await backfillBroadcastReplies();
  console.log(
    JSON.stringify(
      {
        event: 'conversation_channel_backfill_complete',
        whatsappUpdated,
        broadcastReplyUpdated
      },
      null,
      2
    )
  );
  process.exit(0);
}

main().catch((error) => {
  console.error('Conversation channel backfill failed:', error);
  process.exit(1);
});
