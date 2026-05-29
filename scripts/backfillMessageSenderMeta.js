require('dotenv').config();

const connectDB = require('../config/database');
const Message = require('../models/Message');
const User = require('../models/User');
const { normalizeRole } = require('../utils/accessControl');
const mongoose = require('mongoose');

const BATCH_SIZE = Math.max(25, Number(process.env.BACKFILL_MESSAGE_SENDER_BATCH_SIZE || 500));

const toCleanString = (value = '') => String(value || '').trim();

const toObjectIdIfValid = (value) => {
  const normalized = toCleanString(value);
  if (!normalized) return null;
  return mongoose.Types.ObjectId.isValid(normalized)
    ? new mongoose.Types.ObjectId(normalized)
    : null;
};

const resolveSenderRole = (user = {}, message = {}) => {
  const existingRole = toCleanString(message?.senderRole).toLowerCase();
  if (existingRole) return existingRole;

  const normalizedRole = normalizeRole(user?.normalizedRole || user?.companyRole || user?.role);
  return normalizedRole === 'agent' ? 'agent' : 'admin';
};

const resolveSenderName = (user = {}, message = {}, senderRole = 'agent') => {
  const candidates = [
    message?.senderName,
    user?.displayName,
    user?.fullName,
    user?.name,
    user?.username,
    user?.email
  ];

  for (const candidate of candidates) {
    const normalized = toCleanString(candidate);
    if (normalized) return normalized;
  }

  return senderRole === 'admin' ? 'Admin' : 'Agent';
};

const main = async () => {
  await connectDB();

  const query = {
    sender: 'agent',
    $or: [
      { senderRole: { $exists: false } },
      { senderRole: null },
      { senderRole: '' },
      { senderName: { $exists: false } },
      { senderName: null },
      { senderName: '' },
      { senderId: { $exists: false } },
      { senderId: null }
    ]
  };

  const total = await Message.countDocuments(query);
  if (!total) {
    console.log('No message sender metadata backfill needed.');
    return;
  }

  console.log(`Backfilling sender metadata for ${total} message(s)...`);

  const userIds = await Message.distinct('userId', query);
  const userDocs = await User.find({
    _id: { $in: userIds.filter(Boolean) }
  })
    .select('_id name email role')
    .lean();

  const userMap = new Map(userDocs.map((user) => [String(user._id), user]));

  let processed = 0;
  let updated = 0;
  let skipped = 0;
  let bulkOps = [];

  const cursor = Message.find(query)
    .select('_id userId sender senderRole senderName senderId')
    .sort({ timestamp: 1, _id: 1 })
    .lean()
    .cursor();

  for await (const message of cursor) {
    processed += 1;
    const messageUserId = String(message?.userId || '').trim();
    const user = userMap.get(messageUserId) || null;
    const senderRole = resolveSenderRole(user || {}, message || {});
    const senderName = resolveSenderName(user || {}, message || {}, senderRole);
    const senderId = toObjectIdIfValid(message?.senderId || messageUserId);

    const nextSenderRole = toCleanString(message?.senderRole).toLowerCase() || senderRole;
    const nextSenderName = toCleanString(message?.senderName) || senderName;
    const nextSenderId = toObjectIdIfValid(message?.senderId || messageUserId);

    const shouldUpdate =
      nextSenderRole !== toCleanString(message?.senderRole).toLowerCase() ||
      nextSenderName !== toCleanString(message?.senderName) ||
      String(nextSenderId || '') !== String(message?.senderId || '');

    if (!shouldUpdate) {
      skipped += 1;
      continue;
    }

    bulkOps.push({
      updateOne: {
        filter: { _id: message._id },
        update: {
          $set: {
            senderId,
            senderRole: nextSenderRole,
            senderName: nextSenderName
          }
        }
      }
    });

    if (bulkOps.length >= BATCH_SIZE) {
      const result = await Message.bulkWrite(bulkOps, { ordered: false });
      updated += result.modifiedCount || 0;
      bulkOps = [];
      console.log(`Processed ${processed}/${total} messages...`);
    }
  }

  if (bulkOps.length > 0) {
    const result = await Message.bulkWrite(bulkOps, { ordered: false });
    updated += result.modifiedCount || 0;
  }

  console.log(
    `Message sender metadata backfill complete. Processed: ${processed}, updated: ${updated}, skipped: ${skipped}.`
  );
};

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Message sender metadata backfill failed:', error);
    process.exit(1);
  });
