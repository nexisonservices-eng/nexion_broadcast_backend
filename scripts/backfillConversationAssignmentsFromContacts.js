require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Conversation = require('../models/Conversation');
const ConversationSummary = require('../models/ConversationSummary');
const Contact = require('../models/Contact');
const User = require('../models/User');
const { upsertConversationSummary } = require('../services/conversationSummaryService');

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) => {
  const normalized = toCleanString(value);
  return mongoose.Types.ObjectId.isValid(normalized) ? new mongoose.Types.ObjectId(normalized) : null;
};

const readArg = (names = []) => {
  const args = process.argv.slice(2);
  for (const name of names) {
    const eqMatch = args.find((arg) => arg.startsWith(`${name}=`));
    if (eqMatch) return eqMatch.slice(name.length + 1);
    const index = args.indexOf(name);
    if (index >= 0 && args[index + 1] && !String(args[index + 1]).startsWith('--')) {
      return args[index + 1];
    }
  }
  return '';
};

const hasFlag = (names = []) => {
  const args = process.argv.slice(2);
  return names.some((name) => args.includes(name) || args.some((arg) => arg.startsWith(`${name}=`)));
};

const parsePositiveInt = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
};

const userNameCache = new Map();

const getDisplayName = async (assignedTo = '') => {
  const normalized = toCleanString(assignedTo);
  if (!normalized) return '';
  if (userNameCache.has(normalized)) return userNameCache.get(normalized);

  let displayName = normalized;
  try {
    const user = await User.findById(normalized).select('name displayName fullName username email').lean();
    displayName = toCleanString(
      user?.name || user?.displayName || user?.fullName || user?.username || user?.email || normalized
    );
  } catch {
    displayName = normalized;
  }

  userNameCache.set(normalized, displayName);
  return displayName;
};

const run = async () => {
  const batchSize = parsePositiveInt(
    readArg(['--batch-size', '--batchSize']) || process.env.CONVERSATION_ASSIGNMENT_BACKFILL_BATCH_SIZE,
    200
  );
  const limit = parsePositiveInt(readArg(['--limit']), 0);
  const companyId = toObjectIdIfValid(readArg(['--company-id', '--companyId']));
  const dryRun =
    hasFlag(['--dry-run', '--dryRun']) ||
    String(process.env.CONVERSATION_ASSIGNMENT_BACKFILL_DRY_RUN || '').trim().toLowerCase() === 'true';

  await connectDB();

  const filters = {
    contactId: { $exists: true, $ne: null }
  };
  if (companyId) filters.companyId = companyId;

  console.log(
    JSON.stringify(
      {
        event: 'conversation_assignment_backfill_start',
        batchSize,
        limit: limit || null,
        dryRun,
        filters: {
          companyId: companyId ? String(companyId) : null
        }
      },
      null,
      2
    )
  );

  const cursor = Conversation.find(filters)
    .select('_id companyId contactId assignedTo assignedToId assignedAgent assignedToName assignedAgentName assigneeName ownerName')
    .sort({ _id: 1 })
    .lean()
    .cursor({ batchSize });

  const updates = [];
  const summaryUpdates = [];
  const stats = {
    scanned: 0,
    matched: 0,
    updated: 0
  };

  const flush = async () => {
    if (!updates.length) return;
    if (!dryRun) {
      await Conversation.bulkWrite(updates.splice(0, updates.length), { ordered: false });
      await ConversationSummary.bulkWrite(summaryUpdates.splice(0, summaryUpdates.length), { ordered: false }).catch(() => null);
    } else {
      updates.length = 0;
      summaryUpdates.length = 0;
    }
  };

  for await (const conversation of cursor) {
    stats.scanned += 1;
    const contactId = toCleanString(conversation?.contactId);
    if (!contactId || !mongoose.Types.ObjectId.isValid(contactId)) {
      if (limit > 0 && stats.scanned >= limit) break;
      continue;
    }

    const contact = await Contact.findOne(
      {
        _id: new mongoose.Types.ObjectId(contactId),
        ...(companyId ? { companyId } : {})
      }
    )
      .select('ownerId assignedTo assignedAgent')
      .lean();

    if (!contact) {
      if (limit > 0 && stats.scanned >= limit) break;
      continue;
    }

    stats.matched += 1;
    const nextAssignedTo = toCleanString(contact?.ownerId || contact?.assignedTo || contact?.assignedAgent || '');
    const currentAssignedTo = toCleanString(conversation?.assignedTo || conversation?.assignedToId || conversation?.assignedAgent || '');

    if (nextAssignedTo !== currentAssignedTo) {
      const displayName = nextAssignedTo ? await getDisplayName(nextAssignedTo) : '';
      const patch = {
        assignedTo: nextAssignedTo || null,
        assignedAgent: nextAssignedTo || null,
        assignedToId: toObjectIdIfValid(nextAssignedTo) || null,
        assignedToName: nextAssignedTo ? displayName || nextAssignedTo : null,
        assignedAgentName: nextAssignedTo ? displayName || nextAssignedTo : null,
        assigneeName: nextAssignedTo ? displayName || nextAssignedTo : null,
        ownerName: nextAssignedTo ? displayName || nextAssignedTo : null,
        updatedAt: new Date()
      };

      updates.push({
        updateOne: {
          filter: { _id: conversation._id, ...(conversation?.companyId ? { companyId: conversation.companyId } : {}) },
          update: { $set: patch }
        }
      });
      summaryUpdates.push({
        updateOne: {
          filter: { _id: conversation._id, ...(conversation?.companyId ? { companyId: conversation.companyId } : {}) },
          update: { $set: patch }
        }
      });
      stats.updated += 1;
    }

    if (updates.length >= batchSize) {
      await flush();
    }

    if (limit > 0 && stats.scanned >= limit) {
      break;
    }
  }

  await flush();

  console.log(
    JSON.stringify(
      {
        event: 'conversation_assignment_backfill_complete',
        scanned: stats.scanned,
        matched: stats.matched,
        updated: stats.updated,
        dryRun
      },
      null,
      2
    )
  );

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Conversation assignment backfill failed:', error);
  try {
    await mongoose.connection.close();
  } catch (_closeError) {
    // ignore close failures
  }
  process.exitCode = 1;
});
