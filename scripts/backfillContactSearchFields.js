require('dotenv').config();

const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Contact = require('../models/Contact');

const readArg = (names = []) => {
  for (const name of names) {
    const prefix = `${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
  }
  return null;
};

const hasFlag = (names = []) => names.some((name) => process.argv.includes(name));

const toCleanString = (value = '') => String(value || '').trim();
const toLowerCleanString = (value = '') => toCleanString(value).toLowerCase();
const toDigitsString = (value = '') => toCleanString(value).replace(/\D/g, '');

const parseObjectId = (value, label) => {
  const normalized = toCleanString(value);
  if (!normalized) return null;
  if (!mongoose.Types.ObjectId.isValid(normalized)) {
    throw new Error(`Invalid ${label}: ${normalized}`);
  }
  return new mongoose.Types.ObjectId(normalized);
};

const run = async () => {
  await connectDB();

  const companyId = parseObjectId(readArg(['--company-id', '--companyId']), 'companyId');
  const userId = parseObjectId(readArg(['--user-id', '--userId']), 'userId');
  const batchSize = Math.max(100, Math.min(Number(readArg(['--batch-size']) || 1000) || 1000, 5000));
  const limit = Math.max(0, Number(readArg(['--limit']) || 0) || 0);
  const dryRun = hasFlag(['--dry-run', '--dryRun']);

  const filter = {};
  if (companyId) filter.companyId = companyId;
  if (userId) filter.userId = userId;

  const cursor = Contact.find(filter)
    .select('_id name phone companyId userId nameLower phoneDigits')
    .sort({ _id: 1 })
    .lean()
    .cursor();

  let scanned = 0;
  let updated = 0;
  let batches = 0;
  let operations = [];

  const flush = async () => {
    if (!operations.length) return;
    if (!dryRun) {
      await Contact.bulkWrite(operations, { ordered: false });
    }
    updated += operations.length;
    batches += 1;
    operations = [];
  };

  for await (const contact of cursor) {
    scanned += 1;
    const nameLower = toLowerCleanString(contact?.name);
    const phoneDigits = toDigitsString(contact?.phone);

    if (
      String(contact?.nameLower || '') === nameLower &&
      String(contact?.phoneDigits || '') === phoneDigits
    ) {
      if (limit && scanned >= limit) break;
      continue;
    }

    operations.push({
      updateOne: {
        filter: { _id: contact._id },
        update: {
          $set: {
            nameLower,
            phoneDigits,
            updatedAt: new Date()
          }
        }
      }
    });

    if (operations.length >= batchSize) {
      await flush();
    }

    if (limit && scanned >= limit) break;
  }

  await flush();

  console.log(
    JSON.stringify(
      {
        dryRun,
        scanned,
        updated,
        batches
      },
      null,
      2
    )
  );

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Contact search backfill failed:', error);
  try {
    await mongoose.connection.close();
  } catch {}
  process.exit(1);
});
