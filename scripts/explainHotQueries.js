require('dotenv').config();
const mongoose = require('mongoose');
const connectDB = require('../config/database');
const Contact = require('../models/Contact');
const Conversation = require('../models/Conversation');
const Message = require('../models/Message');
const Template = require('../models/Template');

const toJson = (value) => JSON.stringify(value, null, 2);

const summarizePlan = (explain = {}) => {
  const winningPlan = explain?.queryPlanner?.winningPlan || {};
  const executionStats = explain?.executionStats || {};
  return {
    stage: winningPlan.stage || winningPlan?.inputStage?.stage || winningPlan?.queryPlan?.stage || 'unknown',
    indexName:
      winningPlan.indexName ||
      winningPlan?.inputStage?.indexName ||
      winningPlan?.queryPlan?.indexName ||
      null,
    nReturned: executionStats.nReturned ?? null,
    totalDocsExamined: executionStats.totalDocsExamined ?? null,
    totalKeysExamined: executionStats.totalKeysExamined ?? null,
    executionTimeMillis: executionStats.executionTimeMillis ?? null
  };
};

const printPlan = async (label, cursor) => {
  const explain = await cursor.explain('executionStats');
  const summary = summarizePlan(explain);
  console.log(`\n=== ${label} ===`);
  console.log(toJson(summary));
};

const run = async () => {
  await connectDB();

  const tenantFilter = {
    companyId: new mongoose.Types.ObjectId('000000000000000000000001'),
    userId: new mongoose.Types.ObjectId('000000000000000000000002')
  };

  await printPlan(
    'Contacts inbox page',
    Contact.find(tenantFilter)
      .select('_id name phone lastContact createdAt')
      .sort({ lastContact: -1, createdAt: -1, _id: -1 })
      .limit(50)
  );

  await printPlan(
    'Conversations inbox page',
    Conversation.find({ ...tenantFilter, status: 'active' })
      .select('_id contactPhone lastMessageTime unreadCount')
      .sort({ lastMessageTime: -1, _id: -1 })
      .limit(50)
  );

  await printPlan(
    'Message thread page',
    Message.find({
      ...tenantFilter,
      conversationId: new mongoose.Types.ObjectId('000000000000000000000003')
    })
      .select('_id conversationId timestamp sender status')
      .sort({ timestamp: -1, _id: -1 })
      .limit(50)
  );

  await printPlan(
    'Template lookup',
    Template.findOne({
      ...tenantFilter,
      name: 'example_template'
    }).select('_id name category status isActive createdAt')
  );

  await mongoose.connection.close();
};

run().catch(async (error) => {
  console.error('Explain script failed:', error);
  try {
    await mongoose.connection.close();
  } catch {
    // no-op
  }
  process.exit(1);
});
