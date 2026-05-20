require('dotenv').config();

const connectDB = require('../config/database');
const { runDataRetentionMaintenance } = require('../services/dataRetentionService');

const run = async () => {
  await connectDB();
  const result = await runDataRetentionMaintenance();
  console.log(JSON.stringify(result, null, 2));
};

run()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Data retention run failed:', error);
    process.exit(1);
  });
