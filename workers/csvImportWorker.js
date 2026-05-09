require('dotenv').config();

const { Worker } = require('bullmq');
const { connection, queueName } = require('../queues/csvImportQueue');
const { processCsvImport, failCsvImport } = require('../services/csvImportProcessor');

const csvImportWorker = new Worker(queueName, processCsvImport, {
  connection,
  concurrency: Math.max(1, Number(process.env.CSV_IMPORT_WORKER_CONCURRENCY || 2))
});

csvImportWorker.on('completed', (job) => {
  console.log(`CSV import job completed: ${job?.id || 'unknown'}`);
});

csvImportWorker.on('failed', async (job, error) => {
  const importJobId = String(job?.data?.importJobId || '').trim();
  const userId = String(job?.data?.userId || '').trim();
  const filePath = String(job?.data?.filePath || '').trim();
  await failCsvImport({
    importJobId,
    userId,
    filePath,
    error
  });
  console.error('CSV import worker failed:', String(error?.message || error || 'unknown error'));
});

const shutdown = async () => {
  await csvImportWorker.close();
  await connection.quit();
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

module.exports = csvImportWorker;
