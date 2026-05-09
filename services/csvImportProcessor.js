const fs = require('fs');
const csvParser = require('csv-parser');
const CsvImportJob = require('../models/CsvImportJob');
const {
  bulkUpsertImportedContacts,
  countCsvDataRows,
  removeFileQuietly,
  getEtaMs
} = require('./csvImportService');
const { publishBroadcastEvent } = require('../realtime/broadcastEventBus');

const batchSize = Math.max(50, Number(process.env.CSV_IMPORT_BATCH_SIZE || 500));
const progressFlushMs = Math.max(250, Number(process.env.CSV_IMPORT_PROGRESS_FLUSH_MS || 1000));

const emitImportEvent = async (userId, payload) => {
  if (!userId || !payload) return;
  await publishBroadcastEvent({
    userId,
    payload
  });
};

const updateImportJob = async (importJobId, patch = {}) => {
  if (!importJobId) return null;
  return CsvImportJob.findByIdAndUpdate(
    importJobId,
    {
      $set: {
        ...patch,
        updatedAt: new Date()
      }
    },
    { new: true }
  );
};

const buildProgressPayload = (job, extra = {}) => {
  const totalRows = Math.max(0, Number(job?.totalRows || 0));
  const processedRows = Math.max(0, Number(job?.processedRows || 0));
  const percentComplete =
    totalRows > 0 ? Math.min(100, Math.round((processedRows / totalRows) * 100)) : 0;
  const etaMs = getEtaMs(job?.startedAt ? new Date(job.startedAt).getTime() : null, processedRows, totalRows);

  return {
    type: 'csv_import_progress',
    importJobId: String(job?._id || ''),
    status: String(job?.status || 'processing'),
    totalRows,
    processedRows,
    successCount: Math.max(0, Number(job?.successCount || 0)),
    failedCount: Math.max(0, Number(job?.failedCount || 0)),
    duplicateCount: Math.max(0, Number(job?.duplicateCount || 0)),
    skippedCount: Math.max(0, Number(job?.skippedCount || 0)),
    percentComplete,
    etaMs,
    currentStage: String(job?.currentStage || 'processing'),
    ...extra
  };
};

const processCsvImport = async (job) => {
  const importJobId = String(job?.data?.importJobId || '').trim();
  const userId = String(job?.data?.userId || '').trim();
  const companyId = String(job?.data?.companyId || '').trim() || null;
  const filePath = String(job?.data?.filePath || '').trim();

  if (!importJobId || !userId || !filePath) {
    throw new Error('Missing csv import job inputs');
  }

  let importJob = await CsvImportJob.findById(importJobId);
  if (!importJob) {
    throw new Error(`CSV import job ${importJobId} not found`);
  }

  const totalRows = await countCsvDataRows(filePath);
  importJob = await updateImportJob(importJobId, {
    status: 'processing',
    startedAt: importJob.startedAt || new Date(),
    currentStage: 'counting_rows',
    totalRows,
    processedRows: 0,
    successCount: 0,
    failedCount: 0,
    duplicateCount: 0,
    skippedCount: 0,
    percentComplete: 0,
    errorMessage: '',
    queueJobId: String(job.id || importJob.queueJobId || '')
  });

  await emitImportEvent(userId, buildProgressPayload(importJob, {
    type: 'csv_import_progress',
    stage: 'counting_rows'
  }));

  const batch = [];
  let processedRows = 0;
  let successCount = 0;
  let failedCount = 0;
  let duplicateCount = 0;
  let skippedCount = 0;
  let lastFlushAt = Date.now();

  const flushBatch = async () => {
    if (!batch.length) return;

    const batchRows = batch.splice(0, batch.length);
    const result = await bulkUpsertImportedContacts(batchRows, {
      userId,
      companyId,
      consentReferenceId: `csv-import-${importJobId}`,
      importJobId
    });

    successCount += Number(result.success || 0);
    skippedCount += Number(result.skipped || 0);
    duplicateCount += 0;
    processedRows += batchRows.length;

    const now = new Date();
    importJob = await updateImportJob(importJobId, {
      currentStage: 'writing',
      processedRows,
      successCount,
      failedCount,
      duplicateCount,
      skippedCount,
      percentComplete: totalRows > 0 ? Math.min(100, Math.round((processedRows / totalRows) * 100)) : 0,
      etaMs: getEtaMs(importJob.startedAt ? new Date(importJob.startedAt).getTime() : null, processedRows, totalRows),
      lastProgressAt: now
    });

    await emitImportEvent(userId, buildProgressPayload(importJob, {
      stage: 'writing',
      summary: {
        processedRows,
        successCount,
        failedCount,
        skippedCount
      }
    }));
  };

  await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    const parser = csvParser();

    const cleanupAndReject = (error) => {
      stream.destroy();
      parser.destroy?.();
      reject(error);
    };

    stream
      .on('error', cleanupAndReject)
      .pipe(parser)
      .on('data', async (row) => {
        stream.pause();
        try {
          batch.push({
            data: row,
            rowNumber: processedRows + batch.length + 2
          });

          if (batch.length >= batchSize) {
            await flushBatch();
          }

          const now = Date.now();
          if (now - lastFlushAt >= progressFlushMs) {
            lastFlushAt = now;
            importJob = await updateImportJob(importJobId, {
              currentStage: 'processing',
              processedRows,
              successCount,
              failedCount,
              duplicateCount,
              skippedCount,
              percentComplete: totalRows > 0 ? Math.min(100, Math.round((processedRows / totalRows) * 100)) : 0,
              etaMs: getEtaMs(importJob.startedAt ? new Date(importJob.startedAt).getTime() : null, processedRows, totalRows),
              lastProgressAt: new Date()
            });
            await emitImportEvent(userId, buildProgressPayload(importJob, { stage: 'processing' }));
          }

          stream.resume();
        } catch (error) {
          cleanupAndReject(error);
        }
      })
      .on('error', cleanupAndReject)
      .on('end', async () => {
        try {
          await flushBatch();
          importJob = await updateImportJob(importJobId, {
            status: 'completed',
            currentStage: 'completed',
            processedRows,
            successCount,
            failedCount,
            duplicateCount,
            skippedCount,
            percentComplete: 100,
            etaMs: 0,
            completedAt: new Date(),
            lastProgressAt: new Date()
          });

          await emitImportEvent(userId, {
            type: 'csv_import_completed',
            importJobId,
            status: 'completed',
            totalRows,
            processedRows,
            successCount,
            failedCount,
            duplicateCount,
            skippedCount,
            percentComplete: 100,
            etaMs: 0
          });

          await removeFileQuietly(filePath);
          resolve(importJob);
        } catch (error) {
          reject(error);
        }
      });
  });

  return importJob;
};

const failCsvImport = async ({ importJobId, userId, filePath, error }) => {
  const message = String(error?.message || 'CSV import failed').trim();
  if (importJobId) {
    await updateImportJob(importJobId, {
      status: 'failed',
      currentStage: 'failed',
      errorMessage: message,
      completedAt: new Date(),
      lastProgressAt: new Date()
    });
  }

  if (userId) {
    await emitImportEvent(userId, {
      type: 'csv_import_failed',
      importJobId,
      status: 'failed',
      error: message
    });
  }

  await removeFileQuietly(filePath);
};

module.exports = {
  processCsvImport,
  failCsvImport,
  buildProgressPayload
};
