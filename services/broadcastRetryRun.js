const TERMINAL_BROADCAST_STATUSES = [
  'completed',
  'completed_with_errors',
  'failed',
];

const isSenderSpamRateRestriction = (errorCode) =>
  String(errorCode || '').trim() === '131048';

const isRetryableBroadcastFailure = ({ errorCode = '', reason = '' } = {}) => {
  if (isSenderSpamRateRestriction(errorCode)) return false;
  const code = String(errorCode || '').trim();
  const message = String(reason || '');
  return ['131049', '131056', '131016', '63018'].includes(code) ||
    /timeout|temporar|rate limit|try again/i.test(message);
};

const getRetryRun = (broadcast, runId) =>
  (Array.isArray(broadcast?.retryRuns) ? broadcast.retryRuns : []).find(
    (run) => String(run?.runId || '') === String(runId || ''),
  ) || null;

const getRetryRunByIdempotencyKey = (broadcast, idempotencyKey) => {
  if (!idempotencyKey) return null;
  return (
    (Array.isArray(broadcast?.retryRuns) ? broadcast.retryRuns : []).find(
      (run) => String(run?.idempotencyKey || '') === idempotencyKey,
    ) || null
  );
};

const acquireRetryRun = async ({
  Broadcast,
  broadcastId,
  runId,
  idempotencyKey = '',
  recipientPhones = [],
  now = new Date(),
}) => {
  const current = await Broadcast.findById(broadcastId).lean();
  if (!current) return { acquired: false, error: 'Broadcast not found' };

  const prior = getRetryRunByIdempotencyKey(current, idempotencyKey);
  if (prior) {
    return {
      acquired: false,
      duplicate: true,
      broadcast: current,
      run: prior,
    };
  }

  const activeRun = String(current.activeRetryRunId || '');
  if (activeRun) {
    return {
      acquired: false,
      duplicate: true,
      broadcast: current,
      run: getRetryRun(current, activeRun),
    };
  }

  if (!TERMINAL_BROADCAST_STATUSES.includes(String(current.status || '').toLowerCase())) {
    return {
      acquired: false,
      error: 'Retry is allowed only after a completed, partially completed, or failed broadcast',
    };
  }

  const claimed = await Broadcast.findOneAndUpdate(
    {
      _id: broadcastId,
      status: { $in: TERMINAL_BROADCAST_STATUSES },
      $or: [
        { activeRetryRunId: { $exists: false } },
        { activeRetryRunId: '' },
        { activeRetryRunId: null },
      ],
    },
    {
      $set: {
        activeRetryRunId: runId,
        status: 'queued',
        queueLastError: '',
        updatedAt: now,
      },
      $push: {
        retryRuns: {
          runId,
          idempotencyKey,
          status: 'queued',
          recipientPhones,
          recipientCount: recipientPhones.length,
          createdAt: now,
        },
      },
    },
    { new: true },
  ).lean();

  if (claimed) {
    return {
      acquired: true,
      broadcast: claimed,
      run: getRetryRun(claimed, runId),
    };
  }

  const raced = await Broadcast.findById(broadcastId).lean();
  const racedRun = raced?.activeRetryRunId
    ? getRetryRun(raced, raced.activeRetryRunId)
    : getRetryRunByIdempotencyKey(raced, idempotencyKey);
  return {
    acquired: false,
    duplicate: Boolean(racedRun),
    broadcast: raced,
    run: racedRun,
    error: racedRun ? '' : 'Broadcast retry could not be claimed; refresh and retry.',
  };
};

const finishRetryRun = async ({
  Broadcast,
  broadcastId,
  runId,
  status = 'completed',
  error = '',
  now = new Date(),
}) => {
  const set = {
    activeRetryRunId: '',
    updatedAt: now,
    'retryRuns.$[run].status': status,
    'retryRuns.$[run].completedAt': now,
    'retryRuns.$[run].error': String(error || ''),
  };
  if (status === 'failed') set.status = 'completed_with_errors';

  return Broadcast.updateOne(
    { _id: broadcastId, activeRetryRunId: runId },
    {
      $set: set,
    },
    { arrayFilters: [{ 'run.runId': runId }] },
  );
};

module.exports = {
  TERMINAL_BROADCAST_STATUSES,
  acquireRetryRun,
  finishRetryRun,
  getRetryRun,
  isSenderSpamRateRestriction,
  isRetryableBroadcastFailure,
};
