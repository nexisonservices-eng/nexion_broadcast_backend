const claimBroadcastDispatch = async ({
  BroadcastDispatch,
  normalizePhoneNumber,
  broadcastDispatchKey,
  broadcastId,
  userId,
  companyId,
  recipientPhone,
  chunkId = '',
  chunkIndex = 0,
  recipientIndex = 0,
  now = new Date(),
}) => {
  const existing = await BroadcastDispatch.findOne({ broadcastDispatchKey }).lean();
  if (existing?.whatsappMessageId || ['sent', 'suppressed', 'skipped', 'delivered', 'read'].includes(String(existing?.status || '').toLowerCase())) {
    return { claimed: false, alreadyFinal: true, dispatch: existing };
  }
  // A restarted worker cannot know whether Meta accepted an in-flight request.
  // Keep it locked until reconciliation so a replay cannot send it twice.
  if (existing?.status === 'sending') {
    return { claimed: false, locked: true, uncertain: true, dispatch: existing };
  }

  const attemptNumber = Number(existing?.retryCount || 0) + 1;
  const filter = existing
    ? { _id: existing._id, status: existing.status, retryCount: Number(existing.retryCount || 0) }
    : { broadcastDispatchKey };
  try {
    const dispatch = await BroadcastDispatch.findOneAndUpdate(
      filter,
      {
        $setOnInsert: {
          broadcastDispatchKey,
          broadcastId,
          userId,
          companyId: companyId || null,
          recipientPhone,
          recipientPhoneNormalized: normalizePhoneNumber(recipientPhone),
          recipientPhoneDigits: normalizePhoneNumber(recipientPhone),
          chunkId,
          chunkIndex,
          recipientIndex,
          createdAt: now,
        },
        $set: {
          status: 'sending',
          claimedAt: now,
          lastAttemptAt: now,
          errorMessage: '',
          updatedAt: now,
        },
        $inc: { retryCount: 1 },
        $push: {
          attemptHistory: {
            attemptNumber,
            status: 'sending',
            startedAt: now,
            completedAt: null,
            whatsappMessageId: '',
            errorCode: '',
            errorReason: '',
          },
        },
      },
      { new: true, upsert: !existing },
    ).lean();
    if (!dispatch) {
      const raced = await BroadcastDispatch.findOne({ broadcastDispatchKey }).lean();
      return { claimed: false, locked: true, dispatch: raced };
    }
    return {
      claimed: true,
      dispatch,
      previousStatus: existing?.status || 'pending',
      retryCount: attemptNumber,
    };
  } catch (error) {
    if (error?.code !== 11000) throw error;
    const raced = await BroadcastDispatch.findOne({ broadcastDispatchKey }).lean();
    return { claimed: false, locked: true, dispatch: raced };
  }
};

module.exports = { claimBroadcastDispatch };
