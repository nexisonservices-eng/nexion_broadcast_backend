// This boundary never retries a creation POST. Unknown outcomes are read-only
// reconciliations, including retries restored from a persisted partial stack.
const cleanId = (value) => typeof value === 'string' ? value.trim() : '';
const extractAdId = (response) => {
  for (const body of [response, response?.data, response?.result, response?.data?.data, response?.data?.result]) {
    const id = cleanId(body?.id) || cleanId(body?.ad_id);
    if (id) return id;
  }
  return '';
};

const diagnostic = (response, error) => ({
  httpStatus: response?.status ?? error?.response?.status ?? null,
  responseType: Array.isArray(response?.data) ? 'array' : typeof response?.data,
  // Describe only known structural fields; arbitrary keys can contain user data.
  shape: ['id', 'ad_id', 'data', 'result', 'error'].filter((key) =>
    Object.prototype.hasOwnProperty.call(response?.data || {}, key)),
  metaErrorCode: error?.response?.data?.error?.code ?? error?.details?.code ?? response?.data?.error?.code ?? null,
  metaErrorSubcode: error?.response?.data?.error?.error_subcode ?? error?.details?.errorSubcode ?? null
});

const createAdCreation = ({ graphRequest, buildAdAccountPath, buildStageError, buildStageErrorWithDetails,
  wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), logger = console }) => {
  const resolve = async ({ createdAd, createdAdSetId, createdCreativeId, effectiveAdAccountId, resolvedAccessToken }) => {
    const directId = extractAdId(createdAd);
    if (directId) return directId;
    const adSetId = cleanId(createdAdSetId);
    const creativeId = cleanId(createdCreativeId);
    if (!adSetId || !creativeId) return '';

    for (const waitMs of [0, 800, 1500, 2500, 4000]) {
      if (waitMs) await wait(waitMs);
      const matches = new Set();
      for (const path of [`${adSetId}/ads`, buildAdAccountPath(effectiveAdAccountId, 'ads')]) {
        let after;
        const seen = new Set();
        try {
          do {
            const response = await graphRequest({ path, params: {
              fields: 'id,adset_id,creative{id}', limit: 100, ...(after ? { after } : {})
            }, accessToken: resolvedAccessToken, bypassCache: true, returnResponse: true, timeout: 30000 });
            const body = response?.data;
            if (body?.error) throw Object.assign(new Error('Meta lookup failed'), { response });
            for (const ad of Array.isArray(body?.data) ? body.data : []) {
              if (cleanId(ad?.creative?.id) === creativeId &&
                  cleanId(ad?.adset_id || ad?.adset?.id) === adSetId && cleanId(ad?.id)) matches.add(cleanId(ad.id));
            }
            after = body?.paging?.next ? cleanId(body?.paging?.cursors?.after) : '';
            if (body?.paging?.next && !after) throw new Error('Missing pagination cursor');
            // Use a cursor on our original endpoint; never follow token-bearing URLs.
            if (after && seen.has(after)) throw new Error('Repeated pagination cursor');
            if (after) seen.add(after);
            logger.info('[Meta ad reconciliation]', { endpoint: path, ...diagnostic(response), matches: matches.size });
          } while (after);
          if (matches.size === 1) return [...matches][0];
          if (matches.size > 1) return '';
        } catch (error) {
          logger.warn('[Meta ad reconciliation]', { endpoint: path, ...diagnostic(error?.response, error) });
        }
      }
    }
    return '';
  };

  const create = async ({ partialData, effectiveAdAccountId, resolvedAccessToken, payload,
    reconciliationOnly = false, onPending }) => {
    const endpoint = buildAdAccountPath(effectiveAdAccountId, 'ads');
    let response;
    if (!reconciliationOnly) {
      // The caller persists this marker BEFORE sending any potentially successful POST.
      try {
        await onPending?.({ ...partialData, adAccountId: effectiveAdAccountId, reconciliationRequired: true });
      } catch (error) {
        error.partialData = { ...partialData, adAccountId: effectiveAdAccountId };
        throw error;
      }
      try {
        response = await graphRequest({ method: 'POST', path: endpoint, data: payload,
          accessToken: resolvedAccessToken, returnResponse: true, timeout: 60000 });
        if (response?.data?.error) {
          const error = new Error('Meta rejected ad creation');
          error.response = response;
          throw error;
        }
        logger.info('[Meta ad creation]', { endpoint, ...diagnostic(response) });
      } catch (error) {
        logger.warn('[Meta ad creation]', { endpoint, ...diagnostic(error?.response, error) });
        // A Graph rejection is confirmed. Network failures and 5xx responses can
        // occur after creation and must be reconciled without sending another POST.
        if (error.metaRateLimited || (error?.response?.data?.error && error.response.status < 500)) {
          const wrapped = buildStageError('Ad creation', error);
          wrapped.status = error.status || wrapped.status;
          wrapped.details = error.details || wrapped.details;
          wrapped.partialData = { ...partialData, adAccountId: effectiveAdAccountId };
          throw wrapped;
        }
      }
    }
    const adId = await resolve({ createdAd: response, createdAdSetId: partialData.metaAdSetId,
      createdCreativeId: partialData.metaCreativeId, effectiveAdAccountId, resolvedAccessToken });
    if (adId) return adId;
    const preserved = { ...partialData, adAccountId: effectiveAdAccountId, reconciliationRequired: true };
    const error = buildStageErrorWithDetails('Ad creation',
      'Meta ad creation outcome is unconfirmed. Reconciliation is required before creating another ad.',
      { code: 'META_AD_RECONCILIATION_REQUIRED', partialData: preserved }, 409);
    error.code = 'META_AD_RECONCILIATION_REQUIRED';
    error.reconciliationRequired = true;
    error.partialData = preserved;
    throw error;
  };
  return { resolve, create };
};

module.exports = { createAdCreation, extractAdId, diagnostic };
