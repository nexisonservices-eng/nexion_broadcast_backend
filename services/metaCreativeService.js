const FormData = require('form-data');

const sanitizeWhatsappNumber = (value) =>
  String(value || '')
    .replace(/[^\d]/g, '')
    .trim();

const buildCreativeDestination = ({ whatsappNumber, pageId }) => {
  const sanitizedWhatsapp = sanitizeWhatsappNumber(whatsappNumber);
  if (sanitizedWhatsapp) {
    return {
      whatsappNumber: sanitizedWhatsapp,
      destinationUrl: `https://wa.me/${sanitizedWhatsapp}`
    };
  }

  if (pageId) {
    return {
      whatsappNumber: '',
      destinationUrl: `https://www.facebook.com/${pageId}`
    };
  }

  return {
    whatsappNumber: '',
    destinationUrl: 'https://www.facebook.com/'
  };
};

const normalizeMetaApiError = (error) => {
  const metaError = error?.response?.data?.error || error?.response?.data || {};
  const code = Number(metaError?.code);
  const subcode = Number(metaError?.error_subcode);

  return {
    message: String(metaError?.message || error?.message || 'Meta API request failed'),
    type: String(metaError?.type || ''),
    code: Number.isFinite(code) ? code : null,
    error_subcode: Number.isFinite(subcode) ? subcode : null,
    fbtrace_id: String(metaError?.fbtrace_id || '')
  };
};

const summarizePage = (page) => ({
  id: String(page?.id || '').trim(),
  name: String(page?.name || '').trim()
});

const getAccessiblePages = async ({ accessToken, graphRequest }) => {
  const response = await graphRequest({
    path: 'me/accounts',
    params: { fields: 'id,name,access_token' },
    accessToken
  });

  return Array.isArray(response?.data) ? response.data : [];
};

const resolveCreativePageContext = async ({
  requestedPageId,
  accessToken,
  graphRequest,
  env,
  buildStageErrorWithDetails
}) => {
  const normalizedRequestedPageId = String(requestedPageId || '').trim();
  const accessiblePages = await getAccessiblePages({ accessToken, graphRequest });
  const matchedPage =
    accessiblePages.find((page) => String(page?.id || '') === normalizedRequestedPageId) ||
    accessiblePages[0];

  if (!accessiblePages.length) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      normalizedRequestedPageId
        ? 'The current Meta login does not have access to the selected Facebook Page profile.'
        : 'No accessible Facebook pages were found for this Meta token.',
      {
        requestedPageId: normalizedRequestedPageId || '',
        accessiblePages: [],
        action:
          'Reconnect Facebook with a user who has full Page access, then select the correct Facebook Page before creating ads.'
      },
      400
    );
  }

  if (!matchedPage?.id) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      'The selected Facebook page is not available for this Meta token.',
      {
        requestedPageId: normalizedRequestedPageId,
        accessiblePages: accessiblePages.map(summarizePage)
      },
      400
    );
  }

  return {
    pageId: String(matchedPage.id),
    pageName: String(matchedPage.name || ''),
    pageAccessToken: String(matchedPage.access_token || '').trim(),
    requestedPageId: normalizedRequestedPageId,
    accessiblePages: accessiblePages.map(summarizePage)
  };
};

const uploadCreativeAsset = async ({
  fileBuffer,
  fileName,
  mediaUrl,
  mediaType,
  userId,
  adAccountId,
  shouldUseMockMode,
  getAccessContextForUser,
  getEnvConfig,
  graphRequest,
  buildAdAccountPath,
  buildStageErrorWithDetails,
  extractApiErrorMessage
}) => {
  const normalizedMediaType = String(mediaType || '').trim().toLowerCase() === 'video' ? 'video' : 'image';

  if (!fileBuffer && !mediaUrl) {
    return { mediaType: normalizedMediaType, mediaHash: '', mediaUrl: '', videoId: '' };
  }

  if (shouldUseMockMode()) {
    const now = Date.now();
    return {
      mediaType: normalizedMediaType,
      mediaHash: normalizedMediaType === 'image' ? `mock_${now}` : '',
      mediaUrl: mediaUrl || `mock://${fileName || 'upload'}`,
      videoId: normalizedMediaType === 'video' ? `mock_video_${now}` : ''
    };
  }

  const accessContext = await getAccessContextForUser(userId);
  const adAccountCandidates = [
    adAccountId,
    accessContext.connection?.selectedAdAccountId
  ].filter(Boolean);
  const tokenCandidates = [...new Set([accessContext.accessToken].filter(Boolean))];

  const tryUpload = async ({ effectiveAdAccountId, accessToken }) => {
    if (normalizedMediaType === 'video') {
      if (mediaUrl) {
        const response = await graphRequest({
          method: 'POST',
          path: buildAdAccountPath(effectiveAdAccountId, 'advideos'),
          data: { file_url: mediaUrl },
          accessToken
        });
        const videoId = String(response?.id || response?.video_id || '').trim();
        return {
          mediaType: 'video',
          mediaHash: '',
          mediaUrl,
          videoId
        };
      }

      const form = new FormData();
      form.append('source', fileBuffer, { filename: fileName || `creative-${Date.now()}.mp4` });

      const response = await graphRequest({
        method: 'POST',
        path: buildAdAccountPath(effectiveAdAccountId, 'advideos'),
        data: form,
        headers: form.getHeaders(),
        accessToken
      });
      const videoId = String(response?.id || response?.video_id || '').trim();

      return {
        mediaType: 'video',
        mediaHash: '',
        mediaUrl: '',
        videoId
      };
    }

    if (mediaUrl) {
      const response = await graphRequest({
        method: 'POST',
        path: buildAdAccountPath(effectiveAdAccountId, 'adimages'),
        data: { url: mediaUrl },
        accessToken
      });
      const image = response?.images ? Object.values(response.images)[0] : null;
      return {
        mediaType: 'image',
        mediaHash: image?.hash || '',
        mediaUrl,
        videoId: ''
      };
    }

    const form = new FormData();
    form.append('filename', fileBuffer, { filename: fileName || `creative-${Date.now()}.jpg` });

    const response = await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(effectiveAdAccountId, 'adimages'),
      data: form,
      headers: form.getHeaders(),
      accessToken
    });
    const image = response?.images ? Object.values(response.images)[0] : null;

    return {
      mediaType: 'image',
      mediaHash: image?.hash || '',
      mediaUrl: '',
      videoId: ''
    };
  };

  let lastError = null;
  for (const effectiveAdAccountId of adAccountCandidates) {
    for (const accessToken of tokenCandidates) {
      try {
        return await tryUpload({ effectiveAdAccountId, accessToken });
      } catch (error) {
        if (mediaUrl) {
          const message = extractApiErrorMessage(error);
          if (
            /capability to make this api call/i.test(message) ||
            /application does not have the capability/i.test(message)
          ) {
            return {
              mediaType: normalizedMediaType,
              mediaHash: '',
              mediaUrl,
              videoId: ''
            };
          }
        }
        lastError = {
          error,
          effectiveAdAccountId,
          source: accessContext.source
        };
      }
    }
  }

  if (lastError) {
    throw buildStageErrorWithDetails(
      'Creative upload',
      extractApiErrorMessage(lastError.error),
      {
        mediaType: normalizedMediaType,
        mediaUrl: '',
        fileName: fileName || '',
        adAccountId: lastError.effectiveAdAccountId,
        tokenSource: lastError.source
      },
      lastError.error?.response?.status || 400
    );
  }

  return {
    mediaType: normalizedMediaType,
    mediaHash: '',
    mediaUrl: '',
    videoId: ''
  };
};

const createCreative = async ({
  campaignName,
  creative,
  creativeUpload,
  configuredPageId,
  pageAccessToken,
  instagramActorId,
  destinationUrl,
  sanitizedWhatsappNumber,
  adAccountId,
  accessToken,
  graphRequest,
  buildAdAccountPath,
  buildStageErrorWithDetails,
  extractApiErrorMessage,
  creativePageContext,
  logMetaRequest
}) => {
  const requestedCtaType = String(creative?.callToAction || 'WHATSAPP_MESSAGE').trim();
  const effectiveCtaType =
    requestedCtaType === 'WHATSAPP_MESSAGE' && !sanitizedWhatsappNumber
      ? 'LEARN_MORE'
      : requestedCtaType;

  const callToActionValue =
    effectiveCtaType === 'WHATSAPP_MESSAGE'
      ? {
          app_destination: 'WHATSAPP',
          link: destinationUrl,
          page_welcome_message:
            creative?.primaryText || campaignName || 'Start a conversation on WhatsApp'
        }
      : {
          link: destinationUrl
        };

  const objectStorySpec = {
    page_id: configuredPageId
  };

  const normalizedMediaType =
    String(creative?.mediaType || '').trim().toLowerCase() === 'video' || creativeUpload?.videoId
      ? 'video'
      : 'image';

  if (normalizedMediaType === 'video') {
    objectStorySpec.video_data = {
      video_id: creativeUpload?.videoId,
      message: creative?.primaryText || campaignName || 'Learn more',
      title: creative?.headline || campaignName,
      call_to_action: {
        type: effectiveCtaType,
        value: callToActionValue
      }
    };
    if (creative?.description) {
      objectStorySpec.video_data.description = creative.description;
    }
  } else {
    objectStorySpec.link_data = {
      link: destinationUrl,
      message: creative?.primaryText || campaignName || 'Learn more',
      name: creative?.headline || campaignName,
      description: creative?.description || '',
      call_to_action: {
        type: effectiveCtaType,
        value: callToActionValue
      }
    };

    if (creativeUpload?.mediaHash) {
      objectStorySpec.link_data.image_hash = creativeUpload.mediaHash;
    } else if (creativeUpload?.mediaUrl) {
      objectStorySpec.link_data.picture = creativeUpload.mediaUrl;
    }
  }
  if (instagramActorId) {
    objectStorySpec.instagram_actor_id = instagramActorId;
  }

  const isVideoProcessingError = (error) =>
    /video/i.test(extractApiErrorMessage(error)) &&
    /(processing|not ready|transcod)/i.test(extractApiErrorMessage(error));
  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const maxAttempts = normalizedMediaType === 'video' && creativeUpload?.videoId ? 4 : 1;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const endpoint = buildAdAccountPath(adAccountId, 'adcreatives');
    const requestPayload = {
      name: `${campaignName} - Creative`,
      object_story_spec: objectStorySpec
    };

    try {
      const response = await graphRequest({
        method: 'POST',
        path: endpoint,
        data: requestPayload,
        accessToken: String(pageAccessToken || accessToken || '').trim(),
        returnResponse: true
      });
      logMetaRequest?.({
        stage: 'creative',
        endpoint,
        adAccountId,
        payload: requestPayload,
        response
      });
      return response.data;
    } catch (error) {
      lastError = error;
      logMetaRequest?.({
        stage: 'creative',
        endpoint,
        adAccountId,
        payload: requestPayload,
        error
      });
      if (attempt >= maxAttempts || !isVideoProcessingError(error)) {
        break;
      }
      await wait(3000);
    }
  }

  throw buildStageErrorWithDetails(
    'Creative creation',
    extractApiErrorMessage(lastError),
    {
      metaError: lastError?.response?.data || null,
      requestedPageId: creativePageContext.requestedPageId,
      resolvedPageId: creativePageContext.pageId,
      resolvedPageName: creativePageContext.pageName,
      resolvedPageAccessToken: Boolean(creativePageContext.pageAccessToken),
      accessiblePages: creativePageContext.accessiblePages
    },
    lastError?.response?.status || 400
  );
};

module.exports = {
  sanitizeWhatsappNumber,
  buildCreativeDestination,
  getAccessiblePages,
  resolveCreativePageContext,
  uploadCreativeAsset,
  createCreative
};
