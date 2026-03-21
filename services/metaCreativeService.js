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

const getAccessiblePages = async ({ accessToken, graphRequest }) => {
  const response = await graphRequest({
    path: 'me/accounts',
    params: { fields: 'id,name' },
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
          'Reconnect Facebook with a user who has Page access, grant pages_show_list/pages_read_engagement/pages_manage_metadata, and then select the correct Facebook Page before creating ads.'
      },
      400
    );
  }

  const matchedPage =
    accessiblePages.find((page) => String(page?.id || '') === normalizedRequestedPageId) ||
    accessiblePages[0];

  if (!matchedPage?.id) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      'The selected Facebook page is not available for this Meta token.',
      {
        requestedPageId: normalizedRequestedPageId,
        accessiblePages
      },
      400
    );
  }

  return {
    pageId: String(matchedPage.id),
    pageName: String(matchedPage.name || ''),
    requestedPageId: normalizedRequestedPageId,
    accessiblePages
  };
};

const uploadCreativeAsset = async ({
  fileBuffer,
  fileName,
  mediaUrl,
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
  if (!fileBuffer && !mediaUrl) {
    return { mediaHash: '', mediaUrl: '' };
  }

  if (shouldUseMockMode()) {
    return {
      mediaHash: `mock_${Date.now()}`,
      mediaUrl: mediaUrl || `mock://${fileName || 'upload'}`
    };
  }

  const accessContext = await getAccessContextForUser(userId);
  const adAccountCandidates = [
    adAccountId,
    accessContext.connection?.selectedAdAccountId
  ].filter(Boolean);
  const tokenCandidates = [...new Set([accessContext.accessToken].filter(Boolean))];

  const tryUpload = async ({ effectiveAdAccountId, accessToken }) => {
    if (mediaUrl) {
      const response = await graphRequest({
        method: 'POST',
        path: buildAdAccountPath(effectiveAdAccountId, 'adimages'),
        data: { url: mediaUrl },
        accessToken
      });
      const image = response?.images ? Object.values(response.images)[0] : null;
      return {
        mediaHash: image?.hash || '',
        mediaUrl
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
      mediaHash: image?.hash || '',
      mediaUrl: ''
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
              mediaHash: '',
              mediaUrl
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
        mediaUrl: '',
        fileName: fileName || '',
        adAccountId: lastError.effectiveAdAccountId,
        tokenSource: lastError.source
      },
      lastError.error?.response?.status || 400
    );
  }

  return {
    mediaHash: '',
    mediaUrl: ''
  };
};

const createCreative = async ({
  campaignName,
  creative,
  creativeUpload,
  configuredPageId,
  instagramActorId,
  destinationUrl,
  sanitizedWhatsappNumber,
  adAccountId,
  accessToken,
  graphRequest,
  buildAdAccountPath,
  buildStageErrorWithDetails,
  extractApiErrorMessage,
  creativePageContext
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
    page_id: configuredPageId,
    link_data: {
      link: destinationUrl,
      message: creative?.primaryText || campaignName || 'Learn more',
      name: creative?.headline || campaignName,
      description: creative?.description || '',
      call_to_action: {
        type: effectiveCtaType,
        value: callToActionValue
      }
    }
  };

  if (creativeUpload?.mediaHash) {
    objectStorySpec.link_data.image_hash = creativeUpload.mediaHash;
  } else if (creativeUpload?.mediaUrl) {
    objectStorySpec.link_data.picture = creativeUpload.mediaUrl;
  }
  if (instagramActorId) {
    objectStorySpec.instagram_actor_id = instagramActorId;
  }

  try {
    return await graphRequest({
      method: 'POST',
      path: buildAdAccountPath(adAccountId, 'adcreatives'),
      data: {
        name: `${campaignName} - Creative`,
        object_story_spec: objectStorySpec
      },
      accessToken
    });
  } catch (error) {
    throw buildStageErrorWithDetails(
      'Creative creation',
      extractApiErrorMessage(error),
      {
        metaError: error?.response?.data || null,
        requestedPageId: creativePageContext.requestedPageId,
        resolvedPageId: creativePageContext.pageId,
        resolvedPageName: creativePageContext.pageName,
        accessiblePages: creativePageContext.accessiblePages
      },
      error?.response?.status || 400
    );
  }
};

module.exports = {
  sanitizeWhatsappNumber,
  buildCreativeDestination,
  getAccessiblePages,
  resolveCreativePageContext,
  uploadCreativeAsset,
  createCreative
};
