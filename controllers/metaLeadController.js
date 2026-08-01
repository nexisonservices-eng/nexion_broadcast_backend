const axios = require('axios');

const GRAPH_API_HOST = 'graph.facebook.com';
const Campaign = require('../models/campaign');
const MetaAdCampaign = require('../models/MetaAdCampaign');
const metaAdsService = require('../services/metaAdsService');
const { getMetaConfigByUserId } = require('../services/userMetaCredentialsService');

const normalizeText = (value) => String(value || '').trim();

const normalizeFieldValues = (value) => {
  if (Array.isArray(value)) {
    return value.map((item) => normalizeText(item)).filter(Boolean);
  }

  const normalized = normalizeText(value);
  return normalized ? [normalized] : [];
};

const findLeadFieldValue = (fieldData = [], fieldNames = []) => {
  const normalizedFieldNames = fieldNames.map((name) => normalizeText(name).toLowerCase()).filter(Boolean);
  if (!normalizedFieldNames.length) return '';

  for (const field of Array.isArray(fieldData) ? fieldData : []) {
    const fieldName = normalizeText(field?.name).toLowerCase();
    if (!fieldName || !normalizedFieldNames.includes(fieldName)) continue;

    const values = normalizeFieldValues(field?.values);
    if (values.length) return values[0];
  }

  return '';
};

const toBoolean = (value) => {
  if (typeof value === 'boolean') return value;
  const normalized = normalizeText(value).toLowerCase();
  return ['true', '1', 'yes', 'y'].includes(normalized);
};

function normalizeMetaLeadConfig(metaConfig = {}) {
  return {
    leadFormId: normalizeText(metaConfig?.leadFormId),
    pageAccessToken: normalizeText(metaConfig?.pageAccessToken)
  };
}

const formatLead = (lead = {}) => {
  const fieldData = Array.isArray(lead?.field_data) ? lead.field_data : [];
  const fullName = findLeadFieldValue(fieldData, ['full_name', 'full name', 'name']);
  const phoneNumber = findLeadFieldValue(fieldData, ['phone_number', 'phone number', 'phone']);
  const email = findLeadFieldValue(fieldData, ['email', 'email address']);
  const phoneVerifiedValue = findLeadFieldValue(fieldData, ['phone_number_verified', 'phone verified', 'phone_verified']);

  return {
    leadId: normalizeText(lead?.id),
    createdTime: normalizeText(lead?.created_time),
    adId: normalizeText(lead?.ad_id),
    formId: normalizeText(lead?.form_id),
    campaignId: normalizeText(lead?.campaign_id),
    fullName,
    phoneNumber,
    email,
    phoneVerified: toBoolean(phoneVerifiedValue)
  };
};

const fetchCampaignFromAdId = async ({ adId, accessToken, apiVersion = '' } = {}) => {
  const normalizedAdId = normalizeText(adId);
  if (!normalizedAdId || !normalizeText(accessToken)) return null;

  try {
    const response = await axios.get(`https://${GRAPH_API_HOST}/${encodeURIComponent(normalizedAdId)}`, {
      timeout: 30000,
      params: {
        access_token: accessToken,
        fields: 'id,name,campaign{id,name},campaign_id'
      }
    });

    const campaignId = normalizeText(
      response?.data?.campaign?.id ||
      response?.data?.campaign_id ||
      ''
    );
    const campaignName = normalizeText(response?.data?.campaign?.name || response?.data?.name || '');

    if (!campaignId && !campaignName) return null;
    return {
      adId: normalizedAdId,
      campaignId,
      campaignName
    };
  } catch (error) {
    console.warn('[Meta Leads] Failed to resolve campaign from ad id:', normalizedAdId, error?.message || error);
    return null;
  }
};

const buildCampaignNameLookup = async ({ userId, campaignIds = [], adAccountId = '', accessToken = '', apiVersion = '' } = {}) => {
  const lookup = new Map();
  const uniqueCampaignIds = Array.from(
    new Set(
      (Array.isArray(campaignIds) ? campaignIds : [])
        .map((campaignId) => normalizeText(campaignId))
        .filter(Boolean)
    )
  );

  if (!uniqueCampaignIds.length) {
    return lookup;
  }

  const [localCampaigns, metaCampaigns] = await Promise.all([
    Campaign.find({
      createdBy: normalizeText(userId),
      metaCampaignId: { $in: uniqueCampaignIds }
    })
      .select('name metaCampaignId')
      .lean(),
    MetaAdCampaign.find({
      userId: normalizeText(userId),
      $or: [
        { 'meta.campaignId': { $in: uniqueCampaignIds } },
        { metaCampaignId: { $in: uniqueCampaignIds } }
      ]
    })
      .select('campaignName meta metaCampaignId')
      .lean()
  ]);

  const setLookup = (campaignId, campaignName) => {
    const normalizedCampaignId = normalizeText(campaignId);
    const normalizedCampaignName = normalizeText(campaignName);
    if (!normalizedCampaignId || !normalizedCampaignName || lookup.has(normalizedCampaignId)) {
      return;
    }
    lookup.set(normalizedCampaignId, normalizedCampaignName);
  };

  localCampaigns.forEach((campaign) => {
    setLookup(campaign?.metaCampaignId, campaign?.name);
  });

  metaCampaigns.forEach((campaign) => {
    setLookup(campaign?.meta?.campaignId || campaign?.metaCampaignId, campaign?.campaignName);
  });

  const unresolvedCampaignIds = uniqueCampaignIds.filter((campaignId) => !lookup.has(campaignId));
  if (unresolvedCampaignIds.length && (adAccountId || accessToken)) {
    try {
      const remoteCampaigns = await metaAdsService.fetchMetaCampaignsFromAdsManager({
        userId,
        adAccountId,
        accessToken,
        apiVersion
      });

      remoteCampaigns.forEach((campaign) => {
        setLookup(campaign?.id, campaign?.name);
      });
    } catch (error) {
      console.warn('[Meta Leads] Failed to resolve campaign names from Meta Ads Manager:', error?.message || error);
    }
  }

  return lookup;
};

const getMetaLeads = async (req, res) => {
  try {
    const userId = normalizeText(req.query?.userId || req.query?.adminId);
    const requestedFormId = normalizeText(req.params?.formId || req.query?.formId || req.query?.form_id);
    const requestedPageId = normalizeText(req.query?.pageId || req.query?.page_id);

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: 'userId is required to resolve the correct Meta lead settings.'
      });
    }

    const metaConfig = normalizeMetaLeadConfig(await getMetaConfigByUserId(userId));
    const formId = requestedFormId || normalizeText(metaConfig?.leadFormId);

    const leadsResult = await metaAdsService.getPageLeads({
      userId,
      pageId: requestedPageId,
      formId,
      limit: 100
    });
    const leads = Array.isArray(leadsResult?.leads) ? leadsResult.leads : [];
    const adCampaignLookups = await Promise.all(
      leads
        .map((lead) => normalizeText(lead?.campaignId) ? null : normalizeText(lead?.adId))
        .filter(Boolean)
        .map((adId) => fetchCampaignFromAdId({
          adId,
          accessToken: metaConfig?.userAccessToken || metaConfig?.pageAccessToken || '',
          apiVersion: metaConfig?.apiVersion || ''
        }))
    );

    const adToCampaignMap = new Map();
    adCampaignLookups.filter(Boolean).forEach((item) => {
      if (!item?.adId) return;
      adToCampaignMap.set(item.adId, item);
    });

    const campaignNameLookup = await buildCampaignNameLookup({
      userId,
      campaignIds: leads.map((lead) => lead?.campaignId || adToCampaignMap.get(normalizeText(lead?.adId))?.campaignId).filter(Boolean),
      adAccountId: metaConfig?.adAccountId || '',
      accessToken: metaConfig?.userAccessToken || metaConfig?.pageAccessToken || '',
      apiVersion: metaConfig?.apiVersion || ''
    });
    const enrichedLeads = leads.map((lead) => {
      const adId = normalizeText(lead?.adId);
      const adCampaign = adToCampaignMap.get(adId) || null;
      const campaignId = normalizeText(lead?.campaignId || adCampaign?.campaignId);
      const campaignName = campaignNameLookup.get(campaignId) || adCampaign?.campaignName || '';

      return {
        ...lead,
        adId,
        campaignId,
        campaign_id: campaignId,
        campaign_name: campaignName,
        campaignName
      };
    });

    return res.json({
      success: true,
      count: enrichedLeads.length,
      pageId: normalizeText(leadsResult?.pageId || requestedPageId),
      formId: normalizeText(leadsResult?.formId || formId),
      leads: enrichedLeads,
      campaigns: Array.from(
        new Map(
          enrichedLeads
            .filter((lead) => normalizeText(lead?.campaignId) && normalizeText(lead?.campaign_name))
            .map((lead) => [normalizeText(lead.campaignId), normalizeText(lead.campaign_name)])
        ).entries()
      ).map(([campaignId, campaignName]) => ({ campaignId, campaignName }))
    });
  } catch (error) {
    const metaError = error?.response?.data?.error || error?.response?.data || {};
    const status = Number(error?.response?.status || error?.status || 500);
    const responseError = {
      success: false,
      error: normalizeText(metaError?.message || error?.message || 'Failed to fetch Meta leads.'),
      details: {
        type: normalizeText(metaError?.type),
        code: Number.isFinite(Number(metaError?.code)) ? Number(metaError.code) : null,
        error_subcode: Number.isFinite(Number(metaError?.error_subcode)) ? Number(metaError.error_subcode) : null,
        fbtrace_id: normalizeText(metaError?.fbtrace_id)
      }
    };

    console.error(
      '[Meta Leads] Failed to fetch leads',
      JSON.stringify({
        status,
        error: responseError.error,
        code: responseError.details.code,
        error_subcode: responseError.details.error_subcode
      })
    );

    return res.status(status).json(responseError);
  }
};

module.exports = {
  getMetaLeads
};
