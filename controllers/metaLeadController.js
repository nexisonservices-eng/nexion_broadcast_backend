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
    campaignId: normalizeText(lead?.campaign_id),
    fullName,
    phoneNumber,
    email,
    phoneVerified: toBoolean(phoneVerifiedValue)
  };
};

const buildMetaLeadsUrl = (formId) =>
  `https://${GRAPH_API_HOST}/${encodeURIComponent(normalizeText(formId))}/leads`;

const fetchAllMetaLeads = async ({ formId, accessToken }) => {
  const allLeads = [];
  const visited = new Set();
  let nextUrl = buildMetaLeadsUrl(formId);

  while (nextUrl) {
    if (visited.has(nextUrl)) break;
    visited.add(nextUrl);

    const response = await axios.get(nextUrl, {
      timeout: 30000,
      params: nextUrl === buildMetaLeadsUrl(formId) ? { access_token: accessToken } : undefined
    });

    const pageLeads = Array.isArray(response?.data?.data) ? response.data.data : [];
    allLeads.push(...pageLeads);

    const pagingNext = normalizeText(response?.data?.paging?.next);
    if (!pagingNext) {
      nextUrl = '';
      continue;
    }

    try {
      const parsedNextUrl = new URL(pagingNext);
      if (parsedNextUrl.hostname.toLowerCase() !== GRAPH_API_HOST) {
        nextUrl = '';
        continue;
      }
      nextUrl = parsedNextUrl.toString();
    } catch {
      nextUrl = '';
    }
  }

  return allLeads.map(formatLead);
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

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: 'userId is required to resolve the correct Meta lead settings.'
      });
    }

    const metaConfig = normalizeMetaLeadConfig(await getMetaConfigByUserId(userId));
    const formId = requestedFormId || normalizeText(metaConfig?.leadFormId);
    const accessToken = normalizeText(metaConfig?.pageAccessToken);

    if (!formId || !accessToken) {
      return res.status(400).json({
        success: false,
        error: requestedFormId
          ? 'Page access token is missing for the selected user in Superadmin settings.'
          : 'Lead form ID and page access token are missing for the selected user in Superadmin settings.'
      });
    }

    const leads = await fetchAllMetaLeads({ formId, accessToken });
    const campaignNameLookup = await buildCampaignNameLookup({
      userId,
      campaignIds: leads.map((lead) => lead?.campaignId).filter(Boolean),
      adAccountId: metaConfig?.adAccountId || '',
      accessToken: metaConfig?.userAccessToken || metaConfig?.pageAccessToken || accessToken,
      apiVersion: metaConfig?.apiVersion || ''
    });
    const enrichedLeads = leads.map((lead) => {
      const campaignId = normalizeText(lead?.campaignId);
      const campaignName = campaignNameLookup.get(campaignId) || '';

      return {
        ...lead,
        campaignId,
        campaign_id: campaignId,
        campaign_name: campaignName,
        campaignName
      };
    });

    return res.json({
      success: true,
      count: enrichedLeads.length,
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
