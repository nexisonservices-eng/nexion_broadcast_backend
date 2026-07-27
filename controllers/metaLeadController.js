const axios = require('axios');

const GRAPH_API_HOST = 'graph.facebook.com';
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

const getMetaLeads = async (req, res) => {
  try {
    const queryFormId = normalizeText(req.query?.formId);
    const queryAccessToken = normalizeText(req.query?.pageAccessToken);
    const metaConfig = normalizeMetaLeadConfig(await getMetaConfigByUserId('superadmin-id'));
    const formId = queryFormId || normalizeText(metaConfig?.leadFormId);
    const accessToken = queryAccessToken || normalizeText(metaConfig?.pageAccessToken);

    if (!formId || !accessToken) {
      return res.status(400).json({
        success: false,
        error: 'Lead form ID and page access token are missing from the superadmin Meta settings.'
      });
    }

    const leads = await fetchAllMetaLeads({ formId, accessToken });

    return res.json({
      success: true,
      count: leads.length,
      leads
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
