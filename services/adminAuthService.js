const axios = require('axios');

const ADMIN_API_BASE_URLS = [
  process.env.ADMIN_API_BASE_URL,
  process.env.ADMIN_BACKEND_URL
]
  .map((url) => (url || '').trim())
  .filter(Boolean)
  .filter((url, index, arr) => arr.indexOf(url) === index);

const ADMIN_USER_CREDENTIALS_ENDPOINT =
  process.env.ADMIN_USER_CREDENTIALS_ENDPOINT ||
  process.env.ADMIN_CREDENTIALS_ENDPOINT_PATH ||
  '/api/user/credentials';

const fetchUserContext = async (authHeader) => {
  let lastError = null;
  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const response = await axios.get(`${baseUrl}${ADMIN_USER_CREDENTIALS_ENDPOINT}`, {
        headers: { Authorization: authHeader },
        timeout: 10000
      });
      const data = response?.data?.data || response?.data || {};
      return data;
    } catch (error) {
      lastError = error;
    }
  }
  if (lastError) throw lastError;
  return null;
};

const fetchWorkspaceUsers = async (authHeader, { superAdmin = false, companyId = '' } = {}) => {
  const normalizedCompanyId = String(companyId || '').trim();
  const endpoint = superAdmin
    ? normalizedCompanyId
      ? `/api/admin/users/roster?companyId=${encodeURIComponent(normalizedCompanyId)}`
      : ''
    : '/api/crm/ops/users';
  if (!endpoint) return [];
  let lastError = null;

  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const response = await axios.get(`${baseUrl.replace(/\/+$/, '')}${endpoint}`, {
        headers: { Authorization: authHeader },
        timeout: 10000
      });
      const payload = response?.data?.data || response?.data || {};
      const users = Array.isArray(payload?.users)
        ? payload.users
        : Array.isArray(payload)
          ? payload
          : [];
      if (!superAdmin) return users;

      return users.filter((user) => String(user?.companyId || '').trim() === normalizedCompanyId);
    } catch (error) {
      lastError = error;
    }
  }

  const error = new Error('Unable to verify the task assignee against the workspace user directory.');
  error.status = 503;
  error.cause = lastError;
  throw error;
};

const fetchSuperAdminCustomers = async (authHeader) => {
  if (ADMIN_API_BASE_URLS.length === 0) {
    const error = new Error('The admin service is not configured for the customer directory.');
    error.status = 503;
    throw error;
  }
  let lastError = null;
  for (const baseUrl of ADMIN_API_BASE_URLS) {
    try {
      const response = await axios.get(`${baseUrl.replace(/\/+$/, '')}/api/admin/users`, {
        headers: { Authorization: authHeader },
        timeout: 15000
      });
      const payload = response?.data?.data || response?.data || {};
      return Array.isArray(payload) ? payload : Array.isArray(payload?.users) ? payload.users : [];
    } catch (error) {
      lastError = error;
    }
  }
  const error = new Error('Unable to load the customer directory from the admin service.');
  error.status = 503;
  error.cause = lastError;
  throw error;
};

module.exports = {
  fetchUserContext,
  fetchWorkspaceUsers,
  fetchSuperAdminCustomers
};
