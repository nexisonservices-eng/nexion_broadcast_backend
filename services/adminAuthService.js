const axios = require('axios');

const ADMIN_API_BASE_URLS = [
  process.env.ADMIN_API_BASE_URL,
  process.env.ADMIN_BACKEND_URL,
  'http://localhost:8000',
  'http://localhost:5000'
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

module.exports = {
  fetchUserContext
};
