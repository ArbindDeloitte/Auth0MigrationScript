const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const config = require('../config');
const logger = require('../logger');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Respect Auth0's Retry-After / x-ratelimit-reset header; default 60s
function parseRetryAfterMs(headers = {}) {
  const ra = headers['retry-after'];
  if (ra) {
    const n = Number(ra);
    // Retry-After can be seconds or an HTTP-date epoch
    return n > 1_000_000 ? (n - Date.now()) : n * 1_000;
  }
  const reset = headers['x-ratelimit-reset'];
  if (reset) return Math.max(0, Number(reset) * 1_000 - Date.now());
  return 60_000; // fallback
}

class Auth0Service {
  constructor() {
    this._token = null;
    this._tokenExpiresAt = null;
  }

  async _getToken() {
    // Refresh if token is absent or within 5 minutes of expiry
    if (this._token && this._tokenExpiresAt && Date.now() < this._tokenExpiresAt - 300_000) {
      return this._token;
    }

    logger.info('Fetching Auth0 Management API token');
    const response = await axios.post(
      `https://${config.auth0.domain}/oauth/token`,
      {
        client_id: config.auth0.clientId,
        client_secret: config.auth0.apiKey,
        audience: config.auth0.audience,
        grant_type: 'client_credentials',
      }
    );

    this._token = response.data.access_token;
    this._tokenExpiresAt = Date.now() + response.data.expires_in * 1000;
    logger.info('Auth0 Management API token acquired');
    return this._token;
  }

  async createImportJob(chunkFilePath, { upsert = true, externalId } = {}) {
    const MAX_RATE_LIMIT_RETRIES = 8;

    for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
      const token = await this._getToken();
      const form = new FormData();

      form.append('users', fs.createReadStream(chunkFilePath), {
        filename: 'users.json',
        contentType: 'application/json',
      });
      form.append('connection_id', config.auth0.connectionId);
      form.append('upsert', String(upsert));
      form.append('send_completion_email', 'false');
      if (externalId) form.append('external_id', externalId);

      try {
        const response = await axios.post(
          `https://${config.auth0.domain}/api/v2/jobs/users-imports`,
          form,
          {
            headers: {
              Authorization: `Bearer ${token}`,
              ...form.getHeaders(),
            },
          }
        );
        return response.data; // { id, type, status, connection_id, external_id, ... }
      } catch (err) {
        if (err.response?.status === 429 && attempt < MAX_RATE_LIMIT_RETRIES) {
          const waitMs = parseRetryAfterMs(err.response.headers);
          // Add ±10% jitter to avoid thundering herd when multiple workers hit the limit
          const jitter = Math.floor(waitMs * 0.1 * Math.random());
          const delay = Math.max(waitMs + jitter, 10_000); // never wait less than 10s
          logger.warn(`Auth0 rate limit (429) on createImportJob — waiting ${Math.round(delay / 1000)}s before retry`, {
            attempt: attempt + 1,
            maxRetries: MAX_RATE_LIMIT_RETRIES,
            externalId,
            retryAfterMs: waitMs,
          });
          await sleep(delay);
          // Continue to next loop iteration (re-creates form with fresh stream)
        } else {
          throw err;
        }
      }
    }
  }

  async getJobStatus(jobId) {
    const token = await this._getToken();
    const response = await axios.get(
      `https://${config.auth0.domain}/api/v2/jobs/${jobId}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    return response.data; // { id, status, summary: { total, failed, inserted, updated } }
  }

  async getJobErrors(jobId) {
    const token = await this._getToken();
    const response = await axios.get(
      `https://${config.auth0.domain}/api/v2/jobs/${jobId}/errors`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    // Returns array of { type, code, message, user } objects
    return Array.isArray(response.data) ? response.data : [];
  }
}

module.exports = new Auth0Service();
