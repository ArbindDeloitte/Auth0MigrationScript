const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const config = require('../config');
const logger = require('../logger');

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
