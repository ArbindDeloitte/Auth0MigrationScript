const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');
const logger = require('../logger');

function rowToAuth0User(rowObj) {
  // Support both new column names (first_name, last_name, uid, language_preference, password_hash)
  // and legacy names (given_name, family_name) so older Excel inputs still work.
  const firstName = (rowObj['first_name'] || rowObj['given_name'] || '').trim();
  const lastName = (rowObj['last_name'] || rowObj['family_name'] || '').trim();
  const fullName = [firstName, lastName].filter(Boolean).join(' ');

  const user = {
    email: rowObj['email'],
    email_verified: rowObj['email_verified'] === true || rowObj['email_verified'] === 'true',
  };

  if (firstName) user.given_name = firstName;
  if (lastName) user.family_name = lastName;
  if (fullName) user.name = fullName;

  // UID maps to Auth0 username
  const uid = rowObj['uid'] || rowObj['username'];
  if (uid) user.username = String(uid);

  // password_hash: wrap string in Auth0 custom_password_hash envelope;
  // pass objects (already-structured ETL output) straight through.
  const rawHash = rowObj['password_hash'];
  if (rawHash) {
    user.custom_password_hash =
      typeof rawHash === 'object'
        ? rawHash
        : { algorithm: 'sha512', hash: { value: rawHash, encoding: 'base64' } };
  }

  // Language preference goes into user_metadata
  const lang = rowObj['language_preference'];
  if (lang) {
    user.user_metadata = { language: lang };
  } else if (rowObj['user_metadata']) {
    try {
      user.user_metadata = JSON.parse(rowObj['user_metadata']);
    } catch {
      logger.warn('Invalid JSON in user_metadata column — skipping field', { value: rowObj['user_metadata'] });
    }
  }

  if (rowObj['app_metadata']) {
    try {
      user.app_metadata = JSON.parse(rowObj['app_metadata']);
    } catch {
      logger.warn('Invalid JSON in app_metadata column — skipping field', { value: rowObj['app_metadata'] });
    }
  }

  // Remove undefined fields — Auth0 rejects unknown undefined keys
  return Object.fromEntries(Object.entries(user).filter(([, v]) => v !== undefined));
}

async function* streamUsersFromExcel(filePath) {
  const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {});
  let headers = null;
  let rowNumber = 0;

  for await (const worksheet of workbookReader) {
    for await (const row of worksheet) {
      rowNumber++;
      if (row.number === 1) {
        headers = row.values.slice(1);
        continue;
      }
      if (!headers) continue;

      const rowObj = {};
      const values = row.values.slice(1);
      headers.forEach((header, i) => {
        if (header && values[i] !== undefined && values[i] !== null) {
          rowObj[String(header).trim()] = values[i];
        }
      });

      if (!rowObj['email']) {
        logger.warn('Row missing email — skipped', { rowNumber });
        continue;
      }

      yield rowToAuth0User(rowObj);
    }
  }
}

async function createChunks(filePath, chunksDir) {
  if (!fs.existsSync(chunksDir)) fs.mkdirSync(chunksDir, { recursive: true });

  const chunks = [];
  let currentBatch = [];
  let currentSizeBytes = 2; // accounts for JSON array brackets '[]'
  let chunkIndex = 0;

  const flushChunk = () => {
    if (currentBatch.length === 0) return;
    const chunkId = uuidv4();
    const chunkPath = path.join(chunksDir, `chunk-${chunkIndex}-${chunkId}.json`);
    const content = JSON.stringify(currentBatch);
    fs.writeFileSync(chunkPath, content, 'utf-8');
    const actualBytes = Buffer.byteLength(content, 'utf-8');
    chunks.push({ chunkId, chunkPath, userCount: currentBatch.length });
    logger.info(`Chunk ${chunkIndex} written`, { chunkId, users: currentBatch.length, bytes: actualBytes });
    chunkIndex++;
    currentBatch = [];
    currentSizeBytes = 2;
  };

  for await (const user of streamUsersFromExcel(filePath)) {
    const userJson = JSON.stringify(user);
    // +1 for the comma separator between array elements
    const addedBytes = Buffer.byteLength(currentBatch.length > 0 ? ',' + userJson : userJson, 'utf-8');

    if (currentSizeBytes + addedBytes > config.migration.maxChunkBytes && currentBatch.length > 0) {
      flushChunk();
    }

    currentBatch.push(user);
    currentSizeBytes += Buffer.byteLength(
      currentBatch.length === 1 ? userJson : ',' + userJson,
      'utf-8'
    );
  }

  flushChunk(); // flush the last partial chunk

  logger.info(`Chunking complete`, { totalChunks: chunks.length });
  return chunks;
}

module.exports = { streamUsersFromExcel, createChunks };
