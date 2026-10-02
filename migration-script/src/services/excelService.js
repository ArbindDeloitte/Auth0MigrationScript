const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');
const logger = require('../logger');
const failedUserService = require('./failedUserService');
const checkpointService = require('./checkpointService');

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

  // UID maps to Auth0 username. Auth0 enforces a 15-char maximum.
  // Oversized usernames are not truncated here — createChunks detects them,
  // skips the user from the import chunk, and routes them to manual review.
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

  const parsedAppMeta = rowObj['app_metadata']
    ? (() => { try { return JSON.parse(rowObj['app_metadata']); } catch { logger.warn('Invalid JSON in app_metadata column — skipping field', { value: rowObj['app_metadata'] }); return {}; } })()
    : {};

  user.app_metadata = {
    duplicateEmail: parsedAppMeta.duplicateEmail ?? parsedAppMeta.requireEmailChange ?? false,
    emailChanged:   parsedAppMeta.emailChanged   ?? false,
    ...parsedAppMeta,
  };
  delete user.app_metadata.requireEmailChange;

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
  const invalidUsernameUsers = [];  // users skipped due to username > 15 chars
  const allValidUsers = [];         // accumulate for Redis email index
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
    // Auth0 enforces a configurable username length limit (AUTH0_USERNAME_MAX_LENGTH, default 15).
    // Rather than silently truncating (which could map two different users to the same username),
    // skip the user entirely and route them to manual review so they can be fixed at the source.
    if (user.username && user.username.length > config.migration.usernameMaxLength) {
      logger.warn('Username exceeds max length — user skipped from import, routed to manual review', {
        email: user.email,
        username: user.username,
        length: user.username.length,
        maxLength: config.migration.usernameMaxLength,
      });
      invalidUsernameUsers.push(user);
      continue;
    }

    allValidUsers.push(user);

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

  // Build the email → full-user index in Redis. statusProcessor uses this as a
  // fallback when a chunk file is unavailable (e.g. after a crash), ensuring the
  // full user object — including custom_password_hash — is always retrievable.
  await checkpointService.bulkIndexUsersByEmail(allValidUsers);

  // Write oversized-username users to manual review in one batch
  if (invalidUsernameUsers.length > 0) {
    await failedUserService.appendUsers(
      invalidUsernameUsers,
      `Username exceeds Auth0 ${config.migration.usernameMaxLength}-character limit — must be fixed in source data`
    );
    logger.warn('Oversized-username users written to manual review', {
      count: invalidUsernameUsers.length,
      maxLength: config.migration.usernameMaxLength,
    });
  }

  logger.info(`Chunking complete`, { totalChunks: chunks.length, skippedInvalidUsername: invalidUsernameUsers.length });
  return chunks;
}

module.exports = { streamUsersFromExcel, createChunks };
