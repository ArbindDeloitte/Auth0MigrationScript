const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const lockfile = require('proper-lockfile');
const config = require('../config');
const logger = require('../logger');
const checkpointService = require('./checkpointService');

// Columns written to the manual review Excel file
const COLUMNS = [
  { header: 'Email', key: 'email', width: 36 },
  { header: 'Username (UID)', key: 'username', width: 20 },
  { header: 'First Name', key: 'given_name', width: 18 },
  { header: 'Last Name', key: 'family_name', width: 18 },
  { header: 'Language Preference', key: 'language_preference', width: 20 },
  { header: 'Email Verified', key: 'email_verified', width: 14 },
  { header: 'User Metadata', key: 'user_metadata', width: 40 },
  { header: 'App Metadata', key: 'app_metadata', width: 40 },
  { header: 'Failure Reason', key: '_failureReason', width: 50 },
  { header: 'Added At', key: '_addedAt', width: 22 },
];

class FailedUserService {
  constructor() {
    this.filePath = config.migration.manualReviewFile;
  }

  // Called lazily before any read or write so the constructor stays sync.
  // ExcelJS 4.x has no writeFileSync on the xlsx module — file creation must be async.
  async _ensureFile() {
    const dir = path.dirname(this.filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    if (!fs.existsSync(this.filePath)) {
      await this._createEmptyWorkbook();
    }
  }

  async _createEmptyWorkbook() {
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Manual Review');
    sheet.columns = COLUMNS;

    sheet.getRow(1).font = { bold: true };
    sheet.getRow(1).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FFD9E1F2' },
    };

    await workbook.xlsx.writeFile(this.filePath);
    logger.info('Manual review Excel file created', { path: this.filePath });
  }

  async appendUsers(users, reason) {
    await this._ensureFile();
    let release;
    try {
      release = await lockfile.lock(this.filePath, {
        retries: { retries: 10, minTimeout: 200, maxTimeout: 1500 },
      });

      // Read existing workbook
      const workbook = new ExcelJS.Workbook();
      await workbook.xlsx.readFile(this.filePath);
      const sheet = workbook.getWorksheet('Manual Review');

      // xlsx format does not persist ExcelJS column `key` properties — they are
      // in-memory only. Without re-applying them here, addRow({email, username, ...})
      // finds no column with any key and silently writes an empty row.
      COLUMNS.forEach((colDef, i) => {
        const col = sheet.getColumn(i + 1);
        col.key = colDef.key;
      });

      const addedAt = new Date().toISOString();
      for (const user of users) {
        sheet.addRow({
          email: user.email || '',
          username: user.username || '',
          given_name: user.given_name || '',
          family_name: user.family_name || '',
          language_preference: user.user_metadata?.language || '',
          email_verified: user.email_verified !== undefined ? String(user.email_verified) : '',
          user_metadata: user.user_metadata ? JSON.stringify(user.user_metadata) : '',
          app_metadata: user.app_metadata ? JSON.stringify(user.app_metadata) : '',
          _failureReason: reason,
          _addedAt: addedAt,
        });
      }

      // Atomic write: write to temp file then rename.
      // On Windows, unlink fails with EBUSY if the file is open in Excel.
      // Retry up to 5 times (15 s total) so the user has time to close it.
      const tmpPath = this.filePath + '.tmp';
      await workbook.xlsx.writeFile(tmpPath);

      let swapped = false;
      for (let attempt = 0; attempt < 5 && !swapped; attempt++) {
        try {
          if (fs.existsSync(this.filePath)) fs.unlinkSync(this.filePath);
          fs.renameSync(tmpPath, this.filePath);
          swapped = true;
        } catch (swapErr) {
          if ((swapErr.code === 'EBUSY' || swapErr.code === 'EPERM') && attempt < 4) {
            logger.warn(
              'manual-review.xlsx is locked — please close it in Excel. Retrying in 3 s…',
              { attempt: attempt + 1, maxAttempts: 5 }
            );
            await new Promise(r => setTimeout(r, 3000));
          } else {
            try { fs.unlinkSync(tmpPath); } catch { /* ignore tmp cleanup error */ }
            throw swapErr;
          }
        }
      }

      logger.warn('Users added to manual review Excel', {
        count: users.length,
        reason,
        file: this.filePath,
      });
    } finally {
      if (release) await release();
    }

    // Mirror emails to the Redis SET so gap detection can quickly determine
    // whether a source user is in manual review without reading the Excel file.
    // Called outside the lockfile scope so the lock is released first.
    try {
      const emails = users.map(u => u.email).filter(Boolean);
      if (emails.length > 0) {
        await checkpointService.recordManualReviewUsers(emails);
      }
    } catch (redisErr) {
      // Non-fatal: the Excel file is the source of truth. Gap detection will
      // still work via retryCount checks on the next startup.
      logger.warn('Could not record manual review users in Redis', { error: redisErr.message });
    }
  }

  async readAll() {
    await this._ensureFile();
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(this.filePath);
    const sheet = workbook.getWorksheet('Manual Review');
    const users = [];
    sheet.eachRow((row, rowNumber) => {
      if (rowNumber === 1) return; // skip header
      users.push({
        email: row.getCell('email').value,
        _failureReason: row.getCell('_failureReason').value,
        _addedAt: row.getCell('_addedAt').value,
      });
    });
    return users;
  }

  async getCount() {
    const users = await this.readAll();
    return users.length;
  }
}

module.exports = new FailedUserService();
