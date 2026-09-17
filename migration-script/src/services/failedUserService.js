const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const lockfile = require('proper-lockfile');
const config = require('../config');
const logger = require('../logger');

// Columns written to the manual review Excel file
const COLUMNS = [
  { header: 'Email', key: 'email', width: 36 },
  { header: 'Name', key: 'name', width: 24 },
  { header: 'Given Name', key: 'given_name', width: 18 },
  { header: 'Family Name', key: 'family_name', width: 18 },
  { header: 'Phone Number', key: 'phone_number', width: 18 },
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

      const addedAt = new Date().toISOString();
      for (const user of users) {
        sheet.addRow({
          email: user.email || '',
          name: user.name || '',
          given_name: user.given_name || '',
          family_name: user.family_name || '',
          phone_number: user.phone_number || '',
          email_verified: user.email_verified !== undefined ? String(user.email_verified) : '',
          user_metadata: user.user_metadata ? JSON.stringify(user.user_metadata) : '',
          app_metadata: user.app_metadata ? JSON.stringify(user.app_metadata) : '',
          _failureReason: reason,
          _addedAt: addedAt,
        });
      }

      // Atomic write: write to temp file then rename
      const tmpPath = this.filePath + '.tmp';
      await workbook.xlsx.writeFile(tmpPath);

      // Windows-safe atomic swap
      if (fs.existsSync(this.filePath)) fs.unlinkSync(this.filePath);
      fs.renameSync(tmpPath, this.filePath);

      logger.warn('Users added to manual review Excel', {
        count: users.length,
        reason,
        file: this.filePath,
      });
    } finally {
      if (release) await release();
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
