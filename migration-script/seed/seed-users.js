'use strict';
/**
 * seed-users.js
 * Inserts 2000 synthetic source users into migration:source:users
 * Run from the migration-script directory: node seed/seed-users.js
 */

const Redis = require('ioredis');
const crypto = require('crypto');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const SOURCE_KEY = process.env.REDIS_SOURCE_KEY || 'migration:source:users';
const COUNT      = parseInt(process.env.SEED_COUNT || '2000', 10);
const BATCH_SIZE = 200;

const FIRST_NAMES = [
  'James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
  'David','Barbara','William','Elizabeth','Richard','Susan','Joseph','Jessica',
  'Thomas','Sarah','Charles','Karen','Christopher','Lisa','Daniel','Nancy',
  'Matthew','Betty','Anthony','Margaret','Mark','Sandra','Donald','Ashley',
  'Steven','Dorothy','Paul','Kimberly','Andrew','Emily','Kenneth','Donna',
  'George','Michelle','Joshua','Carol','Kevin','Amanda','Brian','Melissa',
  'Edward','Deborah','Ronald','Stephanie','Timothy','Rebecca','Jason','Sharon',
  'Jeffrey','Laura','Ryan','Cynthia','Jacob','Kathleen','Gary','Amy',
  'Nicholas','Angela','Eric','Shirley','Jonathan','Anna','Stephen','Brenda',
  'Larry','Emma','Justin','Virginia','Scott','Crystal','Brandon','Maria',
  'Raymond','Beverly','Frank','Heather','Gregory','Amber','Samuel','Evelyn',
  'Benjamin','Diane','Patrick','Jean','Jack','Ruth','Alexander','Victoria',
  'Dennis','Carolyn',
];

const LAST_NAMES = [
  'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
  'Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson',
  'Thomas','Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson',
  'White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson','Walker',
  'Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell',
  'Carter','Roberts','Phillips','Evans','Turner','Parker','Collins','Edwards',
  'Stewart','Flores','Morris','Nguyen','Murphy','Cook','Rogers','Morgan',
  'Peterson','Cooper','Reed','Bailey','Bell','Gomez','Kelly','Howard','Ward',
  'Cox','Diaz','Richardson','Wood','Watson','Brooks','Bennett','Gray','James',
  'Reyes','Cruz','Hughes','Price','Myers','Long','Foster','Sanders','Ross',
  'Morales','Powell','Sullivan','Russell','Ortiz','Jenkins','Gutierrez','Perry',
];

const LANGUAGES = ['en', 'en', 'en', 'es', 'es', 'zh', 'ko', 'tl', 'vi', 'hy'];
const DOMAINS   = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com', 'ladwp.com'];

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function fakePasswordHash() {
  // SHA-512 = 64 bytes = 88 base64 chars
  return crypto.randomBytes(64).toString('base64');
}

function makeUser(index) {
  const first = pick(FIRST_NAMES);
  const last  = pick(LAST_NAMES);
  const num   = String(index).padStart(5, '0');
  const domain = pick(DOMAINS);
  const sep  = Math.random() > 0.5 ? '.' : '_';
  const email = `${first.toLowerCase()}${sep}${last.toLowerCase()}${num}@${domain}`;
  const uid   = `UID${num}${Math.floor(Math.random() * 90 + 10)}`;

  return {
    email,
    uid,
    first_name:            first,
    last_name:             last,
    password_hash:         fakePasswordHash(),
    language_preference:   pick(LANGUAGES),
    requireEmailChange:    Math.random() < 0.3, // ~30% of users flagged
  };
}

async function main() {
  const redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: null });
  console.log(`\nConnecting to Redis ${REDIS_HOST}:${REDIS_PORT}...`);

  const before = await redis.llen(SOURCE_KEY);
  console.log(`Current record count: ${before}`);
  console.log(`Inserting ${COUNT} users in batches of ${BATCH_SIZE}...\n`);

  let inserted = 0;
  const startIndex = before + 1; // keep UIDs unique relative to what's already there

  while (inserted < COUNT) {
    const batchEnd  = Math.min(inserted + BATCH_SIZE, COUNT);
    const batch     = [];
    for (let i = inserted; i < batchEnd; i++) {
      batch.push(JSON.stringify(makeUser(startIndex + i)));
    }
    await redis.rpush(SOURCE_KEY, ...batch);
    inserted = batchEnd;
    process.stdout.write(`\r  Inserted ${inserted} / ${COUNT}`);
  }

  const after = await redis.llen(SOURCE_KEY);
  console.log(`\n\n═══════════════════════════════════════════`);
  console.log(`  Done. Total records in Redis: ${after}`);
  console.log(`  New records added:            ${after - before}`);
  console.log(`═══════════════════════════════════════════\n`);

  await redis.quit();
}

main().catch(err => {
  console.error('\nFailed:', err.message);
  process.exit(1);
});
