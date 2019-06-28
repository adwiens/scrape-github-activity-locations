// This script gets GHArchive JSON for a random hour from June 2018 to June 2019, then gets the
// location of each user seen in that hour. It does this in an infinite loop and appends the
// JSON to a file each time it gets a new user location.
//
// The script can be restarted, e.g. after crashing, and it will continue appending to the file.
//
const _ = require('lodash');
const bb = require('bluebird');
const gs = bb.promisify(require('github-scraper'));
const fs = require('fs');
const writeFile = bb.promisify(fs.writeFile);
const rp = require('request-promise-native');
const leftPad = require('left-pad');
const gunzip = bb.promisify(require('zlib').gunzip);

const outFileName = 'user-locations.json';
const nChunks = 16; // num concurrent http requests

const randomDate = (start, end) => new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));

/** Get string array of some randomly sampled, unique usernames from a random hour. */
const getUsers = async () => {
  const randDate = randomDate(new Date(2018, 5), new Date(2019, 5));
  const year = randDate.getFullYear();
  const month = leftPad(randDate.getMonth() + 1, 2, '0');
  const day = leftPad(randDate.getDate(), 2, '0');
  const hour = randDate.getHours();
  const text = (await gunzip(await rp({ uri: `https://data.gharchive.org/${year}-${month}-${day}-${hour}.json.gz`, encoding: null }))).toString();
  const uniqueUsernames = _.uniq(text.split('\n').map(s => s.trim()).filter(s => s.length > 0).map(line => JSON.parse(line).actor.login));
  return _.sampleSize(uniqueUsernames, _.round(0.05 * uniqueUsernames.length));
};

(async () => {
  while(true) {
    console.log('Downloading users json...');
    try {
      const users = await getUsers();
      console.log(`Scraping ${users.length} users...`);
      console.log('Getting details for each user...');
      await Promise.all(_.chunk(users, Math.round(users.length / nChunks)).map((chunk, _parIdx) => (async () => {
        for (let chunkIdx = 0; chunkIdx < chunk.length; chunkIdx++) {
          const name = chunk[chunkIdx];
          try {
            // console.log(`Trying to get details for user ${name}...`);
            const details = await gs(name);
            const location = details.location.trim().split('\n')[0];
            if (location.length > 0) {
              await writeFile(outFileName, JSON.stringify([name, location]) + '\n', { flag: 'a' });
              // console.log(`Saved location ${location} for user ${name}`);
            }
          } catch (e) {
            console.warn('#################');
            console.warn(`Could not scrape location for user ${name}; skipping.`);
            console.warn(`Error details:`);
            console.error(e);
            console.warn('#################');
          }
        }
      })()));
    } catch (e) {
      console.warn('#################');
      console.warn(`Could not get users for a random day. skipping...`);
      console.warn(`Error details:`);
      console.error(e);
      console.warn('#################');
    }
  }
})();
