// This script gets GHArchive JSON for a random hour from June 2018 to April 2019, then gets the
// location of each user seen in that hour. It does this in an infinite loop and saves the JSON
// to a file every ~15s (every 10 sequential HTTP requests).
//
const _ = require('lodash');
const bb = require('bluebird');
const gs = bb.promisify(require('github-scraper'));
const fs = require('fs');
const writeFile = bb.promisify(fs.writeFile);
const rp = require('request-promise-native');
const leftPad = require('left-pad');
const gunzip = bb.promisify(require('zlib').gunzip);

const outFileName = 'out.json';
const outFileNameBlanks = 'out-blanks.json';
const nChunks = 128; // num concurrent http requests
let writeCounter = 0;
let userLocations;
try {
  userLocations = require(`./${outFileName}`);
} catch (e) {
  console.warn(`Could not open ${outFileName}. Will create`);
  userLocations = {};
}
let usersWithoutLocations = [];
try {
  usersWithoutLocations = require(`./${outFileNameBlanks}`);
} catch (e) {
  console.warn(`Could not open ${outFileNameBlanks}. Will create`);
  usersWithoutLocations = [];
}

const randomDate = (start, end) => new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));

const getUsers = async () => {
  const randDate = randomDate(new Date(2018, 5), new Date(2019, 3));
  const year = randDate.getFullYear();
  const month = leftPad(randDate.getMonth() + 1, 2, '0');
  const day = leftPad(randDate.getDate(), 2, '0');
  const hour = randDate.getHours();
  const text = (await gunzip(await rp({ uri: `https://data.gharchive.org/${year}-${month}-${day}-${hour}.json.gz`, encoding: null }))).toString();
  return _.uniq(text.split('\n').map(s => s.trim()).filter(s => s.length > 0).map(line => JSON.parse(line).actor.login));
};

(async () => {
  while(true) {
    console.log('Loading users json...');
    try {
      const users = await getUsers();
      console.log(`Scraping ${users.length} users...`);
      console.log('Getting details for each user...');
      await Promise.all(_.chunk(users, Math.round(users.length / nChunks)).map((chunk, parIdx) => (async () => {
        for (let chunkIdx = 0; chunkIdx < chunk.length; chunkIdx++) {
          const name = chunk[chunkIdx];
          if (!userLocations[name] && !usersWithoutLocations.find(user => user === name)) {
            try {
              // console.log(`Trying to get details for user ${name}...`);
              const details = await gs(name);
              const location = details.location.trim();
              if (location.length > 0) {
                userLocations[name] = location;
                // console.log(`Got location ${location} for user ${name}`);
              } else {
                usersWithoutLocations.push(name);
              }
            } catch (e) {
              console.warn('#################');
              console.warn(`Could not scrape location for user ${name}; skipping.`);
              console.warn(`Error details:`);
              console.error(e);
              console.warn('#################');
            }
            if (parIdx === 0) {
              writeCounter++;
              if (writeCounter % 10 === 0) {
                // Have the first worker periodically write all the location data to the file:
                console.log('Writing all user locations json....');
                try {
                  await writeFile(outFileName, JSON.stringify(userLocations, undefined, 2), {});
                  await writeFile(outFileNameBlanks, JSON.stringify(usersWithoutLocations, undefined, 2), {});
                } catch (e) {
                  console.warn('#################');
                  console.warn(`Could not save data file; skipping.`);
                  console.warn(`Error details:`);
                  console.error(e);
                  console.warn('#################');
                }
              }
            }
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
