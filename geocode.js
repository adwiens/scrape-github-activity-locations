// This script geocodes locations in out.json to latitude and longitude.
// Does not pick up prev data; starts fresh and overwrites output file.
//

// rate-limited! would this work??? https://github.com/openaddresses/pelias-ubuntu


const nodeGeo = require('node-open-geocoder')();
const locs = require('./out.json');
const _ = require('lodash');
const bb = require('bluebird');
const fs = require('fs');
const writeFile = bb.promisify(fs.writeFile);

const maxSpan = 0.4; // max amount that the lat (lon) entries can differ
let counter = 0;
const nChunks = 128; // num concurrent http reqs

(async () => {
  const pairs = _.toPairs(locs);
  const out = [];
  const chunks = _.chunk(pairs, Math.round(pairs.length / nChunks));
  await Promise.all(chunks.map((chunk, chunkIdx) => (async () => {
    for(let pidx = 0; pidx < chunk.length; pidx++) {
      const [name, loc] = chunk[pidx];
      try {
        const escapedLoc = escape(loc);
        // console.log('ESCAPED:', escapedLoc);
        const geocode = nodeGeo.geocode(escapedLoc);
        const codedLoc = await (bb.promisify(geocode.end.bind(geocode))());
        const latLons = codedLoc.map(c => ({ lat: c.lat, lon: c.lon }));
        // console.log('latlons:', latLons);
        const lats = latLons.map(ll => +ll.lat);
        const lons = latLons.map(ll => +ll.lon);
        if ((_.max(lats) - _.min(lats)) < maxSpan && (_.max(lons) - _.min(lons)) < maxSpan) {
          const entry = [name, [_.mean(lats), _.mean(lons)]];
          console.log('CODED:', entry);
          out.push(entry);
          if (chunkIdx === 0) {
            counter++;
            if (counter % 5 === 0) {
              console.log('saving...');
              await writeFile('out-geo.json', JSON.stringify(_.fromPairs(out), undefined, 2), {});
            }
          }
        }
      } catch (e) {
        console.warn('#################');
        console.warn(`Something went wrong with geocode for ${loc} (name: ${name}); skipping.`);
        console.warn(`Error details:`);
        console.error(e);
        console.warn('#################');
      }
    }
  })()));
})();
