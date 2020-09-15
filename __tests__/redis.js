const Redis = require('ioredis');
const path = require('path');
const fs = require('fs');

let script = null;
const scriptPath = path.resolve(__dirname, '../timeseries.lua');


function load(redis) {
  return loadScriptFile().then(script => {
    redis.defineCommand('timeseries', {
      numberOfKeys: 1,
      lua: script
    })
  });
}

function loadScriptFile() {
  return new Promise((resolve, reject) => {
    if (script) return resolve(script);

    fs.readFile(scriptPath, { encoding: 'utf8' }, (err, data) => {
      if (err) return reject(err);
      resolve( script = data );
    });
  })
}

/**
 * Waits for a redis client to be ready.
 * @param {Redis} redis client
 */
function isRedisReady(client) {
  return new Promise((resolve, reject) => {
    if (client.status === 'ready') {
      resolve();
    } else {
      function handleReady() {
        client.removeListener('error', handleError);
        resolve();
      }

      function handleError(err) {
        client.removeListener('ready', handleReady);
        reject(err);
      }

      client.once('ready', handleReady);
      client.once('error', handleError);
    }
  });
}

function createClient() {
  const url = process.env.REDIS_URL || 'localhost:6379';
  const client = new Redis(url);   // todo - specify db ????
  // todo: wait for client ready event
  return isRedisReady(client).then(() => {
    return load(client).then(() => client);
  });
}

function execute(client, method, key, ...args) {
  return client.timeseries(key, method, ...args);
}

async function insertData(client, key, start_ts, samples_count, data) {
  /*
  insert data to key, starting from start_ts, with 1 sec interval between them
  @param key: name of time_series
  @param start_ts: beginning of time series
  @param samples_count: number of samples
  @param data: could be a list of samples_count values, or one value. if a list, insert the values in their
  order, if not, insert the single value for all the timestamps
  */
  let pipeline = client.multi();
  let count = 0;

  const flush = async () => {
    await pipeline.exec();
    pipeline = client.multi();
    count = 0;
  };

  for (let i = 0; i < samples_count; i++) {
    let value = Array.isArray(data) ? data[i] : data;
    if (typeof(value) == 'object') {
      value = Object.entries(value).reduce((res, [key, val]) => res.concat(key, val), []);
      pipeline.timeseries(key, 'add', start_ts + i, ...value);
    } else {
      pipeline.timeseries(key, 'add', start_ts + i, 'value', value);
    }
    count++;
    if (count >= 50) {
      await flush();
    }
  }

  if (count) {
    await flush();
  }

}

// https://github.com/luin/ioredis/issues/747

function parseObjectResponse(reply) {
  if (!Array.isArray(reply)) {
    return reply
  }
  const data = {};
  for (let i = 0; i < reply.length; i += 2) {
    const val = reply[i + 1];
    data[reply[i]] = Array.isArray(val) ? parseObjectResponse(val) : val;
  }
  return data
}

function parseMessageResponse(reply) {
  if (!Array.isArray(reply)) {
    return [];
  }
  return reply.map((message) => {
    return [message[0], parseObjectResponse(message[1])]
  })
}

function parseAggregationResponse(reply) {
  if (!Array.isArray(reply)) {
    return [];
  }
  const data = [];
  for (let i = 0; i < reply.length; i += 2) {
    data.push([
        reply[i],
        parseObjectResponse(reply[i+1])
    ]);
  }
  return data
}


async function getSingleValue(client, key, timestamp, name) {
  const ra = await client.xrange(key, timestamp, timestamp, 'count', 2).then(parseMessageResponse);
  if (!ra || ra.length === 0) return null;
  if (ra.length === 1) {
    const [id, data] = ra[0];
    return data;
  } else {
    throw new Error('Critical error in timeseries.' + (name || 'getValue') + ' : multiple values for a timestamp')
  }
}

async function getRangeEx(client, cmd, key, min, max, ...args) {
  const isAggregation = args.find(x => typeof(x) === 'string' && x.toUpperCase() === 'AGGREGATION');
  const response = await client.timeseries(key, cmd, min, max, ...args);
  if (isAggregation) {
    return parseAggregationResponse(response);
  }
  return parseMessageResponse(response);
}

async function getRange(client, key, min, max, ...args) {
  return getRangeEx(client, 'range', key, min, max, ...args);
}

async function getRevRange(client, key, min, max, ...args) {
  return getRangeEx(client, 'revrange', key, min, max, ...args);
}

async function copy(client, src, dest, min, max, ...args) {
  const sha = client.scriptsSet['timeseries'].sha;

  return client.evalsha(sha, 2, src, dest, 'copy', min, max, ...args);
}

async function merge(client, src1, src2, dest, min, max, ...args) {
  const sha = client.scriptsSet['timeseries'].sha;

  return client.evalsha(sha, 3, src1, src2, dest, 'merge', min, max, ...args);
}


module.exports = {
  createClient,
  insertData,
  getSingleValue,
  getRange,
  getRevRange,
  parseMessageResponse,
  parseObjectResponse,
  parseAggregationResponse,
  copy,
  merge
};
