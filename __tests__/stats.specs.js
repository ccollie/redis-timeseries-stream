const pAll = require('p-all');
const { createClient, parseObjectResponse } = require("./redis");
const { round, calc_stats } = require('./utils');

const TIMESERIES_KEY = 'ts:stats';

describe('stats', () => {

  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  async function getStats(client, key, min, max, ...args) {
    const response = await client.timeseries(key, 'basic_stats', min, max, ...args);
    return parseObjectResponse(response);
  }


  // stats

  function genValues() {
    const values = [31, 41, 59, 26, 53, 58, 97, 93, 23, 84];
    const result = [];
    for (let i = 10; i < 50; i++) {
      const val = Math.floor(i / 10) * 100 + values[i % 10];
      result.push( [i, val] );
    }
    return result
  }
  async function insertData(key, values) {
    const calls = values.map(([i, value]) => () => client.timeseries(key, 'add', i, "value", value) );
    await pAll(calls, { concurrency: 8 });
  }

  test('returns the correct values', async () => {
    const raw_data = genValues();
    await insertData(TIMESERIES_KEY, raw_data);
    const filtered = raw_data.filter(([id, val]) => id >= 10 && id <= 50).map(([id, val]) => val);

    let expected = calc_stats(filtered);
    expected.std = round(expected.std, 5)
    const { value } = await getStats(client, TIMESERIES_KEY, 10, 50, 'LABELS', 'value');
    // convert strings to floats in server response

    Object.keys(value).forEach(k => {
      value[k] = parseFloat(value[k]);
      if (k === 'std') value[k] = round(value[k], 5);
    });

    // for now, just make sure we have objects returned with the proper shape

    expect(value).toEqual(expected);
  });

});
