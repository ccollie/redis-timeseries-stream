const pAll = require('p-all');
const { createClient, getRange, insertData } = require('./redis');
const { round, calc_stats, random } = require('./utils');

const TIMESERIES_KEY = 'ts:aggregation';

describe('aggregation', () => {

  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  // aggregation

  async function insertAggregationData(key = TIMESERIES_KEY) {
    const calls = [];
    const result = [];

    const values = [31, 41, 59, 26, 53, 58, 97, 93, 23, 84];
    for (let i = 10; i < 50; i++) {
      const val = Math.floor(i / 10) * 100 + values[i % 10];
      result.push(val);
      calls.push( () => client.timeseries(key, 'add', i, "value", val) );
    }

    await pAll(calls, { concurrency: 8 });
    return result
  }

  async function runAggregation(key, min, max, aggType) {
    const response = await getRange(client, key, min, max, 'AGGREGATION', 10, `${aggType}(value)`);
    return response.map(([ts, data]) => {
      return [ts, parseFloat(data.value[aggType])]
    });
  }

  async function testAggregation(type, expected) {
    await insertAggregationData(TIMESERIES_KEY);
    const actual = await runAggregation(TIMESERIES_KEY, 10, 50, type);
    expect(actual).toEqual(expected)
  }

  test('min', async () => {
    const expected = [[10, 123], [20, 223], [30, 323], [40, 423]];
    await testAggregation('min', expected);
  });

  test('max', async () => {
    const expected  = [[10, 197], [20, 297], [30, 397], [40, 497]];
    await testAggregation('max', expected);
  });

  test('avg', async () => {
    const expected = [[10, 156.5], [20, 256.5], [30, 356.5], [40, 456.5]];
    await testAggregation('avg', expected);
  });

  test('median', async () => {
    const expected = [[10, 155.5], [20, 255.5], [30, 355.5], [40, 455.5]];
    await testAggregation('median', expected);
  });

  test('sum', async () => {
    const expected = [[10, 1565], [20, 2565], [30, 3565], [40, 4565]];
    await testAggregation('sum', expected);
  });

  test('count', async () => {
    const expected = [[10, 10], [20, 10], [30, 10], [40, 10]];
    await testAggregation('count', expected);
  });

  test('first', async () => {
    const expected = [[10, 131], [20, 231], [30, 331], [40, 431]];
    await testAggregation('first', expected);
  });

  test('last', async () => {
    const expected = [[10, 184], [20, 284], [30, 384], [40, 484]];
    await testAggregation('last', expected);
  });

  test('range', async () => {
    const expected = [[10, 74], [20, 74], [30, 74], [40, 74]];
    await testAggregation('range', expected);
  });

  test('std deviation', async () => {
    const raw_data = await insertAggregationData(TIMESERIES_KEY);
    const filtered = raw_data.filter(([id, val]) => id >= 10 && id <= 50);

    const buckets = {};
    const bucketIds = new Set();
    filtered.forEach(([id, val]) => {
      let round = (id - (id % 10));
      bucketIds.add(round);
      buckets[round] = buckets[round] || [];
      buckets[round].push(val);
    });
    bucketIds.forEach(id => {
      buckets[id] = round(calc_stats(buckets[id]).std, 5);
    });

    const expected = Array.from(bucketIds).sort().map(ts => [ts, buckets[ts]]);

    const response = await getRange(client, TIMESERIES_KEY, 10, 50, 'AGGREGATION', 10, 'stdev(value)');
    // convert strings to floats in server response
    const actual = response.map(([ts, data]) => {
      const value = round(parseFloat(data.value.stdev), 5);
      return [ts, value];
    });

    expect(actual).toEqual(expected);
  });

  test('multiple labels', async () => {
    const start_ts = 1488823384;
    const samples_count = 50;

    const data = [];

    const states = ['ready', 'active', 'waiting', 'complete'];

    for (let i = 0; i < samples_count; i++) {
      const state = states[i % states.length];
      data.push({
        state,
        num: random(5, 100),
        value: random(10, 100)
      })
    }
    await insertData(client, TIMESERIES_KEY, start_ts, samples_count, data);

    const args = ['AGGREGATION', 10, 'sum(num)', 'sum(value)'];

    const response = await getRange(client, TIMESERIES_KEY, '-', '+', ...args);

    response.forEach(x => {
      const agg = x[1];
      expect(agg).toHaveProperty('value');
      expect(agg).toHaveProperty('num');
    })

  });

});
