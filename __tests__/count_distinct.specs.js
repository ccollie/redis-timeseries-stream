const { createClient, parseObjectResponse, insertData } = require("./redis");

const TIMESERIES_KEY = 'ts:count_distinct';


describe('distinct', () => {

  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });

  async function getCountDistinct(key, min, max, ...args) {
    const response =  await client.timeseries(key, 'count_distinct', min, max, ...args);
    return parseObjectResponse(response);
  }

  // stats
  test('returns the correct values', async () => {
    const start_ts = 1488823384;
    const samples_count = 50;

    const data = [];

    const states = ['ready', 'active', 'waiting', 'complete'];
    const jobs = ['cleanup', 'execution', 'preparation'];

    for (let ts = start_ts, i = 0; i < samples_count; i++, ts++) {
      const job = jobs[i % jobs.length];
      const state = states[i % states.length];
      data.push({
        ts,
        id: i,
        job,
        state
      })
    }
    await insertData(client, TIMESERIES_KEY, start_ts, samples_count, data);

    const expected = {};
    data.forEach((rec) => {
      expected[rec.state] = (expected[rec.state] || 0) + 1;
    });

    const { state } = await getCountDistinct(TIMESERIES_KEY, '-', '+', 'LABELS',  'state');

    expect(state).toEqual(expected);
  });

});
