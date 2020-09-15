const { createClient, parseObjectResponse, insertData } = require("./redis");

const TIMESERIES_KEY = 'ts:distinct';


describe('distinct', () => {

  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  async function getDistinct(key, min, max, ...args) {
    return await client.timeseries(key, 'distinct', min, max, ...args);
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

    const response = await getDistinct(TIMESERIES_KEY, '-', '+', 'LABELS',  'job');

    expect(response[0]).toEqual('job');
    const actual = response[1].sort();

    expect(actual).toEqual(jobs);
  });

});
