const { createClient, insertData} = require('./redis');

const TIMESERIES_KEY = 'ts:trimlength';

describe('trimlength', () => {

  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });

  const start_ts = 1511885909;
  const sample_size = 200;

  async function generateData() {
    const data = [];

    for (let i = 0; i < sample_size; i++) {
      data.push(i);
    }

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    return data;
  }


  it('should trim the timeseries by length', async () => {

    await generateData();

    const res = await client.timeseries(TIMESERIES_KEY, 'trimlength', sample_size / 2);
    const size = await client.timeseries(TIMESERIES_KEY, 'size');

    expect(size).toEqual(sample_size/2);
  });


});
