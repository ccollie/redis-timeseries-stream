
const { createClient } = require('./redis');

const TIMESERIES_KEY = 'ts:add';

describe('add', () => {
  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  function callMethod(name, ...args) {
    return client.timeseries(TIMESERIES_KEY, name, ...args);
  }


  it('should add a value to the set', async () => {
    await callMethod('add', 1000, "beers", 30);
    const res = await client.xrange(TIMESERIES_KEY, '-', '+');
    expect(res.length).toBe(1);
    expect(res[0]).toEqual(
        [
            "1000-0", ["beers", "30"]
        ]
    );
  });


  it('should allow arbitrary data to be associated with a timestamp', async () => {

    await callMethod('add', 1000, "active", 1, "waiting", 2, "error", 3, "complete", 4);

    const res = await client.xrange(TIMESERIES_KEY, '-', '+');
    expect(res.length).toBe(1);
    expect(res[0]).toEqual(
        [
          "1000-0", ["active", "1", "waiting", "2", "error", "3", "complete", "4"]
        ]
    );
  });

  it('should disallow duplicate values', async () => {
    await callMethod('add', 1000, "active", 1);
    await callMethod('add', 1000, "active", 1)
        .catch(e => {
          expect(e.message).toMatch(/The ID specified in XADD is equal or smaller than the target stream top item/);
        });
  });

  it('should throw on mismatched key/value count', async () => {
    await callMethod('add', 1000, "last_name")
        .catch(e => expect(e.message).toMatch(/Number of arguments must be even/));
  });

});
