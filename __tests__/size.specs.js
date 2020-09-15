const { createClient } = require('./redis');

const TIMESERIES_KEY = 'timeseries:size_test_key';

describe('size', () => {
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

  it('should return the correct list size', async () => {
    let size = await callMethod('size');
    expect(size).toEqual(0);

    await callMethod('add', 1005, 'first', 1);
    await callMethod('add', 1006, 'second', 2);
    await callMethod('add', 1007, 'third', 3);
    await callMethod('add', 1008, 'fourth', 4);
    size = await callMethod('size');
    expect(size).toBe(4);
  });

  it('should return null if the series does not exist', async () => {
    const sz = await client.timeseries('no-such-key', 'size');
    expect(sz).toEqual(null);

  });

});
