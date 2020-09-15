const { createClient } = require('./redis');

const TIMESERIES_KEY = 'timeseries:span';

describe('span', () => {
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

  it('should return the first and last timestamp', async () => {

    await callMethod('add', 1000, 'name', 'alice');
    await callMethod('add', 2000, 'name', 'bob', 'age', 20);
    await callMethod('add', 7500, 'name', 'charlie');

    const actual = await callMethod('span');
    expect(actual).toStrictEqual(["1000-0", "7500-0"]);
  });

  it('should return the same timestamp for start and end if there is only one entry', async () => {

    await callMethod('add', 6007, "name", "billy");

    const actual = await callMethod('span');
    expect(actual).toStrictEqual(["6007-0", "6007-0"]);
  });

});
