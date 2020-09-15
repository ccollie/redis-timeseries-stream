const {
  createClient,
  insertData,
  getRange,
  parseMessageResponse
} = require('./redis');

// flush
const TIMESERIES_KEY = 'ts:poprange';

describe('poprange', () => {
  let client;

  const start_ts = 1511885909;
  const samples_count = 50;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  async function get_range(min, max, ...args) {
    return getRange(client, TIMESERIES_KEY, min, max, ...args);
  }

  async function pop_range(min, max, ...args) {
    return client.timeseries(TIMESERIES_KEY, 'poprange', min, max, ...args).then(parseMessageResponse);
  }


  function insert_data(start_ts, samples_count, value) {
    return insertData(client, TIMESERIES_KEY, start_ts, samples_count, value);
  }

  function get_size() {
    return client.timeseries(TIMESERIES_KEY, 'size');
  }

  it('should pop and remove data', async () => {

    const data = [];
    for (let i = 0; i < samples_count; i ++) {
      data.push(i);
    }

    await insert_data(start_ts, data.length, data);

    const mid = data.length / 2;
    const mid_ts = start_ts + mid;
    const end_ts = start_ts + data.length;

    let value = await pop_range(mid_ts, end_ts);
    value = value.map(x => parseInt(x.data.value));
    expect(value.length).toEqual(mid);

    const expected = data.slice(mid);
    expect(value).toEqual(expected);

    const remaining = await get_size();
    expect(remaining).toEqual(data.length / 2);
  });


  it('should support a LIMIT', async () => {
    const data = [];

    for (let i = 0; i < samples_count; i++) {
      data.push( (i + 1) * 5 )
    }

    await insert_data(start_ts, samples_count, data);
    let received = await pop_range(start_ts, start_ts + samples_count, 'LIMIT', 4);
    expect(received.length).toEqual(4);
    const actual = received.map(x => x.data.value);
    actual.forEach((val, i) => {
      expect(val).toEqual(data[i].toString());
    });
  });

  it('should support filters', async () => {

    const data = [
      {
        id: "1",
        name: "april",
        last_name: 'winters',
        rating: "middle"
      },
      {
        id: "2",
        name: "may",
        last_name: 'summer'
      },
      {
        id: "3",
        name: "june",
        last_name: 'spring'
      },
      {
        id: "4",
        name: "april",
        last_name: 'black',
        rating: "high"
      },
      {
        id: "5",
        name: "livia",
        last_name: 'araujo',
        rating: "high"
      },
    ];

    async function checkFilter(op, strFilters, predicate) {
      await insert_data(start_ts, data.length, data);

      if (typeof strFilters === 'string') {
        strFilters = [strFilters]
      }

      const response = await pop_range(start_ts, start_ts + data.length, 'FILTER', ...strFilters);
      const actual = response.map(x => x.data);
      const expected = data.filter(predicate);
      try {
        expect(actual).toEqual(expected);
      }
      catch (e) {
        throw new Error(`Filter returns invalid results for operator "${op}" ${JSON.stringify(strFilters)}`, e);
      }
    }

    await checkFilter('=', 'name=april', (v) => v.name === 'april');
  });

  it('should support LABELS', async () => {

    const data = [
      {
        id: 1,
        name: "april",
        last_name: 'winters',
        rating: "middle"
      },
      {
        id: 2,
        name: "may",
        last_name: 'summer'
      },
      {
        id: 3,
        name: "june",
        last_name: 'spring'
      },
      {
        id: 4,
        name: "april",
        last_name: 'black',
        rating: "high"
      },
      {
        id: 5,
        name: "livia",
        last_name: 'araujo',
        rating: "high"
      },
    ];

    const labels = ['last_name', 'name'];

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const response = await pop_range(start_ts, start_ts + data.length, 'LABELS',  ...labels);
    const actual = response.map(x => x.data);
    const expected = data.map(user => {
      return labels.reduce((res, key) => ({...res, [key]: user[key]}), {});
    });
    expect(actual).toEqual(expected);

  });

  it('should support REDACT', async () => {

    const data = [
      {
        id: 1,
        age: 34,
        name: "april",
        last_name: 'winters',
        income: 56000,
        rating: "middle"
      },
      {
        id: 2,
        age: 23,
        name: "may",
        income: 120000,
        last_name: 'summer'
      },
      {
        id: 3,
        age: 31,
        name: "june",
        income: 30000,
        last_name: 'spring'
      },
      {
        id: 4,
        age: 54,
        name: "april",
        last_name: 'black',
        income: 210000,
        rating: "high"
      },
      {
        id: 5,
        age: 22,
        name: "livia",
        income: 27500,
        last_name: 'araujo',
        rating: "high"
      },
    ];

    const labels = ['age', 'income'];

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const response = await pop_range(start_ts, start_ts + data.length, 'REDACT', ...labels);
    const actual = response.map(x => x.data);
    const expected = data.map(user => {
      const data = {...user, id: user.id.toString()};
      labels.forEach(label => delete data[label]);
      return data;
    });
    expect(actual).toEqual(expected);

  });
});
