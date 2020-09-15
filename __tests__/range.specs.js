const {
  createClient,
  insertData,
  getRange,
} = require('./redis');

// flush
const TIMESERIES_KEY = 'ts:range';

describe('range', () => {
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


  function insert_data(start_ts, samples_count, value) {
    return insertData(client, TIMESERIES_KEY, start_ts, samples_count, value);
  }

  it('should support getting all values', async () => {
    const data = [];

    for (let i = 0; i < samples_count; i++) {
      data.push( (i + 1) * 5 )
    }

    await insertData( client, TIMESERIES_KEY , start_ts, samples_count, data);
    let actual = await get_range('-', '+');
    expect(actual.length).toEqual(data.length);
    expect(actual[0][1].value).toEqual(data[0].toString());
    expect(actual[actual.length - 1][1].value).toEqual(data[data.length - 1].toString());
  });

  it('should support a LIMIT', async () => {
    const data = [];

    for (let i = 0; i < samples_count; i++) {
      data.push( (i + 1) * 5 )
    }

    await insert_data(start_ts, samples_count, data);
    let received = await get_range(start_ts, start_ts + samples_count, 'LIMIT', 4);
    expect(received.length).toEqual(4);
    const actual = received.map(x => x[1].value);
    actual.forEach((val, i) => {
      expect(val).toEqual(data[i].toString());
    });
  });

  it('supports special range syntax', async () => {
    const data = [];

    for (let i = 1000; i < 10000; i += 1000) {
      data.push( i )
    }

    await insert_data(start_ts, data.length, data);

    const checkRange = async (min, max, expected) => {
      let range = await get_range(min, max);
      const actual = range.map(x => x[1].value);
      const toCompare = expected.map(x => x.toString());
      try {
        expect(actual).toEqual(toCompare);
      } catch (e) {
        throw new Error(`Failed range query with min = ${min} max = ${max}`);
      }
    };


    await checkRange('-', '+', data);
    await checkRange(start_ts + 2000, '+', data.filter(x => x >= (start_ts + 2000)));
    await checkRange('-', start_ts + 4000, data.filter(x => x < (start_ts + 4000)));
  });

  it('should support filters', async () => {

    const data = [
      {
        id: "1",
        name: "april",
        last_name: 'winters',
        coolness: "middle"
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
        coolness: "high"
      },
      {
        id: "5",
        name: "livia",
        last_name: 'araujo',
        coolness: "high"
      },
    ];

    async function checkFilter(op, strFilters, predicate) {
      await insert_data(start_ts, data.length, data);

      if (typeof strFilters === 'string') {
        strFilters = [strFilters]
      }

      const response = await get_range(start_ts, start_ts + data.length, 'FILTER', ...strFilters);
      const actual = response.map(x => x[1]);
      const expected = data.filter(predicate);
      try {
        expect(actual).toEqual(expected);
      }
      catch (e) {
        throw new Error(`Filter returns invalid results for operator "${op}" ${JSON.stringify(strFilters)}`, e);
      }
    }

    await checkFilter('=', 'name=april', (v) => v.name === 'april');
    await checkFilter('>', 'id>2', (v) => v.id > 2);
  });

  it('should support aggregations', async () => {
    const start_ts = 1488823384;
    const samples_count = 1500;

    await insert_data(start_ts, samples_count, 5);

    const expected = [[1488823000, 116], [1488823500, 500], [1488824000, 500], [1488824500, 384]];
    const response = await get_range(start_ts, start_ts + samples_count, 'AGGREGATION', 500, 'count(value)');
    const actual = response.map(x => [x[0], x[1].value.count]);
    expect(actual).toEqual(expected);
  });

  it('should support LABELS', async () => {

    const data = [
      {
        id: "1",
        name: "april",
        last_name: 'winters',
        coolness: "middle"
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
        coolness: "high"
      },
      {
        id: "5",
        name: "livia",
        last_name: 'araujo',
        coolness: "high"
      },
    ];

    const labels = ['last_name', 'name'];

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const response = await get_range(start_ts, start_ts + data.length, 'LABELS',  ...labels);
    const actual = response.map(x => x[1]);
    const expected = data.map(user => {
      return labels.reduce((res, key) => ({...res, [key]: user[key]}), {});
    });
    expect(actual).toEqual(expected);

  });

  it('should support REDACT', async () => {

    const data = [
      {
        id: "1",
        age: "34",
        name: "april",
        last_name: 'winters',
        income: "56000",
        coolness: "middle"
      },
      {
        id: "2",
        age: "23",
        name: "may",
        income: "120000",
        last_name: 'summer'
      },
      {
        id: "3",
        age: "31",
        name: "june",
        income: "30000",
        last_name: 'spring'
      },
      {
        id: "4",
        age: "54",
        name: "april",
        last_name: 'black',
        income: "210000",
        coolness: "high"
      },
      {
        id: "5",
        age: "22",
        name: "livia",
        income: "27500",
        last_name: 'araujo',
        coolness: "high"
      },
    ];

    const labels = ['age', 'income'];

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const response = await get_range(start_ts, start_ts + data.length, 'REDACT', ...labels);
    const actual = response.map(x => x[1]);
    const expected = data.map(user => {
      const data = {...user, id: user.id.toString()};
      labels.forEach(label => delete data[label]);
      return data;
    });
    expect(actual).toEqual(expected);

  });
});
