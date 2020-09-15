const { createClient, insertData, parseMessageResponse, getRange } = require('./redis');

const TIMESERIES_KEY = 'ts:filter';

describe('filter', () => {
  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  const start_ts = 1488823384;


  async function checkFilter(data, op, strFilters, predicate) {
    const key = `${TIMESERIES_KEY}:${op}`;

    await insertData(client, key, start_ts, data.length, data);

    if (typeof strFilters === 'string') {
      strFilters = [strFilters]
    }

    const min = start_ts;
    const max = start_ts + data.length;
    const response = await getRange(client, key, min, max, 'FILTER', ...strFilters);
    const actual = response.map(x => x[1]);
    const expected = data.filter(predicate);
    try {
      expect(actual).toEqual(expected);
    }
    catch (e) {
      throw new Error(`Filter returns invalid results for operator "${op}" ${JSON.stringify(strFilters)}`);
    }
  }

  async function checkFilterNumeric(data, op, strFilters, predicate) {
    const key = `${TIMESERIES_KEY}:${op}`;

    await insertData(client, key, start_ts, data.length, data);

    if (typeof strFilters === 'string') {
      strFilters = [strFilters]
    }

    const min = start_ts;
    const max = start_ts + data.length;
    const response = await getRange(client, key, min, max, 'FILTER', ...strFilters);
    const actual = response.map(x => x[1].value);
    const expected = data.filter(predicate).map(x => x.toString());
    try {
      expect(actual).toEqual(expected);
    }
    catch (e) {
      throw new Error(`Filter returns invalid results for operator "${op}" ${JSON.stringify(strFilters)}`);
    }
  }


  describe('numeric', () => {

    const data = [];
    for (let i = 0; i < 100; i ++) {
      data.push(i)
    }

    it('Equals', async () => {
      await checkFilterNumeric(data, '=', 'value=10', (v) => v === 10);
    });

    it('Not Equals', async () => {
      await checkFilterNumeric(data,  '!=', 'value!=10', (v) => v !== 10);
    });

    it('Greater Than', async () => {
      await checkFilterNumeric(data,  '>', 'value>50', (v) => v > 50);
    });

    it('Less Than', async () => {
      await checkFilterNumeric(data,  '<', 'value<75', (v) => v < 75);
    });

    test('Greater Or Equal', async () => {
      await checkFilterNumeric(data,  '>=', 'value>=43', (v) => v >= 43);
    });

    test('Less Than Or Equal', async () => {
      await checkFilterNumeric(data,  '<=', 'value<=37', (v) => v <= 37);
    });

    test('Contains', async () => {
      await checkFilterNumeric(data,  'contains', 'value=(1,62,51)', (v) => [1,62,51].includes(v));
    });

    test('Not Contains', async () => {
      await checkFilterNumeric(data,  '!contains', 'value!=(1,62,51)', (v) => ![1, 62, 51].includes(v));
    });

  });

  describe('string', () => {

    const data = [
      {
        id: "1",
        name: "april",
        last_name: 'winters',
        rating: "middle",
      },
      {
        id: "2",
        name: "may",
        last_name: 'summer',
      },
      {
        id: "3",
        name: "june",
        last_name: 'spring',
      },
      {
        id: "4",
        name: "april",
        last_name: 'black',
        rating: "high",
      },
      {
        id: "5",
        name: "livia",
        last_name: 'araujo',
        rating: "high",
      },
    ];

    it('Equals', async () => {
      await checkFilter(data, '=', 'name=april', (v) => v.name === 'april');
    });

    it('Not Equals', async () => {
      await checkFilter(data,  '!=', 'name!=april', (v) => v.name !== 'april');
    });

    it('Greater Than', async () => {
      await checkFilter(data,  '>', 'id>2', (v) => v.id > "2");
    });

    it('Less Than', async () => {
      await checkFilter(data,  '<', 'id<3', (v) => v.id < "3");
    });

    test('Greater Or Equal', async () => {
      await checkFilter(data,  '>=', 'name>=livia', (v) => v.name >= 'livia');
    });

    test('Less Than Or Equal', async () => {
      await checkFilter(data,  '<=', 'last_name<=summer', (v) => v.last_name <= 'summer');
    });

    test('Contains', async () => {
      await checkFilter(data,  'contains', 'id=(1,3,5)', (v) => ["1","3","5"].includes(v.id));
    });

    test('Not Contains', async () => {
      await checkFilter(data,  '!contains', 'last_name!=(black,summer)', (v) => !['black','summer'].includes(v.last_name));
    });

  });

  describe('multiple conditions', () => {
    const data = [
      {
        id: 1,
        name: "april",
        last_name: 'winters',
        rating: "middle",
      },
      {
        id: 2,
        name: "may",
        last_name: 'summer',
      },
      {
        id: 3,
        name: "june",
        last_name: 'spring',
      },
      {
        id: 4,
        name: "april",
        last_name: 'black',
        rating: "high",
      },
      {
        id: 5,
        name: "livia",
        last_name: 'araujo',
        rating: "high",
      },
    ];

    it('should join filter conditions with AND', async () => {
      const key = `${TIMESERIES_KEY}:AND`;
      const min = start_ts;
      const max = start_ts + data.length;

      await insertData(client, key, min, data.length, data);

      const filters = ['name=april', 'AND', 'rating=high'];
      const response = await getRange(client, key, min, max, 'FILTER', ...filters);
      expect(response.length).toEqual(1);
      const actual = response[0][1];
      const expected = {
        ...data[3],
        id: data[3].id + ''
      };
      expect(actual).toEqual(expected);
    });

  });
});
