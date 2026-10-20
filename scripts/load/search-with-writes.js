import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const SIZE = Number(__ENV.SIZE || 10000);
const CAPACITY = Number(__ENV.CAPACITY || SIZE);
const DURATION = __ENV.DURATION || '60s';
const READERS = (__ENV.READ_URLS || '').split(',').filter(Boolean);
const HEADERS = { 'Content-Type': 'application/json',
  ...(__ENV.TOKEN ? { Authorization: `Bearer ${__ENV.TOKEN}` } : {}) };
const SPEC = { specification: 'loadtest-students', schemaVersion: 1 };
const CITIES = ['Oslo', 'Bergen', 'Trondheim', 'Stavanger', 'Tromsø', 'Kristiansand', 'Drammen', 'Fredrikstad', 'Bodø', 'Ålesund'];

export const options = {
  scenarios: {
    searchers: { executor: 'constant-vus', vus: Number(__ENV.SEARCHERS || 3), duration: DURATION, exec: 'search' },
    writer: { executor: 'constant-arrival-rate', rate: Number(__ENV.WRITES_PER_SECOND || 10), timeUnit: '1s',
      duration: DURATION, preAllocatedVUs: 1, exec: 'write' },
  },
  thresholds: { checks: ['rate>0.99'], 'http_req_duration{scenario:writer}': ['p(95)<1000'],
    'http_req_duration{scenario:searchers}': ['p(95)<100'] },
};

function root(id, name) {
  return { id, name, sex: id % 2 === 0 ? 'female' : 'male', age: 18 + id % 40,
    address: { city: CITIES[id % CITIES.length], country: 'Norway' },
    courses: [{ code: `CS${id % 50}`, title: 'Introduction to computing', year: 2020 + id % 6 }] };
}

function post(path, body, base = BASE) { return http.post(`${base}${path}`, JSON.stringify(body), { headers: HEADERS }); }

export function setup() {
  const schema = post('/cache/schemas', {
    id: SPEC.specification, version: 1, identity: '/id', capacity: CAPACITY, ttl: 'infinite',
    dimensions: [
      { name: 'name', path: '/name', index: 'prefix' },
      { name: 'course', path: '/courses/*/code', index: 'exact' },
      { name: 'cityAge', index: 'compound', parts: [{ path: '/address/city', index: 'exact' }, { path: '/age', index: 'range' }] },
    ],
    schema: { type: 'object', required: ['id', 'name', 'sex'], properties: {
      id: { type: 'integer' }, name: { type: 'string' }, sex: { type: 'string' }, age: { type: 'integer' },
      address: { type: 'object', properties: { city: { type: 'string' }, country: { type: 'string' } } },
      courses: { type: 'array', items: { type: 'object', properties: {
        code: { type: 'string' }, title: { type: 'string' }, year: { type: 'integer' } } } } } },
  });
  check(schema, { 'schema published': r => r.status === 201 });
  for (let start = 0; start < SIZE; start += 1000) {
    const batch = [];
    for (let id = start; id < Math.min(start + 1000, SIZE); id++) batch.push({ ...SPEC, revision: 1, value: root(id, `Name${id}`) });
    const response = post('/cache/entries/batch', batch);
    check(response, { 'batch stored': r => r.status === 200 && r.json().every(item => item.status === 200) });
  }
}

export function search() {
  const draw = Math.floor(Math.random() * 4);
  if (draw === 3) {
    const found = post('/cache/entries/get', { ...SPEC, identity: Math.floor(Math.random() * SIZE) });
    check(found, { 'get answered': r => r.status === 200 || r.status === 404 });
    return;
  }
  const age = 18 + Math.floor(Math.random() * 30);
  const query = draw === 0 ? { ...SPEC, dimension: 'name', value: `Name${Math.floor(Math.random() * SIZE / 10)}` }
    : draw === 1 ? { ...SPEC, dimension: 'cityAge', value: [CITIES[Math.floor(Math.random() * CITIES.length)]], from: age, to: age + 1, limit: 100 }
    : { ...SPEC, dimension: 'course', value: `CS${Math.floor(Math.random() * 50)}`, limit: 100, fields: ['/id'] };
  check(post('/cache/search', query), { 'search answered': r => r.status === 200 });
}

let written = 0;
export function write() {
  const id = Math.floor(Math.random() * SIZE);
  const revision = Date.now() * 1000 + (written++ % 1000);
  const name = `Renamed${id}-${revision}`;
  const stored = post('/cache/entries', { ...SPEC, revision, value: root(id, name) });
  check(stored, { 'write acknowledged': r => r.status === 200 });
  if (stored.status !== 200) return;
  for (const reader of READERS) {
    check(post('/cache/entries/get', { ...SPEC, identity: id }, reader), {
      'acknowledged write visible on every node': r => r.status === 0 || r.status === 503 || (r.status === 200 && r.json().name === name),
    });
  }
}
