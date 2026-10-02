// Offline tests exercise both the production merge helper and the actual async page.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const ts = require('typescript');

function loadTs(filename, mocks = {}) {
  const instance = new Module(filename, module);
  instance.filename = filename;
  instance.paths = Module._nodeModulePaths(path.dirname(filename));
  const originalRequire = instance.require.bind(instance);
  instance.require = (name) => Object.hasOwn(mocks, name) ? mocks[name] : originalRequire(name);
  instance._compile(ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
    compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.CommonJS,
      jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
    fileName: filename,
  }).outputText, filename);
  return instance.exports;
}
const helper = loadTs(path.join(__dirname, '../lib/company-news.ts'));
const { mergeCompanyNews: merge } = helper;
const article = (title, ts = '2026-10-02T12:00:00Z', extra = {}) => ({
  title, ts, url: `https://publisher.example/articles/${encodeURIComponent(title)}`, ...extra,
});

test('old nonempty archive cannot hide current core news; history remains available', () => {
  const old = article('Retained August article', '2026-08-17T21:16:00Z', { summary: 'History', s: .2 });
  const fresh = article('Current article', undefined, { source: 'Publisher', provider: 'yahoo_public', s: -.4 });
  const result = merge({ article_count: 360, articles: [old] }, { news: [fresh] });
  assert.deepEqual(result.news.map(x => x.title), ['Current article', 'Retained August article']);
  assert.equal(result.total, 2);
  assert.equal(result.latestArticleAt, '2026-10-02T12:00:00.000Z');
  assert.equal(result.news[0].provider, 'yahoo_public');
  assert.equal(result.news[1].summary, 'History');
});

test('newer archive articles also win; neither source has unconditional priority', () => {
  assert.equal(merge({ articles: [article('New')] }, { news: [article('Old', '2026-08-17')] }).news[0].title, 'New');
});

test('sort and deduplicate before limiting, even when freshest row is last in a large archive', () => {
  const old = Array.from({ length: 200 }, (_, i) => article(`Old ${i}`, '2026-08-17'));
  const result = merge({ articles: [...old, article('Newest')] }, { news: [old[0]] }, 160);
  assert.equal(result.news[0].title, 'Newest');
  assert.equal(result.news.length, 160);
  assert.equal(result.total, 201);
});

test('actual timezone ordering, not lexicographic ordering of input timestamps', () => {
  const result = merge([article('Earlier', '2026-10-02T12:00:00+02:00')], [article('Later', '2026-10-02T11:00:00Z')]);
  assert.equal(result.news[0].title, 'Later');
});

test('legacy headline/date/link fields and offset-less times are UTC', () => {
  const result = merge(null, { news: [{ headline: 'Legacy', date: '2026-10-02 12:00:00', link: 'https://x.example/a' }] });
  assert.equal(result.news[0].ts, '2026-10-02T12:00:00.000Z');
  assert.equal(result.news[0].url, 'https://x.example/a');
});

test('malformed rows, dates and titles cannot displace valid current news', () => {
  const result = merge({ articles: [null, 'junk', {}, article('Bad', 'invalid'), article('Bad day', '2026-02-30'),
    article('Bad time', '2026-10-02T99:00:00Z'), article('   '), { ts: '2026-10-02', title: {} }] }, [article('Valid')]);
  assert.equal(result.total, 1);
  assert.equal(result.news[0].title, 'Valid');
});

test('missing, empty, or malformed feed does not block the other feed', () => {
  for (const missing of [null, undefined, {}, [], { articles: 'broken' }]) {
    assert.equal(merge(missing, [article('A')]).total, 1);
    assert.equal(merge([article('A')], missing).total, 1);
  }
  assert.deepEqual(merge(null, {}), { news: [], total: 0, latestArticleAt: null });
});

test('same normalized headline across providers on the same day is one article', () => {
  const result = merge([article('Company beats estimates!', undefined, { url: 'https://one.example/a' })],
    [article('COMPANY beats estimates', undefined, { url: 'https://two.example/b' })]);
  assert.equal(result.total, 1);
});

test('identical recurring headlines on different days with different URLs stay distinct', () => {
  assert.equal(merge([article('Daily update', '2026-10-01', { url: 'https://x.example/1' })],
    [article('Daily update', '2026-10-02', { url: 'https://x.example/2' })]).total, 2);
});

test('canonical URL dedup removes trackers but keeps article identity query parameters', () => {
  assert.equal(merge([article('Original', undefined, { url: 'https://www.x.example/a/?utm_source=feed#top' })],
    [article('Revised', undefined, { url: 'http://x.example/a?fbclid=tracking' })]).total, 1);
  assert.equal(merge([article('First', undefined, { url: 'https://finnhub.io/api/news?id=1' })],
    [article('Second', undefined, { url: 'https://finnhub.io/api/news?id=2' })]).total, 2);
});

test('homepages are not article identifiers', () => {
  assert.equal(merge([article('First', undefined, { url: 'https://x.example/' })],
    [article('Second', undefined, { url: 'https://x.example/' })]).total, 2);
});

test('transitive headline/URL duplicates collapse to a single group', () => {
  assert.equal(merge([article('A', undefined, { url: 'https://x.example/1' }), article('B', undefined, { url: 'https://x.example/2' })],
    [article('A', undefined, { url: 'https://x.example/2' })]).total, 1);
});

test('duplicate keeps summary and an actual zero score with its probability triple', () => {
  const probs = { pos: .2, neu: .6, neg: .2 };
  const result = merge([article('Same', '2026-10-02T10:00:00Z', { s: 0, sentiment_label: 'Neutral', probs, summary: 'Rich summary' })],
    [article('Same', '2026-10-02T12:00:00Z')]);
  assert.equal(result.total, 1);
  assert.equal(result.news[0].summary, 'Rich summary');
  assert.equal(result.news[0].s, 0);
  assert.deepEqual(result.news[0].probs, probs);
  assert.equal(result.news[0].sentiment_label, 'Neutral');
});

test('a revised headline sharing a URL must not inherit the old headline score', () => {
  const url = 'https://x.example/article';
  const result = merge([article('Old wording', '2026-10-01', { url, s: .9, sentiment_label: 'Positive' })],
    [article('Revised wording', '2026-10-02', { url })]);
  assert.equal(result.news[0].title, 'Revised wording');
  assert.equal(result.news[0].s, null);
  assert.equal(result.news[0].sentiment_label, undefined);
});

test('missing or invalid sentiment stays missing, never neutral zero', () => {
  for (const s of [null, undefined, '', ' ', false, NaN, Infinity, 2]) {
    const row = merge([article('A', undefined, { s, sentiment_label: 'Neutral' })], null).news[0];
    assert.equal(row.s, null);
    assert.equal(row.sentiment_label, undefined);
  }
});

test('non-HTTP links are not published as executable links', () => {
  assert.equal(merge([article('A', undefined, { url: 'javascript:alert(1)' })], null).news[0].url, '');
});

test('inputs are not mutated, including nested scoring fields', () => {
  const input = { articles: [article('A', undefined, { probs: { pos: .6, neu: .3, neg: .1 }, s: .5 })] };
  const before = structuredClone(input);
  merge(input, input).news[0].probs.pos = 0;
  assert.deepEqual(input, before);
});

test('invalid limits are rejected', () => {
  for (const limit of [0, -1, 1.5, NaN]) assert.throws(() => merge(null, null, limit), RangeError);
});

async function renderPage(archive, compact) {
  const element = (type, props) => ({ type, props });
  const page = loadTs(process.env.NEWS_PAGE_SOURCE || path.join(__dirname, '../app/ticker/[symbol]/page.tsx'), {
    'node:fs/promises': { readFile: async (filename) => {
      if (filename.endsWith('/v5/universe.json')) return JSON.stringify({ companies: [{ ticker: 'MSFT' }] });
      if (filename.endsWith('/v5/news/MSFT.json')) return JSON.stringify(archive);
      if (filename.endsWith('/ticker/MSFT.json')) return JSON.stringify(compact);
      throw new Error('Fixture file absent');
    } },
    '../../../components/CompanyVisual': 'CompanyVisual',
    './CompanyDetailTabs': 'CompanyDetailTabs',
    '../../../lib/company-news': helper,
    'react/jsx-runtime': { jsx: element, jsxs: element },
  });
  return page.default({ params: { symbol: 'MSFT' } });
}
function flatten(node) {
  if (Array.isArray(node)) return node.flatMap(flatten);
  if (!node || typeof node !== 'object') return [];
  return [node, ...flatten(node.props?.children)];
}

test('production Page passes merged current news and correct total to CompanyDetailTabs', async () => {
  const page = await renderPage({ article_count: 360, articles: [article('August archive', '2026-08-17')] },
    { news: [article('October core')] });
  const tabs = flatten(page).find(node => node.type === 'CompanyDetailTabs');
  assert.deepEqual(tabs.props.news.map(row => row.title), ['October core', 'August archive']);
  assert.equal(tabs.props.newsTotal, 2);
  assert.ok(JSON.stringify(page).includes('Latest article (UTC): '));
  assert.ok(JSON.stringify(page).includes('2026-10-02'));
});

test('production Page retains available feed when the other is absent', async () => {
  const page = await renderPage(null, { news: [article('Current core')] });
  assert.equal(flatten(page).find(node => node.type === 'CompanyDetailTabs').props.news[0].title, 'Current core');
});
