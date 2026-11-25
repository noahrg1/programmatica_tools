// Vercel serverless function for the Studio Brief feed checker.
// GET /api/feed?url=<feed link>
// Streams the whole feed server-side (so size isn't limited by the browser or by Vercel's
// 4.5 MB response cap), counts the products, and returns a small JSON summary plus the
// first ~1.5 MB as a sample for the browser to inspect.
const dns = require('dns').promises;
const net = require('net');
const zlib = require('zlib');
const { Readable } = require('stream');

const SAMPLE_BYTES = 1.5 * 1024 * 1024;
const TIME_BUDGET_MS = +process.env.FEED_TIME_BUDGET_MS || 150000;   // stop counting after 2.5 min and report a minimum
const MAX_REDIRECTS = 5;

function isPrivate(ip) {
  if (net.isIPv6(ip)) {
    const l = ip.toLowerCase();
    const v4 = l.match(/::ffff:(\d+\.\d+\.\d+\.\d+)$/);
    if (v4) return isPrivate(v4[1]);
    return l === '::1' || l === '::' || l.startsWith('fc') || l.startsWith('fd') || l.startsWith('fe80');
  }
  const [a, b] = ip.split('.').map(Number);
  return a === 0 || a === 10 || a === 127 || a >= 224 || (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127);
}

async function checkHost(u) {
  if (!/^https?:$/.test(u.protocol)) throw new Error('Only http and https links can be checked.');
  if (process.env.FEED_ALLOW_PRIVATE === '1') return;
  const addrs = await dns.lookup(u.hostname, { all: true }).catch(() => { throw new Error("That domain doesn't resolve."); });
  if (addrs.some(a => isPrivate(a.address))) throw new Error('That address is on a private network.');
}

// Most likely product element in an XML sample: item / entry, else the most common
// element (other than the root) that has child elements.
function pickItemTag(xml) {
  if (/<item[\s>]/.test(xml)) return 'item';
  if (/<entry[\s>]/.test(xml)) return 'entry';
  const re = /<!\[CDATA\[[\s\S]*?\]\]>|<!--[\s\S]*?-->|<[?!][^>]*>|<(\/?)([A-Za-z_][\w:.-]*)[^>]*?(\/?)>/g;
  const stack = [], counts = {}, parents = new Set();
  let m;
  while ((m = re.exec(xml))) {
    if (!m[2]) continue;
    if (m[1]) { const i = stack.lastIndexOf(m[2]); if (i >= 0) stack.length = i; continue; }
    if (stack.length) parents.add(stack[stack.length - 1]);
    if (stack.length >= 1) counts[m[2]] = (counts[m[2]] || 0) + 1;
    if (!m[3]) stack.push(m[2]);
  }
  const best = Object.entries(counts).filter(([k]) => parents.has(k)).sort((a, b) => b[1] - a[1])[0];
  return best ? best[0] : null;
}

function countMatches(str, re) { re.lastIndex = 0; let n = 0; while (re.exec(str)) n++; return n; }

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const site = req.headers['sec-fetch-site'];
  if (site && site !== 'same-origin') return res.status(403).json({ error: 'Only the tools site can use this.' });

  let u;
  try { u = new URL(String(req.query.url || '')); } catch (e) { return res.status(400).json({ error: "That isn't a valid link." }); }

  const started = Date.now();
  const ctrl = new AbortController();
  try {
    let r;
    for (let i = 0; ; i++) {
      await checkHost(u);
      r = await fetch(u, { redirect: 'manual', signal: ctrl.signal,
        headers: { 'User-Agent': 'ProgrammaticaFeedCheck/1.0', 'Accept': '*/*' } });
      if (r.status >= 300 && r.status < 400 && r.headers.get('location')) {
        if (i >= MAX_REDIRECTS) throw new Error('Too many redirects.');
        u = new URL(r.headers.get('location'), u);
        continue;
      }
      break;
    }
    if (!r.ok) return res.status(502).json({ error: 'The feed link returned ' + r.status + '.' });
    const totalBytes = +r.headers.get('content-length') || null;

    // Raw byte stream, counting network bytes; gunzip .gz files on the fly.
    const it = Readable.fromWeb(r.body)[Symbol.asyncIterator]();
    let netBytes = 0;
    const first = await it.next();
    const raw = Readable.from((async function* () {
      if (first.done) return;
      netBytes += first.value.length; yield Buffer.from(first.value);
      for (;;) { const n = await it.next(); if (n.done) return; netBytes += n.value.length; yield Buffer.from(n.value); }
    })());
    const gz = !first.done && first.value[0] === 0x1f && first.value[1] === 0x8b;
    const stream = gz ? raw.pipe(zlib.createGunzip()) : raw;

    const sampleParts = []; let sampleLen = 0, kind = null, itemTag = null, itemRe = null;
    let count = 0, tail = '', inQuote = false, newlines = 0, lastByte = 10, complete = true, bytes = 0;

    const startCounting = () => {   // called once the sample is full or the stream ended
      const sample = Buffer.concat(sampleParts).toString('latin1');
      const c = sample.replace(/^\uFEFF|^\xEF\xBB\xBF/, '').trimStart()[0];
      kind = c === '<' ? 'xml' : (c === '{' || c === '[') ? 'json' : 'csv';
      if (kind === 'xml') {
        itemTag = pickItemTag(sample);
        if (itemTag) {
          itemRe = new RegExp('<' + itemTag.replace(/[.:-]/g, '\\$&') + '[\\s>/]', 'g');
          count = countMatches(sample, itemRe);
          tail = sample.slice(-(itemTag.length + 1));
        }
      } else if (kind === 'csv') sampleParts.forEach(scanCsv);
    };
    const scanCsv = buf => {
      for (let i = 0; i < buf.length; i++) {
        const b = buf[i];
        if (b === 34) inQuote = !inQuote;
        else if (b === 10 && !inQuote) newlines++;
      }
      if (buf.length) lastByte = buf[buf.length - 1];
    };

    for await (const chunk of stream) {
      bytes += chunk.length;
      if (kind === null) {
        sampleParts.push(chunk); sampleLen += chunk.length;
        if (sampleLen >= SAMPLE_BYTES) startCounting();
      } else if (kind === 'xml' && itemRe) {
        const s = tail + chunk.toString('latin1');
        count += countMatches(s, itemRe);
        tail = s.slice(-(itemTag.length + 1));
      } else if (kind === 'csv') scanCsv(chunk);
      if (Date.now() - started > TIME_BUDGET_MS) { complete = false; stream.destroy(); raw.destroy(); ctrl.abort(); break; }
    }
    if (kind === null) startCounting();

    let items = null;
    if (kind === 'xml' && itemTag) items = count;
    if (kind === 'csv') items = Math.max(0, newlines + (lastByte !== 10 ? 1 : 0) - 1);

    const sampleBuf = Buffer.concat(sampleParts);
    return res.status(200).json({
      kind, itemTag, items, complete,
      sampleCut: sampleBuf.length > SAMPLE_BYTES || !complete || sampleBuf.length < bytes,
      sample: sampleBuf.subarray(0, Math.min(sampleBuf.length, SAMPLE_BYTES)).toString('utf8'),
      netBytesRead: netBytes, totalBytes, decompressedBytes: bytes, gzip: gz,
      seconds: Math.round((Date.now() - started) / 100) / 10
    });
  } catch (e) {
    return res.status(400).json({ error: e.name === 'AbortError' ? 'The feed took too long to respond.' : e.message });
  }
};

// Allow up to 5 minutes (the Hobby maximum on Fluid compute).
module.exports.config = { maxDuration: 300 };
