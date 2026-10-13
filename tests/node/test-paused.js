const WebSocket = require('ws');
const assert = require('node:assert/strict');

const URL = process.env.TRACKER_URL || 'wss://localhost:8000';
const timeoutMs = 5000;

function createClient() {
  const ws = new WebSocket(URL, { rejectUnauthorized: false });
  const messages = [];
  ws.on('message', raw => {
    try {
      messages.push(JSON.parse(raw.toString()));
    } catch {
      // The test waits for protocol messages and will time out if one is missing.
    }
  });
  return { ws, messages };
}

function waitFor(predicate, description) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (predicate()) {
        clearInterval(timer);
        resolve();
      } else if (Date.now() - started >= timeoutMs) {
        clearInterval(timer);
        reject(new Error('Timed out waiting for ' + description));
      }
    }, 20);
  });
}

function send(client, message) {
  client.ws.send(JSON.stringify(message));
}

function waitForOpen(client) {
  if (client.ws.readyState === WebSocket.OPEN) return Promise.resolve();
  return new Promise((resolve, reject) => {
    client.ws.once('open', resolve);
    client.ws.once('error', reject);
  });
}

function announceReplies(client, infoHash) {
  return client.messages.filter(
    message => message.action === 'announce' && message.info_hash === infoHash
  );
}

async function main() {
  const seed = createClient();
  const partial = createClient();
  const observer = createClient();
  const invalid = createClient();
  const infoHash = 'paused-test-hash-001';
  const seedId = 'paused-test-seed-0001';
  const partialId = 'paused-test-partial-01';

  try {
    await Promise.all([waitForOpen(seed), waitForOpen(partial), waitForOpen(observer)]);

    // Register a complete seed in the swarm first.
    send(seed, {
      action: 'announce',
      event: 'started',
      info_hash: infoHash,
      peer_id: seedId,
      left: 0,
    });
    await waitFor(() => announceReplies(seed, infoHash).length > 0, 'seed announce reply');

    // A partial seed may use paused for its very first announce.
    send(partial, {
      action: 'announce',
      event: 'paused',
      info_hash: infoHash,
      peer_id: partialId,
      left: 1024,
      numwant: 10,
      offers: [{
        offer_id: 'offer-paused-first',
        offer: { type: 'offer', sdp: 'paused-first-sdp' },
      }],
    });

    await waitFor(() => announceReplies(partial, infoHash).length > 0, 'first paused announce reply');
    await waitFor(
      () => seed.messages.some(message => message.offer_id === 'offer-paused-first'),
      'offer from first paused announce'
    );

    let reply = announceReplies(partial, infoHash).at(-1);
    assert.equal(reply.complete, 1, 'the complete seed should remain complete');
    assert.equal(reply.incomplete, 1, 'the paused partial seed should be counted as incomplete');
    assert.equal(seed.messages.find(message => message.offer_id === 'offer-paused-first').peer_id, partialId);
    assert.equal(partial.ws.readyState, WebSocket.OPEN, 'a first paused announce must keep the connection open');

    // Repeated paused announces must refresh the same membership, not duplicate it.
    const beforeRepeat = announceReplies(partial, infoHash).length;
    send(partial, {
      action: 'announce',
      event: 'paused',
      info_hash: infoHash,
      peer_id: partialId,
      left: 1024,
      numwant: 10,
      offers: [],
    });
    await waitFor(
      () => announceReplies(partial, infoHash).length > beforeRepeat,
      'repeated paused announce reply'
    );

    reply = announceReplies(partial, infoHash).at(-1);
    assert.equal(reply.complete, 1);
    assert.equal(reply.incomplete, 1, 'repeated paused announces must not duplicate swarm membership');
    assert.equal(partial.ws.readyState, WebSocket.OPEN, 'repeated paused announces must keep the connection open');

    // Scrape should report the partial seed as incomplete too.
    send(observer, { action: 'scrape', info_hash: infoHash });
    await waitFor(
      () => observer.messages.some(message => message.action === 'scrape' && message.files && message.files[infoHash]),
      'scrape response'
    );
    const scrape = observer.messages.find(
      message => message.action === 'scrape' && message.files && message.files[infoHash]
    );
    assert.equal(scrape.files[infoHash].complete, 1);
    assert.equal(scrape.files[infoHash].incomplete, 1);

    // An announce carrying an answer is routed as an answer even if event is paused.
    send(partial, {
      action: 'announce',
      event: 'paused',
      info_hash: infoHash,
      peer_id: partialId,
      to_peer_id: seedId,
      offer_id: 'offer-paused-answer',
      answer: { type: 'answer', sdp: 'answer-to-seed' },
    });
    await waitFor(
      () => seed.messages.some(message => message.answer && message.answer.sdp === 'answer-to-seed'),
      'answer carried by a paused announce'
    );
    const answer = seed.messages.find(message => message.answer && message.answer.sdp === 'answer-to-seed');
    assert.equal(answer.peer_id, partialId);
    assert.equal(answer.to_peer_id, undefined);

    // Paused is now supported; unrelated unknown events must remain rejected.
    await waitForOpen(invalid);
    const invalidClosed = new Promise(resolve => invalid.ws.once('close', resolve));
    send(invalid, {
      action: 'announce',
      event: 'update',
      info_hash: infoHash,
      peer_id: 'paused-test-invalid-01',
      left: 1,
    });
    await Promise.race([
      invalidClosed,
      new Promise((_, reject) => setTimeout(
        () => reject(new Error('Unknown announce event did not close the connection')),
        timeoutMs
      )),
    ]);

    console.log('PASS: first/repeated paused announces, incomplete counts, offer forwarding, answer routing, and unknown-event rejection');
  } finally {
    for (const client of [seed, partial, observer, invalid]) {
      if (client.ws.readyState === WebSocket.OPEN || client.ws.readyState === WebSocket.CONNECTING) {
        client.ws.terminate();
      }
    }
  }
}

main().catch(error => {
  console.error('FAIL:', error.message);
  process.exitCode = 1;
});
