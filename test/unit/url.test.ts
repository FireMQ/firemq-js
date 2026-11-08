import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { UrlError } from '../../src/errors.js';
import { authority, parseUrl, webSocketUrl } from '../../src/url.js';

describe('parseUrl', () => {
  it('fills in defaults', () => {
    const u = parseUrl('fire://localhost');
    assert.equal(u.transport, 'fire');
    assert.equal(u.host, 'localhost');
    assert.equal(u.port, 5673);
    assert.equal(u.vhost, '/');
    assert.equal(u.user, 'guest');
    assert.equal(u.pass, 'guest');
  });

  it('uses the default port of each scheme', () => {
    assert.equal(parseUrl('fires://h').port, 5674);
    assert.equal(parseUrl('ws://h').port, 15672);
    assert.equal(parseUrl('wss://h').port, 15671);
  });

  it('decodes credentials and vhost', () => {
    const u = parseUrl('fires://app:p%40ss@mq.example.com:7000/prod%2Feu');
    assert.equal(u.transport, 'fires');
    assert.equal(u.host, 'mq.example.com');
    assert.equal(u.port, 7000);
    assert.equal(u.user, 'app');
    assert.equal(u.pass, 'p@ss');
    assert.equal(u.vhost, 'prod/eu');
  });

  it('keeps an explicitly empty password', () => {
    assert.equal(parseUrl('fire://app:@h').pass, '');
    assert.equal(parseUrl('fire://app@h').pass, 'guest');
  });

  it('reads the vhost of a fire URL from the query when the path is empty', () => {
    assert.equal(parseUrl('fire://h?vhost=dev').vhost, 'dev');
    assert.equal(parseUrl('fire://h/').vhost, '/');
    assert.equal(parseUrl('fire://h/%2F').vhost, '/');
  });

  it('uses the query vhost and endpoint path for WebSocket URLs', () => {
    const u = parseUrl('ws://localhost:15672/fire?vhost=dev');
    assert.equal(u.transport, 'ws');
    assert.equal(u.path, '/fire');
    assert.equal(u.vhost, 'dev');
    const w = parseUrl('wss://mq.example.com');
    assert.equal(w.port, 15671);
    assert.equal(w.path, '/fire');
    assert.equal(w.vhost, '/');
    assert.equal(parseUrl('ws://h/custom/path').path, '/custom/path');
  });

  it('keeps explicit web default ports on WebSocket URLs', () => {
    assert.equal(parseUrl('ws://h:80/fire').port, 80);
    assert.equal(parseUrl('wss://h:443').port, 443);
  });

  it('builds WebSocket URLs without credentials', () => {
    assert.equal(webSocketUrl(parseUrl('ws://u:p@h:1234/fire?vhost=dev')), 'ws://h:1234/fire?vhost=dev');
    assert.equal(webSocketUrl(parseUrl('wss://h')), 'wss://h:15671/fire');
  });

  it('brackets IPv6 hosts in the authority', () => {
    const u = parseUrl('fire://[::1]:5673');
    assert.equal(u.host, '::1');
    assert.equal(authority(u), '[::1]:5673');
  });

  it('rejects other schemes and garbage', () => {
    assert.throws(() => parseUrl('amqp://localhost'), UrlError);
    assert.throws(() => parseUrl('localhost:5673'), UrlError);
    assert.throws(() => parseUrl('not a url'), UrlError);
  });
});
