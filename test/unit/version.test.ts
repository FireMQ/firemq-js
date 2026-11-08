import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { it } from 'node:test';

import { VERSION } from '../../src/version.js';

it('VERSION matches package.json', () => {
  const pkg = JSON.parse(readFileSync(new URL('../../../package.json', import.meta.url), 'utf8')) as { version: string };
  assert.equal(VERSION, pkg.version);
});
