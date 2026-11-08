// Removes build output directories. Usage: node scripts/clean.mjs dir...
import { rmSync } from 'node:fs';

for (const dir of process.argv.slice(2)) {
  rmSync(dir, { recursive: true, force: true });
}
