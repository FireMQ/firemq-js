// Work queues: share tasks between workers, one unacknowledged task per worker at a time.

import { connect } from '@firemq/client';

const client = await connect(process.env.FIREMQ_URL ?? 'fire://localhost');
await client.declareQueue('task_queue.js');

for (let n = 1; n <= 3; n++) {
  // Persistent by default; resolves once the broker has written the task to disk.
  await client.publish('', 'task_queue.js', `task ${n}`);
}

// prefetch 1: the broker sends the next task only after this one is acknowledged.
const tasks = await client.subscribe('task_queue.js', { prefetch: 1 });
let done = 0;
for await (const task of tasks) {
  console.log(` [x] Done ${task.text()}`);
  task.ack();
  if (++done === 3) break;
}
await client.close();
