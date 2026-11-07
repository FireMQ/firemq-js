// Publish/subscribe: a fanout exchange copies every message to every bound queue.

import { connect, QueueOptions } from '@firemq/client';

const client = await connect(process.env.FIREMQ_URL ?? 'fire://localhost');
await client.declareExchange('logs.js', 'fanout');

// Two subscribers, each with its own temporary queue.
const queues = [];
for (let i = 0; i < 2; i++) {
  const { name } = await client.declareQueue('', QueueOptions.temporary());
  await client.bindQueue(name, 'logs.js');
  queues.push(name);
}

await client.publish('logs.js', '', 'info: hello everyone');

for (const [i, queue] of queues.entries()) {
  const sub = await client.subscribe(queue, { noAck: true });
  const msg = await sub.receive(5000);
  console.log(` [subscriber ${i + 1}] ${msg?.text()}`);
  await sub.cancel();
}
await client.close();
