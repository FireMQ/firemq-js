// Routing: a direct exchange delivers by exact routing key.

import { connect, QueueOptions } from '@firemq/client';

const client = await connect(process.env.FIREMQ_URL ?? 'fire://localhost');
await client.declareExchange('direct_logs.js', 'direct');

const { name: queue } = await client.declareQueue('', QueueOptions.temporary());
for (const severity of ['warning', 'error']) {
  await client.bindQueue(queue, 'direct_logs.js', severity);
}

for (const severity of ['info', 'warning', 'error']) {
  await client.publish('direct_logs.js', severity, `${severity}: disk almost full`);
}

// Only warning and error were routed to the queue.
const sub = await client.subscribe(queue, { noAck: true });
for (let i = 0; i < 2; i++) {
  const msg = await sub.receive(5000);
  console.log(` [x] ${msg?.routingKey}: ${msg?.text()}`);
}
await client.close();
