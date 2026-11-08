// Topics: route with word patterns; `*` matches one word, `#` matches zero or more.

import { connect, QueueOptions } from '@firemq/client';

const client = await connect(process.env.FIREMQ_URL ?? 'fire://localhost');
await client.declareExchange('topic_logs.js', 'topic');

const { name: queue } = await client.declareQueue('', QueueOptions.temporary());
await client.bindQueue(queue, 'topic_logs.js', '*.critical');

for (const key of ['kern.critical', 'kern.info', 'app.critical']) {
  await client.publish('topic_logs.js', key, key);
}

const sub = await client.subscribe(queue);
let n = 0;
for await (const msg of sub) {
  console.log(` [x] ${msg.text()}`);
  msg.ack();
  if (++n === 2) break;
}
await client.close();
