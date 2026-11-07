// RPC: a request carries `replyTo` and `correlationId`; the server answers there.

import { connect, QueueOptions } from '@firemq/client';

const client = await connect(process.env.FIREMQ_URL ?? 'fire://localhost');
await client.declareQueue('rpc_queue.js');

// The server squares numbers.
const requests = await client.subscribe('rpc_queue.js');
const server = (async () => {
  for await (const req of requests) {
    const n = Number(req.text());
    await client.publish('', req.properties.replyTo, String(n * n), {
      properties: { correlationId: req.properties.correlationId },
    });
    req.ack();
    break;
  }
})();

// The client sends a request and waits for the matching reply.
const { name: replyQueue } = await client.declareQueue('', QueueOptions.temporary());
const answers = await client.subscribe(replyQueue);
await client.publish('', 'rpc_queue.js', '7', {
  properties: { replyTo: replyQueue, correlationId: 'req-1' },
});
for await (const answer of answers) {
  if (answer.properties.correlationId === 'req-1') {
    console.log(` [.] square(7) = ${answer.text()}`);
    answer.ack();
    break;
  }
}
await server;
await client.close();
