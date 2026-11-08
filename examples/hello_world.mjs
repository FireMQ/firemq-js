// Hello world: publish one message to a queue and consume it.
//
//   FIREMQ_URL=fire://localhost node examples/hello_world.mjs

import { connect } from '@firemq/client';

const client = await connect(process.env.FIREMQ_URL ?? 'fire://localhost');
await client.declareQueue('hello.js');

// Resolves once the broker has the message.
await client.publish('', 'hello.js', 'Hello World!');
console.log(" [x] Sent 'Hello World!'");

const sub = await client.subscribe('hello.js');
for await (const msg of sub) {
  console.log(` [x] Received '${msg.text()}'`);
  msg.ack();
  break; // leaving the loop cancels the subscription
}
await client.close();
