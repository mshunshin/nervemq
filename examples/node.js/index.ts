// Minimal NerveMQ example using the official AWS SDK for JavaScript (v3).
//
// Credentials are a NerveMQ API key (the queue is created in the key's
// namespace), minted via the admin UI or the CLI:
//
//   nervemq apikey add --name node-example --namespace <namespace>
//   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... bun index.ts

import {
  SQSClient,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  CreateQueueCommand,
} from "@aws-sdk/client-sqs";

const endpoint = process.env.NERVEMQ_ENDPOINT ?? "http://localhost:8080/api/sqs";
const region = "some-fake-region";

const {
  AWS_ACCESS_KEY_ID: accessKeyId,
  AWS_SECRET_ACCESS_KEY: secretAccessKey,
} = process.env;

if (accessKeyId === undefined || secretAccessKey === undefined) {
  throw new Error(
    "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to a NerveMQ API key " +
      "(create one with: nervemq apikey add --name node-example --namespace <namespace>)",
  );
}

const sqs = new SQSClient({
  endpoint,
  region,
  credentials: { accessKeyId, secretAccessKey },
});

// Get the queue's URL, creating it on first run. The queue lives in the API
// key's namespace.
const url = await sqs
  .send(new GetQueueUrlCommand({ QueueName: "bruh" }))
  .catch(() => sqs.send(new CreateQueueCommand({ QueueName: "bruh" })))
  .then((res) => res.QueueUrl);

console.log(`Queue URL: ${url}`);

const sendResult = await sqs.send(
  new SendMessageCommand({
    QueueUrl: url,
    MessageBody: "Hello World!",
    MessageAttributes: {
      Test: {
        StringValue: "TestString",
        DataType: "String",
      },
    },
  }),
);

console.log(`Message ID: ${sendResult.MessageId}`);

const receiveResult = await sqs.send(
  new ReceiveMessageCommand({
    QueueUrl: url,
    MessageAttributeNames: ["Test"],
  }),
);

console.log(`Messages: ${JSON.stringify(receiveResult.Messages)}`);
