import { Kafka } from "kafkajs";
import {
  log,
  writeToConsole,
  writeToS3,
  startProducingMessages,
  subscribeToTopicsWithRegex,
  upsertTopics
} from "./helpers.mjs";

const ONE_MINUTE = 60 * 1000;
const ONE_SECOND = 1000;
const TOPIC_PRODUCER_INTERVAL = 10 * ONE_SECOND;
const CONSUMER_BATCH_INTERVAL = 5 * ONE_MINUTE;

const clientId = "kafka-s3-consumer-regex-bmw";
const localhostBroker = "localhost:9092";
const brokers = [localhostBroker];
const groupId = clientId;
const topicRegex = "cars-.*";
const topicList = [`cars-bmw`, `cars-toyota`, `cars-honda`];

const kafka = new Kafka({ clientId, brokers });

const admin = kafka.admin();
await admin.connect();

const producer = kafka.producer();
await producer.connect();

const consumer = kafka.consumer({ groupId });
await consumer.connect();

await upsertTopics(admin, [`cars-bmw`, `cars-toyota`, `cars-honda`]);
await subscribeToTopicsWithRegex(admin, consumer, topicRegex);
await startProducingMessages(admin, producer, topicRegex, TOPIC_PRODUCER_INTERVAL);

consumer.run({
  eachBatch: async ({ batch: { topic, partition, messages }, heartbeat }) => {
    log(
      `Batch of ${messages.length} messages of topic: '${topic}' and partition: '${partition}'`
    );
    const data = messages.map((message) => message.value.toString()).join("\n");

    log(`Writing data with ${data.length} lines to S3`);
    await writeToS3(topic, partition, data, heartbeat);
    await writeToConsole(topic, partition, data, heartbeat);

    log(`Pausing the consumer, topic '${topic}', partition '${partition}'`);
    consumer.pause([{ topic, partition }]);

    log(`setTimeout to resume the consumer, topic '${topic}', partition '${partition}'`);
    log(``);
    setTimeout(consumer.resume, CONSUMER_BATCH_INTERVAL, [{ topic }]);
  },
});
