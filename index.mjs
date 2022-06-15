import { Kafka } from "kafkajs";
import AWS from "aws-sdk";

const ONE_MINUTE = 60 * 1000;
const ONE_SECOND = 1000;

const { accessKeyId, secretAccessKey, bucketName } = process.env;

const log = (...args) => console.log(new Date().toISOString(), ...args);

const s3 = new AWS.S3({
  accessKeyId,
  secretAccessKey,
});

const clientId = "kafka-s3-consumer";
const localhostBroker = "localhost:9092";
const brokers = [localhostBroker];
const topic = "kafka-to-s3-topic";
const groupId = clientId;

const kafka = new Kafka({ clientId, brokers });
const consumer = kafka.consumer({ groupId });

consumer.connect();
consumer.subscribe({ topic });

function writeToConsole(data, uploadProgressCallBack) {
  log(`Recieved data to write to console, length: ${data.length}`);
  uploadProgressCallBack();
  log(`First 100 chars: ${data.substring(0, 100)}`);
  uploadProgressCallBack();
  log(`Latest 100 chars: ${data.substring(data.length - 100)}`);
}

function writeToS3(data, uploadProgressCallBack) {
  const secondsFromEpoch = Math.floor(Date.now() / 1000);

  const params = {
    Bucket: bucketName,
    Key: `dev-tests/${secondsFromEpoch}.txt`,
    Body: data,
  };

  const request = s3.upload(params);
  request.on("httpUploadProgress", uploadProgressCallBack);

  return request.promise();
}

consumer.run({
  eachBatch: async ({ batch, heartbeat }) => {
    log(`Receiving a batch of ${batch.messages.length} messages`);
    const messages = batch.messages.map((message) => message.value.toString()).join('\n');

    log(`Writing ${messages.length} messages to S3`);
    await writeToS3(messages, heartbeat);
    await writeToConsole(messages, heartbeat);

    log(`Pausing the consumer`);
    consumer.pause([{ topic }]);

    log(`setTimeout to resume the consumer`);
    setTimeout(consumer.resume, 5 * ONE_MINUTE, [{ topic }]);
  },
});

const producer = kafka.producer();
await producer.connect();

const produceMessage = async () => {
  const now = new Date();
  const secondsFromEpoch = Math.floor(now / 1000);
  const message = { key: secondsFromEpoch.toString(),  value: now.toISOString() }
  const messages = [message];
  await producer.send({ topic, messages });
  log(message);
};

setInterval(produceMessage, 10 * ONE_SECOND);

/**
 * Notes
 * 
 * A) Initial messages
 * The initial behavior of kafka consumer is weird, it doesn't send the previous messages before the 1st consumer.
 * There is an option to change that behavior in kafka that is `auto.offset.reset` but it didn't work at my local machine.
 * 
 * B) The putObject doesn't send the httpUploadProgress event until the file is completely uploaded.
 * So it's useless to use it as a progress indicator and heartbeat for kafka.
 * 
 * C) The ManagedUpload didn't work with the provided access key.
 * 
 * D) Using the upload method that is compatible with htttpUploadProgress event and then calling the kafka heartbeat method.
 * 
 * E) Would be good to use the ManagedUpload because it has a better retry mecanism.
 */