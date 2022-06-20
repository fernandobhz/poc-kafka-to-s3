import AWS from "aws-sdk";

export const log = (...args) => console.log(new Date().toISOString(), ...args);

export function writeToConsole(topic, partition, data, uploadProgressCallBack) {
  log(
    `Console, topic: '${topic}', partition: '${partition}', length: '${data.length}'`
  );
  uploadProgressCallBack();
  log(``);
}

export function writeToS3(topic, partition, data, uploadProgressCallBack) {
  const { accessKeyId, secretAccessKey, bucketName } = process.env;
  const secondsFromEpoch = Math.floor(Date.now() / 1000);

  const params = {
    Bucket: bucketName,
    Key: `kafka-to-s3-uploads/${topic}/${partition}/${secondsFromEpoch}.txt`,
    Body: data,
  };

  const s3 = new AWS.S3({
    accessKeyId,
    secretAccessKey,
  });

  const request = s3.upload(params);
  request.on("httpUploadProgress", uploadProgressCallBack);

  return request.promise();
}

export const produceMessage = async (producer, topic) => {
  const now = new Date();
  const secondsFromEpoch = Math.floor(now / 1000);
  const message = {
    key: secondsFromEpoch.toString(),
    value: `${topic} ${now.toISOString()}`,
  };
  const messages = [message];
  await producer.send({ topic, messages });
  log(topic, message);
  log(``);
};

export const startProducingMessages = async (
  admin,
  producer,
  topicRegex,
  interval
) => {
  const allKafkaTopics = await admin.listTopics();
  const filteredTopics = allKafkaTopics.filter((topic) =>
    topic.match(topicRegex)
  );

  filteredTopics.forEach((topic) => {
    setInterval(produceMessage, interval, producer, topic);
  });
};

export const subscribeToTopicsWithRegex = async (
  admin,
  consumer,
  topicRegex
) => {
  const allKafkaTopics = await admin.listTopics();
  const filteredTopics = allKafkaTopics.filter((topic) =>
    topic.match(topicRegex)
  );

  filteredTopics.forEach((topic) => {
    consumer.subscribe({ topic });
  });
};

export const upsertTopics = async (admin, topics) => {
  const allTopicsArray = await admin.listTopics();

  const allTopicsMap = allTopicsArray.reduce(
    (previousValue, currentValue, currentIndex, array) => ({
      ...previousValue,
      [currentValue]: currentIndex,
    }),
    {}
  );

  const missingTopics = [...topics].filter(
    (topic) => !!allTopicsMap[topic] === false
  );

  const createTopicParams = {
    topics: missingTopics.map((topic) => ({ topic })),
  };

  await admin.createTopics(createTopicParams);
};
