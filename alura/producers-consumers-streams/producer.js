const { Kafka } = require("kafkajs");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

const run = async () => {
  await producer.connect();

  for (let i = 0; i < 10; i += 1) {
    const result = await producer.send({
      topic: "ECOMMERCE_NEW_ORDER",
      messages: [{ value: "123,456,789" }],
    });

    console.log(result);

    await sleep(400);
  }
};

run().catch(console.error);
