import express from "express";
import bodyParser from "body-parser";
import { Kafka } from "kafkajs";

const app = express();
app.use(bodyParser.json())(
  // KAFKA CONFIG
  async () => {
    console.log("Initializing kafka...");
    const kafka = new Kafka({
      clientId: "kafka-nodejs-starter",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({ groupId: "demoTopic-consumerGroup" });

    await consumer
      .connect()
      .then(() => console.warn("🚨🚨🚨Connection to consumer is ready"));

    // KAFKA SUBSCRIPTION
    const subscribeToTopic = async (params) => {
      await consumer.subscribe(params);
    };
    subscribeToTopic({ topic: "quickstart-events", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log("Consumed a message = ", {
          topic,
          partition,
          value: message.value.toString(),
        });
      },
    });

    const PORT = process.env.PORT || 3001;
    app.listen(PORT, () => {
      console.log(`🎉🎉🎉 Application running on port: ${PORT} 🎉🎉🎉`);
    });
  }
)();
