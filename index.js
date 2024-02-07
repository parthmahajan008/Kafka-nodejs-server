import express from "express";
import bodyParser from "body-parser";
import { Kafka } from "kafkajs";
import fs from 'fs';

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3001;

// KAFKA CONFIG
(async () => {
  console.log("Initializing kafka...");
  const kafka = new Kafka({
    clientId: "kafka-nodejs-starter",
    brokers: ["localhost:9092"],
  });

  const consumer = kafka.consumer({ groupId: "demoTopic-consumerGroup" });

  await consumer.connect();
  console.warn("🚨🚨🚨Connection to consumer is ready");

  // KAFKA SUBSCRIPTION
  const subscribeToTopic = async (params) => {
    await consumer.subscribe(params);
  };
  subscribeToTopic({ topic: "quickstart-events", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("Consumed a message =", {
        // topic,
        // partition,
        // value: message.value.toString(),
      });

      try {
        const fileData = JSON.parse(message.value.toString());
        const { name, type, data } = fileData;

        // Decode base64 data and save the file to disk
        const decodedData = Buffer.from(data, 'base64');
        console.log(decodedData)
        const filePath = `/uploads`;

        fs.writeFileSync(filePath, decodedData);

        console.log(`File "${name}" saved successfully.`);
      } catch (error) {
        console.error("Error processing file:", error);
      }
    },
  });

  app.listen(PORT, () => {
    console.log(`🎉🎉🎉 Application running on port: ${PORT} 🎉🎉🎉`);
  });
})();
