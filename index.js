import express from "express";
import bodyParser from "body-parser";
import { Kafka } from "kafkajs";
import fs from 'fs';
import path from 'path';

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3001;

// Ensure the 'uploads' directory exists
const currentModuleDir = path.dirname(new URL(import.meta.url).pathname);
const uploadsDirectory = path.join(currentModuleDir, 'uploads');

if (!fs.existsSync(uploadsDirectory)) {
  fs.mkdirSync(uploadsDirectory);
}

// KAFKA CONFIG
(async () => {
  console.debug("Initializing kafka...");
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
      console.debug("Consumed a message");

      try {
        const fileData = JSON.parse(message.value.toString());
        const { name, type, data } = fileData;

        // Decode base64 data and save the file to disk
        const decodedData = Buffer.from(data, 'base64');
        const filePath = path.join(uploadsDirectory, name);

        fs.writeFileSync(filePath, decodedData);

        console.info(`File "${name}" saved successfully.`);
      } catch (error) {
        console.error("Error processing file:", error);
      }
    },
  });

  app.listen(PORT, () => {
    console.debug(`🎉🎉🎉 Application running on port: ${PORT} 🎉🎉🎉`);
  });
})();
