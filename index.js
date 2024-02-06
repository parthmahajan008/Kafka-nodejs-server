import express from 'express';
import bodyParser from "body-parser"
import { Kafka } from 'kafkajs';
import { ConvexHttpClient } from "convex/browser";
import { api } from "./convex/_generated/api.js";
import { useMutation } from "convex/react";
import * as dotenv from "dotenv";
import MyComponent from './script.js';
dotenv.config({ path: ".env.local" });

const app = express();
app.use(bodyParser.json())

// const client = new ConvexHttpClient(process.env["CONVEX_URL"]);
// export function saveToDb({ task, filepath }) {
//   const mutation = useMutation(api.operations.insertEntry);
//   mutation({ task, filepath })
// };

// KAFKA CONFIG
(async () => {
  console.log("Initializing kafka...");
  const kafka = new Kafka({
    clientId: 'kafka-nodejs-starter',
    brokers: ['localhost:9092'],
  });

  // Initialize the Kafka producer and consumer
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: 'demoTopic-consumerGroup' });


  await producer.connect();
  console.log("Connected to producer.");

  await consumer.connect();
  console.log("Connected to consumer.");

  // KAFKA SUBSCRIPTION
  const subscribeToTopic = async (params) => {
    await consumer.subscribe(params)
  }
  subscribeToTopic({ topic: 'quickstart-events', fromBeginning: true })


  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {


      console.log(
        'Consumed a message = ',
        { topic, partition, value: message.value.toString() }
      )
    },
  });

  // Send an event to the demoTopic topic
  // await producer.send({
  //   topic: 'quickstart-events',
  //   messages: [
  //     { value: 'This event came from another service.' },
  //   ],
  // });


  // Disconnect the producer once we’re done


  app.post('/api/producer-api', async (req, res) => {
    const { messages } = req.body;
    await producer.send({
      topic: 'quickstart-events',
      messages: messages
    }).then(console.log("Produced a message."));

    res.json({ message: 'Hello, World!' });
  });

  const PORT = process.env.PORT || 3001;
  app.listen(PORT, () => {
    console.log(`🎉🎉🎉 Application running on port: ${PORT} 🎉🎉🎉`);
  });
})();
