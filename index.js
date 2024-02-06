const express = require('express');
const bodyParser = require('body-parser');
const app = express();
app.use(bodyParser.json())
const { Kafka } = require('kafkajs');
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
  await consumer.subscribe({ topic: 'quickstart-events', fromBeginning: true });
  // console.log("Consumer subscribed to topic = quickstart-events");

  // Log every message consumed
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
