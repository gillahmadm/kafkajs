const express = require('express');
const app = express();
const port = 3000; // Port number for the server
const { Kafka } = require('kafkajs');
// Define a route for the root URL
const kafka = new Kafka({
    clientId: "test app",
    brokers: ['localhost:9092'],
  })
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });
async function runConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic:"Test", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message on topic ${topic}, partition ${partition}: ${message.value}`);
        },
    });
}

runConsumer().catch(console.error);




app.get('/hello', async (req, res) => {
 console.log("Hello")
 await producer.connect();
  producer.send({
    topic: "Test",
    messages: [{ value: JSON.stringify({
        a : 1,
        b : 2
    })  }],
  });
  res.send('Hello, World!');
});

// Start the server
app.listen(port, () => {
  console.log(`Server is listening at http://localhost:${port}`);
});
