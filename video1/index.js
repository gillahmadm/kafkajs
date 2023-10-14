const express = require('express');
const app = express();
const port = 3000; // Port number for the server
const { Kafka } = require('kafkajs')
const TOPIC = "TOPIC__2"
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
})
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });
const createTopicWithPart = async () => {
    const admin = kafka.admin()
    await admin.connect();
    // const topicConfig = {
    //     topic: TOPIC,
    //     numPartitions: 5, // default patition count
    //     replicationFactor: 1, // replication factor for this topic
    //   }
    //   await admin.createTopics({
    //     topics: [topicConfig],
    //   })
    const topics = await admin.listTopics();


    await admin.disconnect();
}
// createTopicWithPart().then(()=>{
//     console.log("==========")
// })
const consumerSetup = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
                topic: topic,
                partition: partition
            })
        },
    })
    await producer.connect()
    await producer.send({
        topic: TOPIC,
        messages: [
            {
                value: 'Hello KafkaJS user! 3',
                key: "FILL"
            },
            {
                value: 'Heldasdasdasdasasdasdasdadadsadasdasdasuser! 3',
                key: "FILL"
            },
            {
                value: 'Hello KafkaJS user! 3',
                key: "FILL"
            },
            {
                value: 'Heldasdasdasdasasdasdasdadadsadasdasdasuser! 3',
                key: "FILL"
            },
            {
                value: 'Hello KafkaJS user!',
                key: "0"
            },
            {
                value: 'Hello KafkaJS userds!',
                key: "5sweds"
            },
            {
                value: 'Hello KafkaJS userds!',
                key: "6ew"
            },
            {
                value: 'Hello KafkaJS usedsdqdqdr!',
                key: "0"
            },
            {
                value: 'Hello KafkaJqwdeqeS user!',
                key: "5sweds"
            },
            {
                value: 'Hello KafkweqaJS user!',
                key: "6eweqeq"
            },
        ],
    })
}
consumerSetup().catch(() => { console.log("Got error in setup consumer") })
// Define a route for the root URL

app.get('/hello', async (req, res) => {
    await producer.connect()
    await producer.send({
        topic: TOPIC,
        messages: [
            { value: 'Hello KafkaJS user!' },
        ],
    })
    res.send('Hello, World!');
});

// Start the server
app.listen(port, () => {
    console.log(`Server is listening at http://localhost:${port}`);
});