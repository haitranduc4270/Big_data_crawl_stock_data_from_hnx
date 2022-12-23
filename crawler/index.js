// const { Kafka } = require('kafkajs')

// const kafka = new Kafka({
//   clientId: 'my-app',
//   brokers: ['localhost:9092']
// })

// const producer = kafka.producer()
// const delay = t => new Promise(resolve => setTimeout(resolve, t));

// const run = async () => {
//   // Producing
//   await producer.connect()

//   let i = 0;
//   while(1) {
//     await producer.send({
//       topic: 'test-topic',
//       messages: [
//         { value: `Hello KafkaJS user! ${i++}`},
//       ],
//     })
//     await delay(1000);
//   }

// }

// // run().catch(console.error)

// const crawler = require('./crawler.js');

