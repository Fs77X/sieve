var kafka = require('kafka-node')
Producer = kafka.Producer
Consumer = kafka.Consumer
// ConsumerGroup = kafka.ConsumerGroup
client = new kafka.KafkaClient()
const prod = async () => {
    producer = new Producer(client)
    payloads = [
        { topic: 'letime', messages: JSON.stringify({ time: new Date().getTime() }) }
    ];
    const producerPromise = new Promise((resolve, reject) => {
      producer.on('ready', () => {
        producer.send(payloads, (err, data) => {
            console.log(data);
            resolve(data);
        });
      });
      producer.on('error', (err) =>{ 
        console.log(err);
        reject(err);
      })
    })
    await producerPromise;
    const closePromise = new Promise((resolve, reject) => {
        producer.close(() => {
            console.log('done prod')
            resolve('done prod')
        })
    })
    await closePromise
   
  }

   
const consom = async () => {
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    consumer = new Consumer(
        client,
        [
            { topic: 'results', partition: 0 }
        ],
        {
            autoCommit: false
        }
    );
    const consumerPromise = new Promise((resolve, reject) => {
        consumer.on('message', async function (message) {
            resolve(JSON.parse(message.value));
        });
    })
    await consumerPromise;
    consumerPromise.then((data) => console.log(data))
    const consoomClose = new Promise((resolve, reject) => {
        consumer.close(true, () => resolve("done okchamp"))

    })
    await consoomClose
    consoomClose.then((str) => console.log(str))

}

const main = async () => {
    await prod()
    await consom()
}
main()