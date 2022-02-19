const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['127.0.0.1:9092'],
})

const consumer = kafka.consumer({
    groupId: 'group-json'
})

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: 'results' })
    await consumer.run({
      // eachBatch: async ({ batch }) => {
      //   console.log(batch)
      // },
      eachMessage: async ({ topic, partition, message }) => {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
        console.log(`- ${prefix} ${message.key}#${message.value}`)
      },
    })
    await consumer.disconnect()
  }

const prod = async () => {
    const producer = kafka.producer()

    await producer.connect()
    await producer.send({
        topic: 'letime',
        messages: [
            { value: JSON.stringify({ time: new Date().getTime() }) },
        ],
    }).then(console.log).catch(e => console.error(`[example/producer] ${e.message}`, e))

    await producer.disconnect()
    await run().catch(e => console.error(`[example/consumer] ${e.message}`, e))
    console.log('bye :) ')
    process.exit()
    
}

prod();

