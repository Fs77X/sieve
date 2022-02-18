const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['127.0.0.1:9092'],
})

const prod = async () => {
    const producer = kafka.producer()

    await producer.connect()
    await producer.send({
        topic: 'letime',
        messages: [
            { value: JSON.stringify({time: new Date().getTime()}) },
        ],
    }).then(console.log).catch(e => console.error(`[example/producer] ${e.message}`, e))

    await producer.disconnect()
}

prod();
