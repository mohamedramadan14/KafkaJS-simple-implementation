import { Kafka } from 'kafkajs'

const brokers = ['0.0.0.0:9092']

const kafka = new Kafka({
    clientId: 'messages-service',
    brokers
})

const producer = kafka.producer()


export const connectProducer = async () => {
    await producer.connect()
    console.log('Producer Connected')
}

export const disconnectFromProducer = async () => {
    await producer.disconnect()
    console.log('Producer Disconnecting.....')
}

export const topics = ['message-created'] as const;

export const sendMessage = async (topic: typeof topics[number], message: any) => {
    return producer.send({
        topic,
        messages: [
            { value: message }
        ]
    })
}