import {Kafka} from 'kafkajs'

const brokers = ['0.0.0.0:9092']

export const topics = ['message-created'] as const; 

const kafka = new Kafka({
    brokers,
    clientId: 'notifications-service'
})

const consumer = kafka.consumer({
    groupId: 'notifications-service'
})

const messageCreatedHandler = async (data)=>{
    console.log('Get a new MSG ', JSON.stringify(data , null , 2))
}

const topicsToSubscribe: Record<typeof topics[number], Function> = {
    'message-created': messageCreatedHandler
}

export const connectToConsumer = async ()=>{
    console.log('Consumer Connected')
    await consumer.connect()

    for(let i = 0; i < topics.length ; ++i){
        await consumer.subscribe({
            topic: topics[i],
            fromBeginning: true,
        })
    }

    await consumer.run({
        eachMessage:async({topic, partition , message})=>{
            if(!message || !message.value){
                return 
            }
            const data = JSON.parse(message.value.toString())

            const handler  = topicsToSubscribe[topic];

            if(handler){
                handler(data)
            }
        }
        
    })
}

export const disconnectFromConsumer = async ()=>{
    console.log('Consumer Disconnected')
    await consumer.disconnect()
}