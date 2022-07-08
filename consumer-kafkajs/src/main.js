const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: [`${process.env.KAFKA_FQDN}:9093`],
  ssl: true,
  sasl: {
    mechanism: process.env.SASL_MECHANISM,
    username: process.env.SASL_USERNAME,
    password: process.env.SASL_PASSWORD,
  },
})

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID })

const main = async () => {

  await consumer.connect()
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC })

  await consumer.run({
    eachMessage: async ({message}) => {
      console.table(JSON.stringify({ value: JSON.parse(message.value.toString()) }))
    }
  })
}

main();
