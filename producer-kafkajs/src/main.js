const { Kafka, logLevel, CompressionTypes } = require('kafkajs')

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

const producer = kafka.producer({ groupId: process.env.KAFKA_GROUP_ID })

const main = async () => {

  await producer.connect()
  console.log('------------')

  while (true) {
    await producer.send({
      topic: process.env.KAFKA_TOPIC,
      messages: [
        {
          value: JSON.stringify({
            message: `It's ${new Date().toLocaleString()}`
          })
        }
      ]
    })

    await new Promise((resolve) => {
      setTimeout(resolve, 2000)
    })
  }
}

main();
