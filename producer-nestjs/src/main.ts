import { NestFactory } from '@nestjs/core';
import { Interval } from '@nestjs/schedule';
import { Module, Inject, Injectable, Controller, Get } from '@nestjs/common';
import {
  ClientsModule,
  ClientKafka,
  KafkaOptions,
  Transport,
} from '@nestjs/microservices';
import { SASLOptions } from 'kafkajs';

export const KAFKA_CLIENT_CONFIG: KafkaOptions = {
  transport: Transport.KAFKA,
  options: {
    client: {
      clientId: process.env.KAFKA_CLIENT_ID,
      brokers: [`${process.env.KAFKA_FQDN}:9093`],
      ssl: true,
      sasl: {
        mechanism: process.env.SASL_MECHANISM,
        username: process.env.SASL_USERNAME,
        password: process.env.SASL_PASSWORD,
      } as SASLOptions,
    }
  },
};

@Injectable()
export class ProducerService {
  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  @Interval(2000)
  produceMessage(): void {
    console.log("emit a new message")
    this.kafkaClient.emit(process.env.KAFKA_TOPIC, {
      message: 'Hello World!',
      createdAt: new Date().toISOString(),
    });
  }
}


@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        ...KAFKA_CLIENT_CONFIG,
      },
    ]),
  ],
  controllers: [],
  providers: [ProducerService],
})
export class AppModule {}

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const port = process.env.PORT ?? 80;

  const kafka = await NestFactory.createMicroservice(AppModule, KAFKA_CLIENT_CONFIG);
  await kafka.listen();

  await app.listen(port);
  console.log(`app running at ${await app.getUrl()}`);
}

bootstrap();
