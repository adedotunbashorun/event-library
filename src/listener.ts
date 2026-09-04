import { Logger } from '@nestjs/common';

import { RabbitMqBroker } from './brokers/rabbitMq/rabbitMqBroker.js';
import { prefixRoutingKey } from './utils/index.js';
import { IEvent, EventEnvelope } from './interface/IEvent.js';
import { IEventHandlerConfig } from './interface/IEventHandlerConfig.js';
import { DEFAULT_EXCHANGE_NAME, DEFAULT_EXCHANGE_TYPE } from './topology.js';

export abstract class Listener<T extends IEvent> {
  private readonly logger = new Logger(Listener.name);

  abstract subject: T['subject'];
  abstract onMessage(data: EventEnvelope<T['data']>, msg?: unknown): Promise<void>;
  protected client: RabbitMqBroker;
  protected routingKeyPrefix: string;
  protected exchangeName: string;
  protected queue: string;

  constructor(options: IEventHandlerConfig) {
    this.client = options.client;
    this.routingKeyPrefix = options.routingKeyPrefix;
    this.exchangeName = options.exchangeName ?? DEFAULT_EXCHANGE_NAME;
  }

  async listen() {
    const resolvedQueue = this.queue || this.subject;
    const channel = await this.client.connection.createChannel();

    try {
      await channel.prefetch(10);
      await channel.assertExchange(this.exchangeName, DEFAULT_EXCHANGE_TYPE);
      await channel.assertQueue(resolvedQueue);
      await channel.bindQueue(
        resolvedQueue,
        this.exchangeName,
        prefixRoutingKey(this.routingKeyPrefix, this.subject),
      );

      await channel.consume(
        resolvedQueue,
        async (msg) => {
          if (!msg) {
            return;
          }

          try {
            const data = JSON.parse(msg.content.toString()) as EventEnvelope<
              T['data']
            >;
            await this.onMessage(data, msg);
            channel.ack(msg);
          } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            const stack = e instanceof Error ? e.stack : undefined;
            this.logger.error(`Consumer processing error - ${message}`, stack);
            channel.nack(msg, false, false);
          }
        },
        {
          noAck: false,
        },
      );
    } catch (e) {
      await channel.close();
      throw e instanceof Error ? e : new Error(String(e));
    }
  }
}
