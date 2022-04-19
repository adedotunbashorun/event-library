import { Logger } from '@nestjs/common';

import { RabbitMqBroker } from './brokers/rabbitMq/rabbitMqBroker';
import { prefixRoutingKey } from './utils/index';
import { IEvent } from './interface/IEvent';
import { IEventHandlerConfig } from './interface/IEventHandlerConfig';
import { RequestContext } from '@indicina1/observability-nodejs';

export abstract class Listener<T extends IEvent> {
  private readonly logger = new Logger(Listener.name);

  abstract subject: T['subject'];
  abstract onMessage(data: T['data'], msg?: any): Promise<void>;
  protected client: RabbitMqBroker;
  protected routingKeyPrefix: string;
  protected queue: string;

  constructor(options: IEventHandlerConfig) {
    const { client, routingKeyPrefix } = options;
    this.client = client;
    this.routingKeyPrefix = routingKeyPrefix;
  }

  async listen() {
    const resolvedQueue = this.queue || this.subject;
    const exchangeName = 'incoming';
    const exchangeType = 'topic';

    const channel = await this.client.connection.createChannel();

    try {
      await channel.prefetch(10);
      await channel.assertExchange(exchangeName, exchangeType);
      await channel.assertQueue(resolvedQueue);
      await channel.bindQueue(
        resolvedQueue,
        exchangeName,
        prefixRoutingKey(this.routingKeyPrefix, this.subject),
      );

      await channel.consume(
        resolvedQueue,
        async (msg) => {
          const messageBody = msg.content.toString();
          const data = JSON.parse(messageBody);

          try {
            // build context from message
            const ctx = data._ctx || {};
            RequestContext.start(ctx);

            // handle message
            await this.onMessage(data, msg);
            channel.ack(msg);
          } catch (e) {
            this.logger.error(`Consumer processing error - ${e.message}`, e);
            channel.nack(msg, false, true);
          }
        },
        {
          noAck: false,
        },
      );
    } catch (e) {
      channel.close();
      throw new Error(e);
    }
  }
}
