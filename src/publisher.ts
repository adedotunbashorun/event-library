import { Logger } from '@nestjs/common';
import { RabbitMqBroker } from './brokers/rabbitMq/rabbitMqBroker';
import { IEvent } from './interface/IEvent';
import { IEventHandlerConfig } from './interface/IEventHandlerConfig';
import { prefixRoutingKey } from './utils';

export abstract class Publisher<T extends IEvent> {
  private readonly logger = new Logger(Publisher.name);

  abstract subject: T['subject'];
  protected client: RabbitMqBroker;
  protected routingKeyPrefix: string;
  /**
   * Retention Period is in days
   */
  protected auditConfig = {
    template: null,
    retentionPeriod: 7,
  };

  constructor(options: IEventHandlerConfig) {
    const { client, routingKeyPrefix } = options;
    this.client = client;
    this.routingKeyPrefix = routingKeyPrefix;
  }

  async publish(data: T['data']['data']): Promise<void> {
    // add ctx to message
    let _ctx = {};

    const pubData = { data, auditConfig: this.auditConfig, _ctx };
    const exchangeName = 'global';
    const exchangeType = 'topic';

    const channel = await this.client.connection.createChannel();

    try {
      await channel.assertExchange(exchangeName, exchangeType);
      await channel.publish(
        exchangeName,
        prefixRoutingKey(this.routingKeyPrefix, this.subject),
        Buffer.from(JSON.stringify(pubData)),
        { contentType: 'application/json', persistent: true },
      );
    } catch (error) {
      this.logger.log(error.message);
    } finally {
      channel.close();
    }
  }
}
