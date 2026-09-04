import { Logger } from '@nestjs/common';
import { RabbitMqBroker } from './brokers/rabbitMq/rabbitMqBroker.js';
import { IEvent } from './interface/IEvent.js';
import { IEventHandlerConfig } from './interface/IEventHandlerConfig.js';
import { prefixRoutingKey } from './utils/index.js';
import { DEFAULT_EXCHANGE_NAME, DEFAULT_EXCHANGE_TYPE } from './topology.js';

export abstract class Publisher<T extends IEvent> {
  private readonly logger = new Logger(Publisher.name);

  abstract subject: T['subject'];
  protected client: RabbitMqBroker;
  protected routingKeyPrefix: string;
  protected exchangeName: string;
  /**
   * Retention Period is in days
   */
  protected auditConfig = {
    template: null,
    retentionPeriod: 7,
  };

  constructor(options: IEventHandlerConfig) {
    this.client = options.client;
    this.routingKeyPrefix = options.routingKeyPrefix;
    this.exchangeName = options.exchangeName ?? DEFAULT_EXCHANGE_NAME;
  }

  async publish(data: T['data']): Promise<void> {
    const pubData = { data, auditConfig: this.auditConfig, _ctx: {} };
    const channel = await this.client.connection.createChannel();

    try {
      await channel.assertExchange(this.exchangeName, DEFAULT_EXCHANGE_TYPE);
      const published = channel.publish(
        this.exchangeName,
        prefixRoutingKey(this.routingKeyPrefix, this.subject),
        Buffer.from(JSON.stringify(pubData)),
        { contentType: 'application/json', persistent: true },
      );

      if (!published) {
        throw new Error('Publish failed: broker write buffer is full');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(message);
      throw error;
    } finally {
      await channel.close();
    }
  }
}
