import { RabbitMqBroker } from '../brokers/rabbitMq/rabbitMqBroker.js';

export interface IEventHandlerConfig {
  client: RabbitMqBroker;
  routingKeyPrefix: string;
  /**
   * Topic exchange used by both publishers and listeners.
   * Defaults to `events`. Override if you already have topology.
   */
  exchangeName?: string;
}
