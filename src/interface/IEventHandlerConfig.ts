import { RabbitMqBroker } from '../brokers/rabbitMq/rabbitMqBroker';

export interface IEventHandlerConfig {
  client: RabbitMqBroker;
  routingKeyPrefix: string;
}
