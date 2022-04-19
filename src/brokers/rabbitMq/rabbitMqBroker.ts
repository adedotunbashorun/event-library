/**
 * Connect to a rabbitMq instance
 *
 * @todo create custom decorator for this.
 * @todo abstract this implementation
 */
import * as amqpConn from 'amqplib';
import { Injectable, Logger } from '@nestjs/common';
import { IConnectionOptions } from '../../interface/IConnectionOptions';

@Injectable()
export class RabbitMqBroker {
  private _connection: amqpConn.Connection;
  private readonly logger = new Logger(RabbitMqBroker.name);

  /**
   * Initialize rabbitMq client
   */
  async connect(connectionOptions: IConnectionOptions): Promise<void> {
    try {
      this._connection = await amqpConn.connect(connectionOptions);
      this.logger.log('Connection successful');
    } catch (e) {
      throw new Error(e);
    }
  }

  /**
   * Return the rabbitMq connection.
   */
  get connection() {
    if (!this._connection) {
      throw new Error('Cannot access Broker client before connecting');
    }

    return this._connection;
  }

  async close() {
    this._connection.close();
  }
}

export const rabbitMqPublisher = new RabbitMqBroker();
export const rabbitMqListener = new RabbitMqBroker();
