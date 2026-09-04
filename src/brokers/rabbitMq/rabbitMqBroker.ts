/**
 * Connect to a RabbitMQ instance.
 */
import { connect, ChannelModel } from 'amqplib';
import { Injectable, Logger } from '@nestjs/common';
import { IConnectionOptions } from '../../interface/IConnectionOptions.js';

@Injectable()
export class RabbitMqBroker {
  private _connection: ChannelModel;
  private readonly logger = new Logger(RabbitMqBroker.name);

  /**
   * Initialize RabbitMQ client
   */
  async connect(connectionOptions: IConnectionOptions): Promise<void> {
    try {
      this._connection = await connect(connectionOptions);
      this.logger.log('Connection successful');
    } catch (e) {
      throw e instanceof Error ? e : new Error(String(e));
    }
  }

  /**
   * Return the RabbitMQ connection.
   */
  get connection() {
    if (!this._connection) {
      throw new Error('Cannot access Broker client before connecting');
    }

    return this._connection;
  }

  async close() {
    if (!this._connection) {
      return;
    }

    await this._connection.close();
    this._connection = undefined;
  }
}

export const rabbitMqPublisher = new RabbitMqBroker();
export const rabbitMqListener = new RabbitMqBroker();
