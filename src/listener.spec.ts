import { jest } from '@jest/globals';
import { Listener } from './listener.js';
import { Subjects } from './subjects.js';
import { IEvent } from './interface/IEvent.js';
import { RabbitMqBroker } from './brokers/rabbitMq/rabbitMqBroker.js';

interface UserCreatedEvent extends IEvent {
  subject: Subjects.UserCreated;
  data: { userId: string };
}

class UserCreatedListener extends Listener<UserCreatedEvent> {
  subject = Subjects.UserCreated as const;
  onMessage = jest.fn().mockResolvedValue(undefined);
}

function createChannelMock(overrides: Record<string, jest.Mock> = {}) {
  return {
    prefetch: jest.fn().mockResolvedValue(undefined),
    assertExchange: jest.fn().mockResolvedValue(undefined),
    assertQueue: jest.fn().mockResolvedValue(undefined),
    bindQueue: jest.fn().mockResolvedValue(undefined),
    consume: jest.fn().mockResolvedValue(undefined),
    ack: jest.fn(),
    nack: jest.fn(),
    close: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

function createClient(channel: ReturnType<typeof createChannelMock>): RabbitMqBroker {
  return {
    connection: {
      createChannel: jest.fn().mockResolvedValue(channel),
    },
  } as unknown as RabbitMqBroker;
}

function envelope(userId: string) {
  return {
    data: { userId },
    auditConfig: { template: null, retentionPeriod: 7 },
    _ctx: {},
  };
}

describe('Listener', () => {
  it('binds the queue to the shared events topic exchange', async () => {
    const channel = createChannelMock();
    const listener = new UserCreatedListener({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
    });

    await listener.listen();

    expect(channel.assertExchange).toHaveBeenCalledWith('events', 'topic');
    expect(channel.bindQueue).toHaveBeenCalledWith(
      Subjects.UserCreated,
      'events',
      'identity.user.created',
    );
  });

  it('acks a successful message and ignores a null delivery', async () => {
    const channel = createChannelMock();
    const listener = new UserCreatedListener({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
    });

    await listener.listen();
    const onMessage = channel.consume.mock.calls[0][1];

    await onMessage(null);
    expect(listener.onMessage).not.toHaveBeenCalled();

    const msg = {
      content: Buffer.from(JSON.stringify(envelope('u-1'))),
      properties: { headers: {} },
    };
    await onMessage(msg);

    expect(listener.onMessage).toHaveBeenCalledWith(envelope('u-1'), msg);
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('nacks without requeue when onMessage throws', async () => {
    const channel = createChannelMock();
    const listener = new UserCreatedListener({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
    });
    listener.onMessage.mockRejectedValue(new Error('handler failed'));

    await listener.listen();
    const onMessage = channel.consume.mock.calls[0][1];
    const msg = {
      content: Buffer.from(JSON.stringify(envelope('u-1'))),
      properties: { headers: {} },
    };

    await onMessage(msg);

    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(channel.ack).not.toHaveBeenCalled();
  });

  it('uses a custom exchange when configured', async () => {
    const channel = createChannelMock();
    const listener = new UserCreatedListener({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
      exchangeName: 'global',
    });

    await listener.listen();

    expect(channel.assertExchange).toHaveBeenCalledWith('global', 'topic');
    expect(channel.bindQueue).toHaveBeenCalledWith(
      Subjects.UserCreated,
      'global',
      'identity.user.created',
    );
  });
});
