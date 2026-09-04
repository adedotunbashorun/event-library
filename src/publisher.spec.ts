import { jest } from '@jest/globals';
import { Publisher } from './publisher.js';
import { Subjects } from './subjects.js';
import { IEvent } from './interface/IEvent.js';
import { RabbitMqBroker } from './brokers/rabbitMq/rabbitMqBroker.js';

interface UserCreatedEvent extends IEvent {
  subject: Subjects.UserCreated;
  data: { userId: string };
}

class UserCreatedPublisher extends Publisher<UserCreatedEvent> {
  subject = Subjects.UserCreated as const;
}

function createChannelMock(overrides: Record<string, jest.Mock> = {}) {
  return {
    assertExchange: jest.fn().mockResolvedValue(undefined),
    publish: jest.fn().mockReturnValue(true),
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

describe('Publisher', () => {
  it('publishes to the shared events topic exchange', async () => {
    const channel = createChannelMock();
    const publisher = new UserCreatedPublisher({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
    });

    await publisher.publish({ userId: 'u-1' });

    expect(channel.assertExchange).toHaveBeenCalledWith('events', 'topic');
    expect(channel.publish).toHaveBeenCalledWith(
      'events',
      'identity.user.created',
      expect.any(Buffer),
      expect.objectContaining({ contentType: 'application/json', persistent: true }),
    );
    expect(JSON.parse(channel.publish.mock.calls[0][2].toString())).toEqual({
      data: { userId: 'u-1' },
      auditConfig: { template: null, retentionPeriod: 7 },
      _ctx: {},
    });
  });

  it('rethrows when the broker rejects the publish', async () => {
    const channel = createChannelMock({
      publish: jest.fn().mockImplementation(() => {
        throw new Error('channel closed');
      }),
    });
    const publisher = new UserCreatedPublisher({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
    });

    await expect(publisher.publish({ userId: 'u-1' })).rejects.toThrow(
      'channel closed',
    );
  });

  it('uses a custom exchange when configured', async () => {
    const channel = createChannelMock();
    const publisher = new UserCreatedPublisher({
      client: createClient(channel),
      routingKeyPrefix: 'identity',
      exchangeName: 'global',
    });

    await publisher.publish({ userId: 'u-1' });

    expect(channel.assertExchange).toHaveBeenCalledWith('global', 'topic');
    expect(channel.publish.mock.calls[0][0]).toBe('global');
  });
});
