# @adedotunolawale/event-library

Typed RabbitMQ pub/sub for NestJS microservices. Services share one event contract — subjects, publishers, listeners, and routing keys — instead of each team inventing its own AMQP wiring.

```bash
npm install @adedotunolawale/event-library
```

Requires **Node.js 18+** and **NestJS 10+**. This package is ESM (`"type": "module"`).

## Why it exists

In a multi-service NestJS system, “user created” should mean the same routing key and payload everywhere. This library is that shared contract: a publisher and listener bound to the **same topic exchange**, with typed event data.

## Quick start

```ts
import {
  Publisher,
  Listener,
  RabbitMqBroker,
  Subjects,
  IEvent,
  EventEnvelope,
} from '@adedotunolawale/event-library';

interface UserCreatedEvent extends IEvent<{ userId: string; email: string }> {
  subject: Subjects.UserCreated;
}

class UserCreatedPublisher extends Publisher<UserCreatedEvent> {
  subject = Subjects.UserCreated;
}

class UserCreatedListener extends Listener<UserCreatedEvent> {
  subject = Subjects.UserCreated;

  async onMessage(payload: EventEnvelope<UserCreatedEvent['data']>) {
    // payload.data is { userId, email }
  }
}

const broker = new RabbitMqBroker();
await broker.connect({
  protocol: 'amqp',
  hostname: 'localhost',
  username: 'guest',
  password: 'guest',
  vhost: '/',
});

const config = { client: broker, routingKeyPrefix: 'identity' };

await new UserCreatedPublisher(config).publish({
  userId: 'u-1',
  email: 'dev@example.com',
});

await new UserCreatedListener(config).listen();
```

Routing key is `{prefix}.{subject}` — here `identity.user.created`.

## Topology

Publishers and listeners both use the `events` topic exchange by default. Pass `exchangeName` in the config if you already have topology:

```ts
const config = {
  client: broker,
  routingKeyPrefix: 'identity',
  exchangeName: 'global',
};
```

Both sides must use the same exchange, or messages will not be delivered.

## Failure handling

- `publish` throws if the broker rejects the message. Callers can retry or fail the request.
- A listener that throws is **nacked without requeue**, so a poison payload cannot loop forever. Bind a dead-letter exchange on the queue if you need to inspect failures.
- `consume` ignores `null` deliveries (RabbitMQ sends these on cancel).

## Subjects

`Subjects` is a starter set of event names. Add your own string subjects on `IEvent` if this list does not cover your domain.

## Publish envelope

Every message is JSON:

```ts
{
  data: { /* your payload */ },
  auditConfig: { template: null, retentionPeriod: 7 },
  _ctx: {}
}
```

## Development

```bash
npm install
npm test
npm run build
```

## License

MIT
