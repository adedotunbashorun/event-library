import { prefixRoutingKey } from './index.js';
import { Subjects } from '../subjects.js';

describe('prefixRoutingKey', () => {
  it('joins a service prefix with the event subject', () => {
    expect(prefixRoutingKey('identity', Subjects.UserCreated)).toBe(
      'identity.user.created',
    );
  });
});
