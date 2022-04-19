import { Subjects } from '../subjects';

export const prefixRoutingKey = (prefix: string, subject: Subjects) => {
  return `${prefix}.${subject}`;
};
