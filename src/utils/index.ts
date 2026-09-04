export const prefixRoutingKey = (prefix: string, subject: string) => {
  return `${prefix}.${subject}`;
};
