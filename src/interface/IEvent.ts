import { Subjects } from '../subjects.js';

export interface EventEnvelope<TData = unknown> {
  data: TData;
  auditConfig: {
    template: unknown;
    retentionPeriod: number;
  };
  _ctx: Record<string, unknown>;
}

export interface IEvent<TData = unknown> {
  subject: Subjects | string;
  data: TData;
}
