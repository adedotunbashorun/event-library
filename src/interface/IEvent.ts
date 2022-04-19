import { Subjects } from '../subjects';

export interface IEvent {
  subject: Subjects;
  data: any;
}
