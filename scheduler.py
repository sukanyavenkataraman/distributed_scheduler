import heapq
from asyncprocess import schedule_task

class Scheduler:
    def __init__(self, type='current'):
        self.type = type
        
        self.in_progress = []
        self.preempt_queue = {}
        self.all_queues = {}

        #Priorities are kept in a heap and (priority, reservations) in a map
        self.priorities = []
        self.preempt_priorities = []

        #Default
        self.max_allowed = 10
        self.min_priority = 0


    def get_scheduler_state(self):
        print ('Allqueues: ', self.all_queues)
        print ('In progress queue: ', self.in_progress)
        print ('Preempt queue: ', self.preempt_queue)
        print ('All priorities: ', self.priorities)
        print ('Preempt priorities: ', self.preempt_priorities)

    def request_reservation(self, reservation):
        # Requesting reserver for task reservation, for placement group pg and on osd osd

        #ordered dict's order is acending, but we want the order to be reversed, i.e, highest priprity takes precedence

        print ('State before requesting reservation')
        self.get_scheduler_state()

        if -1.0*reservation.priority not in self.priorities:
            heapq.heappush(self.priorities, -1.0*reservation.priority)

            self.all_queues[reservation.priority] = [reservation]

        else:
            print (reservation.priority)
            self.all_queues[reservation.priority].append(reservation)

        self.max_allowed = reservation.osd.max_backfills
        self.min_priority = reservation.osd.min_priority

        print('State after requesting reservation')
        self.get_scheduler_state()

        if self.type == 'current':
            return self.do_queue_current()
        
        else:
            print ('Scheduler type not supported')
            return False


    def cancel_reservation(self, reservation):

        print ('Going to cancel reservation ', reservation.id)
        self.get_scheduler_state()

        if reservation in self.in_progress:
            print ('Removing reservation ', reservation.id, ' from in progress queue')
            self.in_progress.remove(reservation)

            if reservation.priority in self.preempt_queue:
                print ('Can preempt so removing from preempt queue')
                self.preempt_queue[reservation.priority].remove(reservation)

                if len(self.preempt_queue[reservation.priority]) == 0:
                    print ('Length is 0 so going to delete')
                    self.preempt_priorities.remove(reservation.priority)
                    #self.preempt_queue.pop(reservation.priority)
                    del self.preempt_queue[reservation.priority]

        else:
            print ('Reservation ', reservation.id, ' not in progress queue, removing from all queues')
            self.all_queues[reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                print ('No reservations of this priority, so removing')
                self.all_queues.pop(reservation.priority)
                self.priorities.remove(-1.0 * reservation.priority)

        print ('State after cancelling')
        self.get_scheduler_state()


    def preempt(self):
        assert(len(self.preempt_priorities) > 0)

        reservation = self.preempt_queue[self.preempt_priorities[0]][0]
        print ('Preempting reservation')

        print ('Current state')
        self.get_scheduler_state()

        #Call state change to preempted
        reservation.osd.task_preempted(reservation)

        print ('State after preemption')
        self.get_scheduler_state()

    def do_queue_current(self):

        print ('Inside do queue. Current state: ')
        self.get_scheduler_state()

        #TODO: Preempt when higher priority process comes always
        while ((len(self.preempt_queue) > 0 and len(self.in_progress) > self.max_allowed) or \
               ((len(self.preempt_queue) > 0 and self.preempt_priorities[0] < self.min_priority))):
                print ('Going to preempt reservation ', self.preempt_queue, self.preempt_priorities)
                self.preempt()

        while len(self.all_queues) > 0:
            reservation = self.all_queues[-1.0*self.priorities[0]][0]

            if len(self.in_progress) > self.max_allowed:
                break

            # Add to in_progress queue
            self.in_progress.append(reservation)

            # Remove from all_queues
            self.all_queues[reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)
                self.priorities.remove(-1.0 * reservation.priority)

            # Always preempt the least priority reservation first
            # Add to preempt queue
            if reservation.can_preempt:
                print('Can preempt, so adding to preempt queue')
                if reservation.priority not in self.preempt_queue:
                    heapq.heappush(self.preempt_priorities, reservation.priority)
                    self.preempt_queue[reservation.priority] = [reservation]

                else:
                    self.preempt_queue[reservation.priority].append(reservation)

            # Schedule task
            print ('Going to schedule a task')
            callback = reservation.osd.task_completed

            # Call async process
            schedule_task(reservation)

    def set_max(self, max):
        # Change osd max_allowed
        self.max_allowed = max

    def set_min(self, min_priority):
        # Change min osd priority
        self.min_priority = min_priority






