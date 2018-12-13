import heapq
from asyncprocess import schedule_task
from collections import Counter as mset
from threading import Lock

class LocalReserver:
    def __init__(self, type='current', max_replicas=2, max_backfills=2, min_priority=0):
        self.type = type
        self.max_replicas = max_replicas

        self.in_progress = []
        self.preempt_queue = {}
        self.all_queues = {}

        #Priorities are kept in a heap and (priority, reservations) in a map
        self.priorities = []
        self.preempt_priorities = []

        #Default
        self.max_allowed = max_backfills
        self.min_priority = min_priority

        #For alternate algos
        # Max size = max_replicas*max_allowed
        self.mru_osd = []
        self.all_queues_lock = Lock()

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

        self.all_queues_lock.acquire()
        print ('Acquired lock in request reservation')
        if -1.0*reservation.priority not in self.priorities:
            heapq.heappush(self.priorities, -1.0*reservation.priority)

            self.all_queues[reservation.priority] = [reservation]

        else:
            print (reservation.priority)
            self.all_queues[reservation.priority].append(reservation)

        self.all_queues_lock.release()
        print('Released lock in request reservation')

        print('State after requesting reservation')
        self.get_scheduler_state()

        self.do_queue_current()


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
            # Remove from mru_osd
            '''
            print('MRU queue: ', self.mru_osd)
            for replicas in reservation.pg.replica_osd:
                print ('Replica id to be removed from mru queue: ', replicas.id)
                self.mru_osd.remove(replicas.id)
            '''

        else:
            self.all_queues_lock.acquire()
            print('Acquired lock in cancel reservation')
            print ('Reservation ', reservation.id, ' not in progress queue, removing from all queues')
            self.all_queues[reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                print ('No reservations of this priority, so removing')
                self.all_queues.pop(reservation.priority)
                self.priorities.remove(-1.0 * reservation.priority)

            self.all_queues_lock.release()
            print('Released lock in cancel reservation')
        print ('State after cancelling')
        self.get_scheduler_state()

        self.do_queue_current()


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
            if len(self.in_progress) > self.max_allowed:
                break

            reservations = self.all_queues[-1.0*self.priorities[0]]

            if len(reservations) > 1:
                if self.type == 'uniform':
                    reservation = self.get_target_reservation_uniform(reservations)
                elif self.type == 'loaddist':
                    reservation = self.get_target_reservation_loaddist(reservations)
                else:
                    reservation = reservations[0]
            else:
                reservation = reservations[0]

            for replicas in reservation.pg.replica_osd:
                self.mru_osd.append(replicas.id)

            if len(self.mru_osd) > self.max_replicas * self.max_allowed:
                del self.mru_osd[0:len(self.mru_osd) - self.max_replicas * self.max_allowed]

            print ('MRU queue: ', self.mru_osd)

            # Add to in_progress queue
            self.in_progress.append(reservation)

            # Always preempt the least priority reservation first
            # Add to preempt queue
            if reservation.can_preempt:
                print('Can preempt, so adding to preempt queue')
                if reservation.priority not in self.preempt_queue:
                    heapq.heappush(self.preempt_priorities, reservation.priority)
                    self.preempt_queue[reservation.priority] = [reservation]

                else:
                    self.preempt_queue[reservation.priority].append(reservation)

            self.all_queues_lock.acquire()
            print('Acquired lock in do_queue')
            # Remove from all_queues
            self.all_queues[reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)
                self.priorities.remove(-1.0 * reservation.priority)
            self.all_queues_lock.release()

            print('Released lock in do queue')
            # Schedule task
            print ('Going to schedule a task')

            # Call async process
            schedule_task(reservation)


    def get_target_reservation_uniform(self, reservations):

        # First check whether there are reservations whose target replica osds aren't in the most recently used osd list
        # If yes, then grant those in FCFS order. Else, grant reservation in reverse most recently used order
        target_reservation = None
        target_reservation_alt = None
        target_reservation_alt_min = None

        for i in range(len(reservations)):
            if len(set([osd.id for osd in reservations[i].pg.replica_osd]) & set(self.mru_osd)) == 0:
                target_reservation = reservations[i]
                # Found a reservation, so break
                break

            # Alternate reservation is none, so use FCFS
            elif target_reservation_alt is None:
                # Have a list of target reservations, and append according to how recently used the osd was
                target_reservation_alt = reservations[i]
                target_reservation_alt_min = list((mset(self.mru_osd) & mset(
                    [osd.id for osd in target_reservation_alt.pg.replica_osd])).elements())

            # If minimum intersection of current reservation is lesser than or equal to target_reservation_alt
            else:
                intersection = list((mset(self.mru_osd) & mset(
                    [osd.id for osd in target_reservation_alt.pg.replica_osd])).elements())
                if len(intersection) < len(target_reservation_alt_min):
                    target_reservation_alt_min = intersection
                    target_reservation_alt = reservations[i]
                elif len(intersection) == len(target_reservation_alt_min):
                    if intersection == target_reservation_alt_min:
                        # Same, so FCFS
                        break
                    else:
                        # Check how recently used the intersecting osds were
                        for i in range(len(intersection)):
                            # More recently used will be later on in the list
                            if self.mru_osd.index(intersection[i]) < self.mru_osd.index(
                                    target_reservation_alt_min[i]):
                                target_reservation_alt_min = intersection[i]
                                target_reservation_alt = reservations[i]
                                break

        if target_reservation is None:
            target_reservation = target_reservation_alt

        return target_reservation

    def get_target_reservation_loaddist(self, reservations):

        # Get reservation according to avg. target osd usages

        target_reservation = None
        osd_usage_min = 100

        for i in range(len(reservations)):
            avg_osd_usage = sum([osd.get_current_usage() for osd in reservations[i].pg.replica_osd])/len(reservations[i].pg.replica_osd)

            if osd_usage_min > avg_osd_usage:
                target_reservation = reservations[i]
                osd_usage_min = avg_osd_usage

        return target_reservation

    def set_max(self, max):
        # Change osd max_allowed
        self.max_allowed = max

    def set_min(self, min_priority):
        # Change min osd priority
        self.min_priority = min_priority


class RemoteReserver:
    def __init__(self, type='current', max_backfills=2, min_priority=0):
        self.type = type

        self.in_progress = []
        self.preempt_queue = {}
        self.all_queues = {}

        # Priorities are kept in a heap and (priority, reservations) in a map
        self.priorities = []
        self.preempt_priorities = []

        # Default
        self.max_allowed = max_backfills
        self.min_priority = min_priority

        # For alternate algos
        # Max size = max_allowed
        self.mru_osd = []
        self.all_queues_lock = Lock()

    def get_scheduler_state(self):
        print('Allqueues: ', self.all_queues)
        print('In progress queue: ', self.in_progress)
        print('Preempt queue: ', self.preempt_queue)
        print('All priorities: ', self.priorities)
        print('Preempt priorities: ', self.preempt_priorities)

    def request_reservation(self, reservation):
        # Requesting reserver for task reservation, for placement group pg and on osd osd

        # ordered dict's order is acending, but we want the order to be reversed, i.e, highest priprity takes precedence

        print('State before requesting reservation')
        self.get_scheduler_state()

        self.all_queues_lock.acquire()
        print('Acquired lock in release reservation remote')
        if -1.0 * reservation.priority not in self.priorities:
            heapq.heappush(self.priorities, -1.0 * reservation.priority)

            self.all_queues[reservation.priority] = [reservation]

        else:
            print(reservation.priority)
            self.all_queues[reservation.priority].append(reservation)

        self.all_queues_lock.release()
        print('Released lock in release reservation remote')

        self.max_allowed = reservation.osd.max_backfills
        self.min_priority = reservation.osd.min_priority

        print('State after requesting reservation')
        self.get_scheduler_state()

        self.do_queue_current()

    def cancel_reservation(self, reservation):

        print('Going to cancel reservation ', reservation.id)
        self.get_scheduler_state()

        if reservation in self.in_progress:
            print('Removing reservation ', reservation.id, ' from in progress queue')
            self.in_progress.remove(reservation)

            if reservation.priority in self.preempt_queue:
                print('Can preempt so removing from preempt queue')
                self.preempt_queue[reservation.priority].remove(reservation)

                if len(self.preempt_queue[reservation.priority]) == 0:
                    print('Length is 0 so going to delete')
                    self.preempt_priorities.remove(reservation.priority)
                    del self.preempt_queue[reservation.priority]

            #self.mru_osd.remove(reservation.pg.primary_osd)

        else:
            self.all_queues_lock.acquire()
            print('Acquired lock in cancel reservation remote')
            print('Reservation ', reservation.id, ' not in progress queue, removing from all queues')
            self.all_queues[reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                print('No reservations of this priority, so removing')
                self.all_queues.pop(reservation.priority)
                self.priorities.remove(-1.0 * reservation.priority)
            self.all_queues_lock.release()
            print('Released lock in release reservation remote')

        print('State after cancelling')
        self.get_scheduler_state()

        self.do_queue_current()

    def preempt(self):
        assert (len(self.preempt_priorities) > 0)

        reservation = self.preempt_queue[self.preempt_priorities[0]][0]
        print('Preempting reservation')

        print('Current state')
        self.get_scheduler_state()

        # Call state change to preempted
        reservation.osd.task_preempted(reservation)

        print('State after preemption')
        self.get_scheduler_state()

    def do_queue_current(self):

        print('Inside do queue. Current state: ')
        self.get_scheduler_state()

        # TODO: Preempt when higher priority process comes always
        while ((len(self.preempt_queue) > 0 and len(self.in_progress) > self.max_allowed) or \
               ((len(self.preempt_queue) > 0 and self.preempt_priorities[0] < self.min_priority))):
            print('Going to preempt reservation ', self.preempt_queue, self.preempt_priorities)
            self.preempt()

        while len(self.all_queues) > 0:
            if len(self.in_progress) > self.max_allowed:
                break

            reservations = self.all_queues[-1.0 * self.priorities[0]]

            if len(reservations) > 1:
                if self.type == 'distributed':
                    reservation = self.get_target_reservation_dist(reservations)
                elif self.type == 'same_as_prev':
                    reservation = self.get_target_reservation_prev(reservations)
                else:
                    reservation = reservations[0]
            else:
                reservation = reservations[0]

            self.mru_osd.append(reservation.pg.primary_osd)

            if len(self.mru_osd) > self.max_allowed:
                del self.mru_osd[0:len(self.mru_osd)-self.max_allowed]

            # Add to in_progress queue
            self.in_progress.append(reservation)

            self.all_queues_lock.acquire()
            print('Acquired lock in do queue remote')
            # Remove from all_queues
            self.all_queues[reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)
                self.priorities.remove(-1.0 * reservation.priority)
            print('Released lock in do queue remote')
            self.all_queues_lock.release()

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
            print('Going to schedule a task')

            # Call async process
            schedule_task(reservation)


    def get_target_reservation_dist(self, reservations):

        target_reservation = None

        for i in range(len(reservations)):
            if reservations[i].pg.primary_osd not in self.mru_osd:
                target_reservation = reservations[i]
                break

        if target_reservation is None:
            target_reservation = reservations[0]

        return target_reservation

    def get_target_reservation_prev(self, reservations):
        target_reservation = None
        max_index = -1

        for i in range(len(reservations)):
            if reservations[i].pg.primary_osd in self.mru_osd:
                if self.mru_osd.index(reservations[i].pg.primary_osd) >= max_index:
                    target_reservation = reservations[i]

        if target_reservation is None:
            target_reservation = reservations[0]

        return target_reservation

    def set_max(self, max):
        # Change osd max_allowed
        self.max_allowed = max

    def set_min(self, min_priority):
        # Change min osd priority
        self.min_priority = min_priority









