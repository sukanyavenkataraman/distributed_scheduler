import heapq

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
        
    def request_reservation(self, reservation):
        # Requesting reserver for task reservation, for placement group pg and on osd osd

        #ordered dict's order is acending, but we want the order to be reversed, i.e, highest priprity takes precedence

        if reservation.priority not in self.priorities:
            heapq.heappush(self.priorities, -1.0*reservation.priority)

            self.all_queues[reservation.priority] = [reservation]

        else:
            self.all_queues[reservation.priority].append(reservation)


        self.max_allowed = reservation.pg.primary_osd.max_backfills
        self.min_priority = reservation.pg.primary_osd.min_priority


        if self.type == 'current':
            return self.do_queue_current(reservation)
        
        elif self.type == 'round_robin':
            return self.do_queue_rr(reservation)
        
        elif self.type == 'dynamic_prio':
            return self.do_queue_dp(reservation)
        
        elif self.type == 'osd_max_change':
            return self.do_queue_osdma(reservation)
        else:
            print ('Scheduler type not supported')
            return False
        
    def cancel_reservation(self, reservation, callback=None):

        if reservation in self.in_progress:
            self.in_progress.remove(reservation)

            # If no reservations of priority present, then remove from priorities heap
            if len(self.in_progress[reservation.priority]) == 0:
                self.priorities.remove(-1.0*reservation.priority)

            if callback is None:
                callback = reservation.pg.primary_osd.task_completed
            asyncprocess.finish(reservation, callback=callback)

            if (reservation.priority, reservation) in self.preempt_queue:
                self.preempt_queue[reservation.priority].remove(reservation)

                if len(self.preempt_queue[reservation.priority]) == 0:
                    self.priorities.remove(-1.0*reservation.priority)
                    self.preempt_queue.pop(reservation.priority)

        else:
            self.all_queues[-1.0 * reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)


    def process_callback(self):

    def preempt(self):
        assert(len(self.preempt_priorities) > 0)

        reservation = self.preempt_queue[self.preempt_priorities[0]][0]
        self.cancel_reservation(reservation=reservation, callback=reservation.pg.primary_osd.task_preempted)

    def do_queue_current(self, reservation):

        while ((len(self.preempt_queue) > 0 and len(self.in_progress) > self.max_allowed) or \
            self.preempt_queue[0][0] < self.min_priority):
                self.preempt()

        while len(self.all_queues) > 0:
            curr_reservation = self.all_queues[-1.0*self.priorities[0]][0]

            if len(self.in_progress) > self.max_allowed:
                break

            # Add to in_progress queue
            self.in_progress.append(reservation)

            # Schedule task
            reservation.pg.primary_osd.schedule_task()

            callback = self.cancel_reservation()

            # Call async process
            asyncprocess.finish(reservation, callback=callback)

            # Remove from all_queues
            self.all_queues[-1.0 * reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)

            # Always preempt the least priority reservation first
            # Add to preempt queue
            if reservation.can_preempt:
                if reservation.priority not in self.preempt_queue:
                    heapq.heappush(self.preempt_priorities, reservation.priority)
                    self.preempt_queue[reservation.priority] = [reservation]

                else:
                    self.preempt_queue[reservation.priority].append(reservation)

    def do_queue_rr(self, reservation):

        #TODO: implement round robin
        while ((len(self.preempt_queue) > 0 and len(self.in_progress) > self.max_allowed) or \
               self.preempt_queue[0][0] < self.min_priority):
            self.preempt()

        while len(self.all_queues) > 0:
            curr_reservation = self.all_queues[-1.0 * self.priorities[0]][0]

            if len(self.in_progress) > self.max_allowed:
                break

            # Add to in_progress queue
            self.in_progress.append(reservation)

            callback = reservation.pg.primary_osd.schedule_task

            # Call async process
            asyncprocess.finish(reservation, callback=callback)

            # Remove from all_queues
            self.all_queues[-1.0 * reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)

            # Always preempt the least priority reservation first
            # Add to preempt queue
            if reservation.can_preempt:
                if reservation.priority not in self.preempt_queue:
                    heapq.heappush(self.preempt_priorities, reservation.priority)
                    self.preempt_queue[reservation.priority] = [reservation]

                else:
                    self.preempt_queue[reservation.priority].append(reservation)

    def do_queue_dp(self, reservation):

        #TODO: implement dynamic reservation priority changes
        while ((len(self.preempt_queue) > 0 and len(self.in_progress) > self.max_allowed) or \
               self.preempt_queue[0][0] < self.min_priority):
            self.preempt()

        while len(self.all_queues) > 0:
            curr_reservation = self.all_queues[-1.0 * self.priorities[0]][0]

            if len(self.in_progress) > self.max_allowed:
                break

            # Add to in_progress queue
            self.in_progress.append(reservation)

            callback = reservation.pg.primary_osd.schedule_task

            # Call async process
            asyncprocess.finish(reservation, callback=callback)

            # Remove from all_queues
            self.all_queues[-1.0 * reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)

            # Always preempt the least priority reservation first
            # Add to preempt queue
            if reservation.can_preempt:
                if reservation.priority not in self.preempt_queue:
                    heapq.heappush(self.preempt_priorities, reservation.priority)
                    self.preempt_queue[reservation.priority] = [reservation]

                else:
                    self.preempt_queue[reservation.priority].append(reservation)

    def do_queue_osdma(self, reservation):

        #TODO: implement dynamically changing osd ma
        while ((len(self.preempt_queue) > 0 and len(self.in_progress) > self.max_allowed) or \
               self.preempt_queue[0][0] < self.min_priority):
            self.preempt()

        while len(self.all_queues) > 0:
            curr_reservation = self.all_queues[-1.0 * self.priorities[0]][0]

            if len(self.in_progress) > self.max_allowed:
                break

            # Add to in_progress queue
            self.in_progress.append(reservation)

            callback = reservation.pg.primary_osd.schedule_task

            # Call async process
            asyncprocess.finish(reservation, callback=callback)

            # Remove from all_queues
            self.all_queues[-1.0 * reservation.priority].remove(reservation)

            if len(self.all_queues[reservation.priority]) == 0:
                self.all_queues.pop(reservation.priority)

            # Always preempt the least priority reservation first
            # Add to preempt queue
            if reservation.can_preempt:
                if reservation.priority not in self.preempt_queue:
                    heapq.heappush(self.preempt_priorities, reservation.priority)
                    self.preempt_queue[reservation.priority] = [reservation]

                else:
                    self.preempt_queue[reservation.priority].append(reservation)

    def set_max(self, max):
        # Change osd max_allowed
        self.max_allowed = max

    def set_min(self, min_priority):
        # Change min osd priority
        self.min_priority = min_priority






