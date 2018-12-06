
# Priorities
OSD_RECOVERY_PRIORITY_MIN=0
OSD_BACKFILL_PRIORITY_BASE=100

class Reservation:
    '''
    Reservation class to define reservations
    A reservation is defined by an id, and has the following characteristics -
    1. Type that determines its priority
    2. Placement group that is making the reservation
    3. Time taken to complete this task (approx.)
    '''
    def __init__(self, id, pg, can_preempt=True, type='degraded', time=10):
        self.id = id
        self.pg = pg
        self.type = 'degraded'
        self.can_preempt = can_preempt

        if type == 'degraded':
            self.priority = OSD_RECOVERY_PRIORITY_MIN
        elif type == 'backfill':
            self.priority = OSD_BACKFILL_PRIORITY_BASE
        else:
            self.priority = OSD_RECOVERY_PRIORITY_MIN

        self.time = time
        self.state = 'Initiated'

    def get_task_state(self):
        return self.state

    def on_state_change(self):
        state = self.get_task_state()
        if state == 'Completed':
            return True
        elif state == 'Preempted':
            return False
        else:
            print ('Called at the wrong time')
            return False


class OSD:
    '''
    Assume that an OSD can perform a max of max_tasks (foreground and background)
    Usage is computed as a percentage of this max_tasks
    '''
    def __init__(self, id, max_tasks=100, max_backfills=3, min_priority=100):
        self.id = id

        self.max_backfills = max_backfills
        self.min_priority = min_priority

        self.num_tasks = 0
        self.max_tasks = max_tasks

        self.current_tasks = []

    def __repr__(self):
        print ('OSD number: ', self.id)
        print ('Max allowed backfills: ', self.max_backfills)
        print ('Min priority: ', self.min_priority)
        print ('Number of current scheduled tasks: ', self.num_tasks)
        print ('Maximum allowed tasks: ', self.max_tasks)
        print ('Current usage: ', self.get_current_usage())

    def get_current_usage(self):
        return 100.0*self.num_tasks/self.max_tasks

    def schedule_task(self, task):
        self.num_tasks += 1
        self.current_tasks.append(task)
        task.state = 'In progress'

    def task_completed(self, task):
        self.num_tasks -= 1
        self.current_tasks.remove(task)
        task.state = 'Completed'
        task.on_state_changed()

    def task_preempted(self, task):
        self.num_tasks -= 1
        self.current_tasks.remove(task)
        task.state = 'Preempted'
        task.on_state_changed()


class PG:
    def __init__(self, id, primary, replicas):
        '''
        Placement group class that keeps track of where (which OSD) its primary is and where its replicas are
        :param id: float
        :param primary: OSD
        :param replicas: List of OSDs
        '''
        self.id = id
        self.primary_osd = primary
        self.replica_osd = replicas

    def __repr__(self):
        print ('PG number: ', self.id)
        print ('Primary OSD: ', self.primary_osd.id)
        print ('OSDs that contain replicas: ', [replica.id for replica in self.replica_osd])
        print ('Current state: ', self.state)

    def set_pg_state(self, state):
        self.state = state

    def get_pg_state(self):
        return self.state

    def request_reservation(self, id, type, time, scheduler):
        r = Reservation(id, self, type, time)

        # For now, all requests go to primary. Change this later
        i = 0
        ret = False

        scheduler.request_reservation(reservation=r)

        if ret:
            print ('Backfill successfully completed')
        else:
            print ('Backfill failed after ')

        return r

    def cancel_reservation(self, scheduler):
        '''
        Cancel an existing reservation on an osd
        :param scheduler:
        :return:
        '''

        ret = scheduler.cancel_reservation(reservation=r)
        assert(ret == True)


class Workload:

