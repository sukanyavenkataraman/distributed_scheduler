import asyncio
from scheduler import LocalReserver, RemoteReserver
import time

# Priorities
OSD_RECOVERY_PRIORITY=100
OSD_BACKFILL_PRIORITY=50
OSD_SCRUB_PRIORITY=20

class Reservation:
    '''
    Reservation class to define reservations
    A reservation is defined by an id, and has the following characteristics -
    1. Type that determines its priority
    2. Placement group that is making the reservation
    3. Time taken to complete this task (approx.)
    '''
    def __init__(self, id, pg, osd, can_preempt=True, type='TASK_RECOVERY', time=10, max_tries_preempt=3):
        self.id = id
        self.pg = pg
        self.osd = osd
        self.can_preempt = can_preempt
        self.type = type
        self.max_tries_preempt = max_tries_preempt
        self.tries_current = 0

        if type == 'TASK_RECOVERY':
            self.priority = OSD_RECOVERY_PRIORITY
        elif type == 'TASK_BACKFILL':
            self.priority = OSD_BACKFILL_PRIORITY
        else:
            self.priority = OSD_SCRUB_PRIORITY

        self.time = time
        self.state = 'Initiated'

    def set_task_state(self, state):
        self.state = state

    def get_task_state(self):
        return self.state

    def on_state_changed(self):
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
    def __init__(self, id, max_tasks=100, max_backfills=2, min_priority=0, num_replicas=2):
        self.id = id

        self.max_backfills = max_backfills
        self.min_priority = min_priority

        self.num_tasks = 0
        self.max_tasks = max_tasks

        self.current_tasks = []

        self.local_reserver = LocalReserver(max_replicas=num_replicas)
        self.remote_reserver = RemoteReserver()

    '''
    def __repr__(self):
        print ('OSD number: ', self.id)
        print ('Max allowed backfills: ', self.max_backfills)
        print ('Min priority: ', self.min_priority)
        print ('Number of current scheduled tasks: ', self.num_tasks)
        print ('Maximum allowed tasks: ', self.max_tasks)
        print ('Current usage: ', self.get_current_usage())
    '''

    def get_current_state(self):
        print ('Current state of OSD: ', self.id)
        print (self.num_tasks)
        print ([task.id for task in self.current_tasks])
        print ('Usage: ', self.get_current_usage())

    def get_current_usage(self):
        return 100.0*self.num_tasks/self.max_tasks

    def schedule_task(self, task):
        print ('Scheduling task on OSD: ', self.id, ' Current state: ')
        self.get_current_state()

        self.num_tasks += 1
        self.current_tasks.append(task)
        task.state = 'In progress'

    def task_completed(self, task):
        print('Task ', task.id, ' completed on OSD: ', self.id, ' with state ', task.state)
        print('Current state of OSD: ')
        self.get_current_state()

        if task not in self.current_tasks:
            print ('How is task: ', task.id, ' not present in current_tasks: ', [task.id for task in self.current_tasks])
            return

        self.num_tasks -= 1
        self.current_tasks.remove(task)
        task.state = 'Completed'

    def task_preempted(self, task):

        print('Task ', task.id, ' preempted on OSD: ', self.id, ' Current state: ')
        self.get_current_state()
        task.state = 'Preempted'

        if task not in self.current_tasks:
            print('How is task: ', task.id, ' not present in current_tasks: ', [task.id for task in self.current_tasks])
            return

        self.num_tasks -= 1
        self.current_tasks.remove(task)



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