import asyncio
from ceph_classes import Reservation, OSD, PG
import random
import threading
from multiprocessing.pool import ThreadPool
import time

tasks = {0:'TASK_BACKFILL', 1:'TASK_RECOVERY', 2:'TASK_SCRUB'}

class WorkloadDescriptor:
    def __init__(self, num_tasks, num_pgs=128, is_skewed=0, task_sizes=0, num_osds=3):
        self.num_tasks = num_tasks
        self.num_pgs = num_pgs
        self.is_skewed = is_skewed == 0
        self.task_sizes = task_sizes
        self.num_osds = num_osds
        self.tasks = []

class Workload:
    def __init__(self, num_tasks, num_pgs=128, is_skewed=0, taskSizes=0, num_osds=3):
        self.num_tasks = num_tasks
        self.isSkewed = is_skewed
        self.num_pgs = num_pgs

        self.osd_pg_map = {}
        self.pg_osd_map = {}
        self.workloadDescription = WorkloadDescriptor(num_tasks, num_pgs, is_skewed, taskSizes, num_osds)

        #building the osd_pg and pg_osd maps
        for osd in range(num_osds):
            self.osd_pg_map[osd] = []

        for pg in range(num_pgs):
            primary_osd = pg % 3
            replica_1_osd = (primary_osd + 1) % 3
            replica_2_osd = (replica_1_osd + 1) % 3
            self.osd_pg_map[primary_osd].append((pg, 0))
            self.osd_pg_map[replica_1_osd].append((pg, 1))
            self.osd_pg_map[replica_2_osd].append((pg, 2))
            self.pg_osd_map[pg] = [primary_osd, replica_1_osd, replica_2_osd]

        self.response_times = {}
        self.obj_storage_devices = []
        self.placement_groups = {} #map for pg_id -> pg object

        #To select random sized tasks or short/long-running tasks
        if taskSizes == 0:
            self.taskSizes = [1, 10]
        elif taskSizes == -1:
            self.taskSizes = [1]
        else:
            self.taskSizes = [10]

        for i in range(num_osds):
            self.obj_storage_devices.append(OSD(i))

        for i in range(num_pgs):
            pg_primary = self.obj_storage_devices[self.pg_osd_map[i][0]]
            pg_replicas = [self.obj_storage_devices[i] for i in self.pg_osd_map[i][1:]]

            self.placement_groups[i] = PG(i, pg_primary, pg_replicas)

    def get_random_task(self, time_for_task_to_complete):
        task, placement_group = tasks[random.randint(0, 2)], self.placement_groups[random.randint(0, self.num_pgs - 1)]
        self.workloadDescription.tasks.append((task, placement_group.id, time_for_task_to_complete))
        return task, placement_group

    def get_skewed_task(self, time_for_task_to_complete):
        placement_groups = [x[0] for x in self.osd_pg_map[0] if x[1] == 0]
        task, placement_group = tasks[random.randint(0,2)], self.placement_groups[random.choice(placement_groups)]
        self.workloadDescription.tasks.append((task, placement_group.id, time_for_task_to_complete))
        return task, placement_group

    def generate_workload(self):

        thread_pool = ThreadPool(self.num_tasks)
        thread_pool.map(self.workload_thread, [(i, random.choice(self.taskSizes), self.isSkewed) for i in range(self.num_tasks)])

        thread_pool.close()
        thread_pool.join()
        print ('All threads completed')
        print (self.response_times)

    def workload_thread(self, info):
            retry = True
            index = info[0]
            taskSize = info[1]
            isSkewed = info[2]

            time_for_task_to_complete = random.randint(taskSize, taskSize * 2 + 1)

            # Get random/skewed task of a type and on a pg
            if isSkewed:
                task_type, pg = self.get_skewed_task(time_for_task_to_complete)
            else:
                task_type, pg = self.get_random_task(time_for_task_to_complete)

            print('Task and Placement group: ', task_type, pg.id)
            # Random amount of time for task to complete

            can_preempt = True

            self.response_times[index+0.1*pg.primary_osd.id] = time.time()

            while(retry):
                print ('Going to get a random task')

                #Request a local reservation
                print ('Going to create a reservation which should take')
                r = Reservation(index, pg, pg.primary_osd, can_preempt, task_type, time_for_task_to_complete)

                print ('Now going to request a local reservation ins osd: ', r.pg.primary_osd.id)
                ret = r.pg.primary_osd.local_reserver.request_reservation(reservation=r)

                while r.state not in ('Completed', 'Preempted'):
                    print ('State not changed yet:', r.state)
                    time.sleep(1)

                if r.state == 'Completed':
                    print ('\t\t\t\tLocal reservation completed. Going to request remote')
                    r.pg.primary_osd.local_reserver.cancel_reservation(r)
                    #Requesting remote reservations

                    self.response_times[index+0.1*pg.primary_osd.id] = time.time() - self.response_times[index+0.1*pg.primary_osd.id]
                    thread_pool = ThreadPool(len(r.pg.replica_osd))
                    print ('replica osds for reservation ', index, ' are ', len(r.pg.replica_osd))
                    thread_pool.map(self.workload_remote_thread, [(r, replica_osd) for replica_osd in r.pg.replica_osd])

                    thread_pool.close()
                    thread_pool.join()

                    retry = False

                elif r.state == 'Preempted':
                    print('\t\t\t\tPreempted task ', r.id,' so going to retry')
                    r.pg.primary_osd.local_reserver.cancel_reservation(r)
                    #print ('\t\t\t\t')
                    # Preempted, so retry
                    time.sleep(5)
                    continue


    def workload_remote_thread(self, info):

        local_reservation = info[0]
        replica_osd = info[1]

        retry = True

        self.response_times[local_reservation.id+0.1*replica_osd.id] = time.time()

        while (retry):
            new_remote_reservation = Reservation(local_reservation.id+0.1*replica_osd.id, local_reservation.pg, replica_osd, local_reservation.can_preempt, \
                                                 local_reservation.type, local_reservation.time)

            print('Going to request a remote reservation on osd: ', replica_osd.id)
            ret = replica_osd.remote_reserver.request_reservation(new_remote_reservation)

            while new_remote_reservation.state not in ('Completed', 'Preempted'):
                print('State not changed yet:', new_remote_reservation.state)
                time.sleep(1)

            if new_remote_reservation.state == 'Completed':
                print ('Remote reservation ', new_remote_reservation.id, ' completed on osd ', replica_osd.id)
                replica_osd.remote_reserver.cancel_reservation(new_remote_reservation)
                self.response_times[local_reservation.id + 0.1 * replica_osd.id] = time.time() - self.response_times[local_reservation.id+0.1*replica_osd.id]

                retry = False

            elif new_remote_reservation.state == 'Preempted':
                print('\t\t\t\tPreempted remote reservation ', new_remote_reservation.id, ' so going to retry')
                replica_osd.remote_reserver.cancel_reservation(new_remote_reservation)
                # Preempted, so retry
                time.sleep(5)
                continue

        print ('Request reservation success?: ', ret)


wl = Workload(3)
w2 = Workload(5,32,1,-1)  # 5 short skewed tasks for 32 pgs
w3 = Workload(10,32,0,-1) # 10 short uniformly/randomly distributed tasks for 32 pgs

#wl.generate_workload()
w2.generate_workload()

# how to store all the task details for a given workload
for t in w2.workloadDescription.tasks:
    print(t[0], " ", t[1], " ", t[2])


















