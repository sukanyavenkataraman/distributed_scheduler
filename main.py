import asyncio
from ceph_classes import Reservation, OSD, PG
import random
import threading
from multiprocessing.pool import ThreadPool
import time

tasks = {0:'TASK_BACKFILL', 1:'TASK_RECOVERY', 2:'TASK_SCRUB'}

class Workload:
    def __init__(self, num_tasks, num_osds=2, num_pgs=3, num_replicas=2):
        self.num_tasks = num_tasks
        self.num_pgs = num_pgs

        self.osd_pg_map = {0:[(0,0), (1,1), (2,1), (1,2), (2,2)], 1:[(0,1), (0,2), (1,0), (2,0)]}
        self.pg_osd_map = {0:[0,1], 1:[1,0], 2:[1,0]}

        self.response_times = {}
        '''
        for i in range(len(num_pgs)):
            for j in range(num_replicas):
                if j == 0:
                    self.pg_osd_map[j] = [random.randint(0, num_osds)]
                else:
                    self.pg_osd_map[j].append(random.randint(0, num_osds))
                    
                if self.pg_osd_map[j] in self.osd_pg_map:
                    self.osd_pg_map[self.pg_osd_map[j]] = [i]
                else:
                    self.osd_pg_map[self.pg_osd_map[j]] = [i]
        '''

        self.obj_storage_devices = []
        self.placement_groups = []

        for i in range(num_osds):
            self.obj_storage_devices.append(OSD(i))

        for i in range(num_pgs):
            pg_primary = self.obj_storage_devices[self.pg_osd_map[i][0]]
            pg_replicas = [self.obj_storage_devices[i] for i in self.pg_osd_map[i][1:]]

            self.placement_groups.append(PG(i, pg_primary, pg_replicas))

    def get_random_task(self):
        return tasks[random.randint(0,2)], self.placement_groups[random.randint(0, self.num_pgs-1)]

    def generate_workload(self):
        thread_pool = ThreadPool(self.num_tasks)
        thread_pool.map(self.workload_thread, [i for i in range(self.num_tasks)])

        thread_pool.close()
        thread_pool.join()
        print ('All threads completed')
        print (self.response_times)

    def workload_thread(self, index):
            retry = True


            # Get random task of a type and on a pg
            task_type, pg = self.get_random_task()

            print('Task and Placement group: ', task_type, pg.id)
            # Random amount of time for task to complete
            time_for_task_to_complete = random.randint(1, 3)
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
wl.generate_workload()


















