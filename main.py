import asyncio
from ceph_classes import Reservation, OSD, PG
import random

tasks = {0:'TASK_BACKFILL', 1:'TASK_RECOVERY', 2:'TASK_SCRUB'}

class Workload:
    def __init__(self, num_tasks, num_osds=2, num_pgs=3, num_replicas=2):
        self.num_tasks = num_tasks
        self.num_pgs = num_pgs

        self.osd_pg_map = {0:[(0,0), (1,1), (2,1), (1,2), (2,2)], 1:[(0,1), (0,2), (1,0), (2,0)]}
        self.pg_osd_map = {0:[0,1], 1:[1,0], 2:[1,0]}

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
        for i in range(self.num_tasks):

            print ('Going to get a random task')

            #Get random task of a type and on a pg
            task_type, pg = self.get_random_task()

            print ('Task and Placement group: ', task_type, pg.id)
            # Random amount of time for task to complete
            time_for_task_to_complete = random.randint(0, 10)

            #Request a local reservation
            print ('Going to create a reservation')
            can_preempt = False
            r = Reservation(i, pg, pg.primary_osd, can_preempt, task_type, time_for_task_to_complete)

            print ('Now going to request a local reservation ins osd: ', r.pg.primary_osd.id)
            ret = r.pg.primary_osd.local_reserver.request_reservation(reservation=r)

            print ('Request reservation success?: ', ret)



wl = Workload(5)
wl.generate_workload()


















