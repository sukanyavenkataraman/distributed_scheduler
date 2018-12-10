import time

def schedule_task(args):
    reservation = args
    print('Thread started')
    reservation.osd.schedule_task(reservation)

    time.sleep(reservation.time)
    print("here")
    print('Thread finished')
    reservation.osd.task_completed(reservation)

    return reservation.time