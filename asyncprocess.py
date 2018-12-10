import asyncio
import threading
import time

def schedule_task_async(reservation, callback, async_loop):
    # Create a thread for each task
    thread = threading.Thread(target=scheduled_task, name=str(reservation.id)+'_'+reservation.type, args=([reservation]))
    thread.start()

    #thread.run()
    #task = async_loop.create_task(scheduled_task(reservation))

    #print ('Callback - ', callback)
    #task = add_success_callback(task, callback, reservation)
    #reservation.osd.async_event_loop.run_coroutine_threadsafe(task)

    #asyncio.run_coroutine_threadsafe(task, async_loop)

def scheduled_task(args):
    reservation = args
    print('Thread started')
    reservation.osd.schedule_task(reservation)

    time.sleep(reservation.time)
    print("here")
    print('Thread finished')
    reservation.osd.task_completed(reservation)

    return reservation.time

async def add_success_callback(fut, callback, reservation):
    result = await fut
    await my_callback(reservation, callback)
    return result

async def my_callback(reservation, callback):
    print("my_callback got:", reservation.id)
    callback(reservation)
    return "My return value is ignored"

'''
loop = asyncio.get_event_loop()
task = loop.create_task(scheduled_task(4))
task = add_success_callback(task, my_callback)
response = loop.run_until_complete(task)
print("response:", response)
'''
