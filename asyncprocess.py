import asyncio

def schedule_task_async(reservation, callback):
    task = reservation.osd.async_event_loop.create_task(scheduled_task(reservation))

    print ('Callback - ', callback)
    task = add_success_callback(task, callback, reservation)
    reservation.osd.async_event_loop.run_until_complete(task)

async def scheduled_task(reservation):
    print('scheduled_task(): before sleep')
    reservation.osd.schedule_task(reservation)

    await asyncio.sleep(reservation.time)
    print("here")
    print('scheduled_task(): after sleep')
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
