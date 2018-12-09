import asyncio

async def scheduled_task(seconds):
    print('scheduled_task(): before sleep')
    try:
        await asyncio.sleep(seconds)
        print("here")
    except asyncio.CancelledError:
        print('scheduled_task(): cancel sleep')
        raise
    finally:
        print('scheduled_task(): after sleep')
        return seconds

async def add_success_callback(fut, callback):
    result = await fut
    await callback(result)
    return result

async def my_callback(result):
    print("my_callback got:", result)
    #use the value of result to determine what to call - success or failure callbacks
    return "My return value is ignored"


loop = asyncio.get_event_loop()
task = loop.create_task(scheduled_task(4))
task = add_success_callback(task, my_callback)
response = loop.run_until_complete(task)
print("response:", response)
