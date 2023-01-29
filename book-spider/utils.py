import asyncio
import random
from typing import Any, Coroutine, Generator, List

from fake_useragent import UserAgent
from httpx import AsyncClient, Response

fua = UserAgent()


async def make_request(
    client: AsyncClient, url: str, delay: bool = False, seconds: int = None
) -> Response:
    """Send a request to the specified url using the provided client, with an optional
    delay before sending the request. The delay is a random float from 1 and seconds.

    Args:
        client (httpx.AsyncClient): client to send the request
        url (str): url to request
        delay (bool, optional): indicates whether to delay the request. Defaults to False
        seconds (int, optional): seconds to sleep for if delay is set to True. Defaults to SLEEP_TIME

    Returns:
        Response: response of the request
    """
    if delay and not seconds:
        raise ValueError(
            "If delay is set to True, must provide a valid seconds argument."
        )

    headers = {"user-agent": fua.random}
    request = client.get(url, headers=headers)

    if delay:
        return await sleep_and_execute(request, seconds)

    return await request


async def sleep_and_execute(task: Coroutine, seconds: int) -> Any:
    """Sleeps before executing the given task. The actual sleep time is a float
    between 1 and seconds.

    Args:
        task (Coroutine): task to be executed
        seconds (int): seconds to sleep for

    Returns:
        Any: result of the task
    """
    if seconds <= 1:
        raise ValueError("seconds must be greater than 1.")

    await asyncio.sleep(random.uniform(1, seconds))
    return await task


async def execute_with_interval(tasks: Generator, seconds: int) -> List:
    """Executes a single task with a delay between tasks. The delay is a random float
    between 1 and seconds. Returns a list of results.

    Args:
        tasks (Generator): a generator to get each task
        seconds (int): seconds to sleep for

    Returns:
        List: list of results
    """
    results = []

    for task in tasks:
        result = await sleep_and_execute(task, seconds)
        results.append(result)

    return results


async def execute_batch_with_interval(
    tasks: Generator, seconds: int, batch_size: int
) -> List:
    """Executes a batch of task with a delay between batches. The delay is a random
    float between 1 and seconds. Returns a list of results.

    Args:
        tasks (Generator): generator to get tasks
        batch_size (int): number of task in a single batch
        seconds (int): seconds to sleep for

    Returns:
        List: list of results
    """
    results = []
    task_iter = iter(tasks)

    while True:
        try:
            batch = []
            for _ in range(batch_size):
                batch.append(next(task_iter))

            task = asyncio.gather(*batch)
            results += await sleep_and_execute(task, seconds)
        except StopIteration:
            if batch:
                task = asyncio.gather(*batch)
                results += await sleep_and_execute(task, seconds)
            break

    return results
