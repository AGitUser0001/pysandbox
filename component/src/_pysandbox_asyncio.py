# type: ignore
import asyncio
import itertools
import math
import time

import componentize_py_async_support as async_support
from wit_world.imports import host

timer_ids = itertools.count()
timers = {}


class TimerHandle(asyncio.TimerHandle):
  def __init__(self, when, callback, args, loop, context):
    super().__init__(when, callback, args, loop, context=context)
    self.timer_id = next(timer_ids)
    self.waiting = False

  def cancel(self):
    if not self.cancelled():
      asyncio.Handle.cancel(self)
      if self.waiting:
        host.cancel_timer(self.timer_id)


async def wait_timer(handle):
  if handle.cancelled():
    return
  handle.waiting = True
  try:
    await host.wait_timer(handle.timer_id, max(0.0, handle.when() - time.monotonic()))
  finally:
    handle.waiting = False
  if not handle.cancelled():
    # Queue the actual TimerHandle so cancellation is checked before dispatch.
    async_support._future_state.get().handles.append(handle)


def call_at(loop, when, callback, *args, context=None):
  if math.isnan(when):
    raise ValueError("timer deadline must not be NaN")
  if context is not None:
    context.run(async_support._future_state.set, async_support._future_state.get())
  handle = TimerHandle(when, callback, args, loop, context)
  task = loop.create_task(wait_timer(handle))
  timers[handle.timer_id] = (handle, task)
  task.add_done_callback(lambda _: timers.pop(handle.timer_id, None))
  return handle


def call_later(loop, delay, callback, *args, context=None):
  return call_at(loop, time.monotonic() + delay, callback, *args, context=context)


def loop_time(loop):
  return time.monotonic()


def install_timers():
  async_support._Loop.call_later = call_later
  async_support._Loop.call_at = call_at
  async_support._Loop.time = loop_time


async def close_timers():
  active = list(timers.values())
  for handle, _ in active:
    handle.cancel()
  await asyncio.gather(*(task for _, task in active), return_exceptions=True)
