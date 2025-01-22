"""
Class that executes acquistion events across a pool of threads
"""
import threading
import warnings
import traceback
from dataclasses import dataclass
from typing import Union, Iterable, Callable, Type, Dict, Any
import queue
import inspect

from .notification_base import Notification, NotificationCategory
from .ex_event_base import ExecutorEvent, AnonymousCallableEvent
from .ex_future import ExecutionFuture
from .queue import PriorityQueue, Queue, Shutdown

# todo: Add shutdown to __del__
# todo: simplify worker threads:
#   - remove enqueing on free thread -> replace by a thread pool mechanism
#   - decouple enqueing and dequeing (related)
#   - remove is_free and related overhead
# todo: simplify ExecutorEvent class and lifecycle

_MAIN_THREAD_NAME = 'MainExecutorThread'
_ANONYMOUS_THREAD_NAME = 'AnonymousExecutorThread'

class DeviceBase:
    __slots__ = ('_engine', '_device', '_thread', '_queue', '_queue_condition')
    def __init__(self, engine, wrapped_device):
        self._engine = engine
        self._device = wrapped_device
        self._thread = None
        self._queue: list[ExecutorEvent] = []
        self._queue_condition = threading.Condition()

    def submit(self, event: Union[ExecutorEvent | Callable]) -> "ExecutionFuture":
        """
        Submit an event to run on the thread of this device

        todo: add thread safety / locking
        Thread safety / locking:
            A device can be associated to a control thread (i.e. the `control_thread` attribute is set).
            In that case, only the control thread may submit events to the device, and an exception is raised
            if any other thread submits an event.
            Each device is initially controlled by the thread that created it. The `control_thread` can be cleared
            by calling `make_multi_threaded()`, enabling any thread to submit events to the device.
            To obtain temporary exclusive control over a device, use the `with device:` syntax.
            This will block until the ownership is released by the owning thread, and release control when the block is exited.
        """
        if not isinstance(event, ExecutorEvent):
            event = AnonymousCallableEvent(event)
        else:
            if event._finished:
                # this is unrecoverable, never retry
                raise RuntimeError("Event ", event, " was already executed")

        if self._thread is None:
            self._thread = threading.Thread(target=self._run_thread)
            self._thread.start()

        # todo: rethink lifecycle of events. Perhaps the event itself can be the Future?
        future = event._pre_execution(self._engine)
        with self._queue_condition:
            self._queue.append(event)
            self._queue_condition.notify()

        return future

    def shutdown(self):
        self._engine = None
        self._device = None
        if self._thread is not None:
            self.submit(ShutdownEvent())
            self._thread.join()
            self._thread = None


    def _run_thread(self):
        """Device thread, executing events in order.

        todo: implement event retry. Probably not here, but in the event class itself, allowing it to requeue itself on failure.
        """
        while True:
            try:
                with self._queue_condition:
                    # Get the next event from the queue. Blocks until an event is available.
                    while len(self._queue) == 0:
                        self._queue_condition.wait()
                    event = self._queue.pop(0)

                    # Wait until the event can be executed
                    # If the event is not ready to start, it is responsible for signalling the condition
                    # as soon as some condition changes that may cause the event to be ready to start.
                    # Note: this signalling should be done _from_a_different_thread_ and never from the can_start function itself.
                    # or a deadlock will occur.
                    while not event.can_start(self._queue_condition):
                        self._queue_condition.wait()

                result = event.execute()
                event._post_execution(result)
            except Exception as e:
                self._engine._log_exception(e)
                event._post_execution(exception=e)  # cannot raise an exception
                if isinstance(e, Shutdown):
                    break


class MultipleExceptions(Exception):
    def __init__(self, exceptions: list[Exception]):
        self.exceptions = exceptions
        messages = [f"{type(e).__name__}: {''.join(traceback.format_exception(type(e), e, e.__traceback__))}" for e in exceptions]
        super().__init__("Multiple exceptions occurred:\n" + "\n".join(messages))

class ExecutionEngine:
    _debug = False

    def __init__(self):
        self._exceptions = Queue()
        self._devices = {}
        self._queue = []
        self._queue_condition = threading.Condition()
        self._workers = [threading.Thread(target=self._run_thread, name=f"ExecutorThread{i}") for i in range(4)]
        self._notification_queue = Queue()
        self._notification_subscribers: list[Callable[[Notification], None]] = []
        self._notification_subscriber_filters: list[Union[NotificationCategory, Type]] = []
        self._notification_lock = threading.Lock()
        self._notification_thread = None
        for thread in self._workers:
            thread.start()

    def _run_thread(self):
        """Main loop for worker threads.

        A thread is stopped by sending a TerminateThreadEvent to it, which raises a Shutdown exception.
        todo: refactor, code duplication with Device
        """
        while True:
            with self._queue_condition:
                # Finds the first event that is ready to start
                event = next((e for e in self._queue if e.can_start(self._queue_condition)), None)
                if event is None:
                    self._queue_condition.wait()
                    continue

            self._queue.remove(event)
            try:
                result = event.execute()
                event._post_execution(result)
            except Exception as e:
                self._log_exception(e)
                event._post_execution(exception=e)  # cannot raise an exception
                if isinstance(e, Shutdown):
                    break



    def register(self, id: str, obj: object):
        """
        Wraps an object for use with the ExecutionEngine

        The wrapper exposes the public properties and attributes of the wrapped object, converting
        all get and set access, as well as method calls to Events.
        Private methods and attributes are not exposed.

        After wrapping, the original object should not be used directly anymore.
        All access should be done through the wrapper, which takes care of thread safety, synchronization, etc.

        Args:
            id: Unique id (name) of the device, used by the ExecutionEngine.
            obj: object to wrap. The object should only be registered once. Use of the original object should be avoided after wrapping,
                since access to the original object is not thread safe or otherwise managed by the ExecutionEngine.
        """
        #
        if any(d is obj for d in self._devices) or isinstance(obj, DeviceBase):
            raise ValueError("Object already registered")

        # get a list of all properties and methods, including the ones in base classes
        # Also process class annotations, for attributes that are not properties
        class_hierarchy = inspect.getmro(obj.__class__)
        all_dict = {}
        for c in class_hierarchy[::-1]:
            all_dict.update(c.__dict__)
            annotations = c.__dict__.get('__annotations__', {})
            all_dict.update(annotations)

        # add all attributes that are not already in the dict
        for n, a in obj.__dict__.items():
            if not n.startswith("_") and n not in all_dict:
                all_dict[n] = None

        # create the wrapper class
        class_dict = {}
        slots = []
        for name, attribute in all_dict.items():
            if name.startswith('_'):
                continue  # skip private attributes

            if inspect.isfunction(attribute):
                def method(self, *args, _name=name, **kwargs):
                    event = MethodCallEvent(method_name=_name, args=args, kwargs=kwargs, instance=self._device)
                    return self.submit(event)

                class_dict[name] = method
            else:
                def getter(self, _name=name):
                    event = GetAttrEvent(attr_name=_name, instance=self._device, method=getattr)
                    return self.submit(event).await_execution()

                def setter(self, value, _name=name):
                    event = SetAttrEvent(attr_name=_name, value=value, instance=self._device, method=setattr)
                    self.submit(event).await_execution()

                has_setter = not isinstance(attribute, property) or attribute.fset is not None
                class_dict[name] = property(getter, setter if has_setter else None, None, f"Wrapped attribute {name}")
                if not isinstance(attribute, property):
                    slots.append(name)

        class_dict['__slots__'] = () # prevent addition of new attributes.
        WrappedObject = type('_' + obj.__class__.__name__, (DeviceBase,), class_dict)
        # todo: cache dynamically generated classes
        wrapped = WrappedObject(self,obj)
        self._devices[id] = wrapped
        return wrapped

    def subscribe_to_notifications(self, subscriber: Callable[[Notification], None],
                                   notification_type: Union[NotificationCategory, Type] = None
                                   ) -> None:
        """
        Subscribe an object to receive notifications.

        Args:
            subscriber (Callable[[Notification], Any]): A callable that takes a single
                Notification object as an argument.
            notification_type (Union[NotificationCategory, Type], optional): The type of notification to subscribe to.
              this can either be a NotificationCategory or a specific subclass of Notification.

        Returns:
            None

        Raises:
            TypeError: If the subscriber is not a callable taking exactly one argument.
        """
        with self._notification_lock:
            if len(self._notification_subscribers) == 0:
                self._notification_thread = threading.Thread(target=self._notification_thread_run)
                self._notification_thread.start()
            self._notification_subscribers.append(subscriber)
            self._notification_subscriber_filters.append(notification_type)

    def unsubscribe_from_notifications(self, subscriber: Callable[[Notification], None]) -> None:
        """
        Unsubscribe an object from receiving notifications.

        Args:
            subscriber (Callable[[Notification], Any]): The callable that was previously subscribed to notifications.

        Returns:
            None
        """
        with self._notification_lock:
            index = self._notification_subscribers.index(subscriber)
            self._notification_subscribers.pop(index)
            self._notification_subscriber_filters.pop(index)

    def _notification_thread_run(self):
        try:
            while True:
                notification = self._notification_queue.get()
                try:
                    with self._notification_lock:
                        for subscriber, filter in zip(self._notification_subscribers, self._notification_subscriber_filters):
                            if filter is not None and isinstance(filter, type) and not isinstance(notification, filter):
                                continue  # not interested in this type
                            if filter is not None and isinstance(filter, NotificationCategory) and notification.category != filter:
                                continue
                            subscriber(notification)
                except Exception as e:
                    self._log_exception(e)
                finally:
                    self._notification_queue.task_done()
        except Shutdown:
            pass

    def publish_notification(self, notification: Notification):
        """
        Publish a notification by adding it the publish queue
        """
        self._notification_queue.put(notification)

    def __getitem__(self, device_id: str):
        """
        Get a device by name

        Args:
            device_id: unique id of the device that was used in the call to register_device.
        Returns:
            device
        Raises:
            KeyError if a device with this id is not found.
        """
        return self._devices[device_id]

    @staticmethod
    def on_any_executor_thread():
        #todo: remove
        result = (hasattr(threading.current_thread(), 'execution_engine_thread')
                  and threading.current_thread().execution_engine_thread)
        return result

    def _start_new_thread(self, name):
        self._thread_managers[name] = _ExecutionThreadManager(self, name)

    @staticmethod
    def set_debug_mode(debug):
        ExecutionEngine._debug = debug

    def _log_exception(self, exception):
        self._exceptions.put(exception)

    def check_exceptions(self):
        """
        Check if any exceptions have been raised during the execution of events and raise them if so
        """
        exceptions = self._exceptions
        self._exceptions = queue.Queue()
        exceptions = list(exceptions.queue)
        if exceptions:
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                raise MultipleExceptions(exceptions)

    def submit(self, event: Union[ExecutorEvent | Callable]) -> ExecutionFuture:
        """
        Submit an event for execution in the worker thread pool.
        Events are executed as soon as the conditions for execution are met.

        The execution threads test if an event is ready to execute by calling the `can_start` method of the event.
        If the event is not ready to start, the event is skipped and the next event is checked.
        After all events have been checked, the threads are suspended.

        Events that report can_start = False are responsible for waking up the suspended threads as soon as they become ready to be executed.
        To enable this, can_start takes a notification listener as an argument, which is notified when the event becomes ready to start (or at least
        needs to be re-evaluated).

        todo: prioritize events?


        See Also:
            `DeviceBase.submit` for submitting events to a specific device.

        Returns:
        --------
        ExecutionFuture.

        Notes:
        ------
        - If a callable object with no arguments is submitted, it will be automatically wrapped in a AnonymousCallableEvent.
        """
        # Auto convert callable to event
        if not isinstance(event, ExecutorEvent):
            event = AnonymousCallableEvent(event)
        else:
            if event._finished:
                # this is unrecoverable, never retry
                raise RuntimeError("Event ", event, " was already executed")

        # todo: rethink lifecycle of events. Perhaps the event itself can be the Future?
        future = event._pre_execution(self)
        with self._queue_condition:
            self._queue.append(event)
            self._queue_condition.notify()

        return future

    def shutdown(self):
        """
        Stop all devices, then stop all threads in the thread pool
        """
        for device in self._devices.values():
            device.shutdown()

        for _ in self._workers:
            self.submit(ShutdownEvent())
        for thread in self._workers:
            thread.join()

        # Make sure the notification thread is stopped if it was started at all
        if self._notification_thread is not None:
            self._notification_queue.shutdown()
            self._notification_thread.join()




class _ExecutionThreadManager:
    """
    Class which manages a single thread that executes events from a queue, one at a time. Events can be added
    to either end of the queue, in order to prioritize them. The thread will stop when the shutdown method is called,
    or in the event of an unhandled exception during event execution.

    This class handles thread safety so that it is possible to check if the thread has any currently executing events
    or events in its queue with the is_free method.

    """
    def __init__(self, engine: ExecutionEngine, name='UnnamedExectorThread'):
        self.thread = threading.Thread(target=self._run_thread, name=name)
        self.thread.execution_engine_thread = True
        # todo: use single queue for all threads in a pool
        # todo: custom queue class or re-queuing mechanism that allows checking requirements for starting the operation?
        self._queue = PriorityQueue()
        self._exception = None
        self._engine = engine
        self._event_executing = threading.Event()
        self.thread.start()

    def join(self):
        self.thread.join()

    def is_free(self):
        """
        return true if an event is not currently being executed and the queue is empty
        """
        return not self._event_executing.is_set() and self._queue.empty()

    def submit_event(self, event, prioritize=False):
        """
        Submit an event for execution on this thread. If prioritize is True, the event will be executed before any other
        events in the queue.

        Raises:
            Shutdown: If the thread is shutting down
        """
        if prioritize:
            event.priority = 0 # place at front of queue
        self._queue.put(event)

    def terminate(self):
        """
        Stop the thread immediately, without waiting for the current event to finish
        """
        self._queue.shutdown(immediately=True)
        self.thread.join()

    def shutdown(self):
        """
        Stop the thread and wait for it to finish
        """
        self._queue.shutdown(immediately=False)
        self.thread.join()


@dataclass
class MethodCallEvent(ExecutorEvent):

    def __init__(self, method_name: str, args: tuple, kwargs: Dict[str, Any], instance: Any):
        super().__init__()
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs
        self.instance = instance

    def execute(self):
        method = getattr(self.instance, self.method_name)
        return method(*self.args, **self.kwargs)

class ShutdownEvent(ExecutorEvent):
    def execute(self):
        raise Shutdown()

class GetAttrEvent(ExecutorEvent):

    def __init__(self, attr_name: str, instance: Any, method: Callable):
        super().__init__()
        self.attr_name = attr_name
        self.instance = instance
        self.method = method

    def execute(self):
        value = self.method(self.instance, self.attr_name)
        print(f"Got {self.attr_name} with value {value}")
        return value


class SetAttrEvent(ExecutorEvent):

    def __init__(self, attr_name: str, value: Any, instance: Any, method: Callable):
        super().__init__()
        self.attr_name = attr_name
        self.value = value
        self.instance = instance # wrapped object
        self.method = method

    def execute(self):
        self.method(self.instance, self.attr_name, self.value)
        print(f"Set {self.attr_name} to {self.value}")


