from examples.sync_worker import SleepySyncWorker
from runsmith.supervisor import SyncSupervisor


def root_thread_supervisor():
    root_sup = SyncSupervisor("root", "thread")
    child_sup = SyncSupervisor("child", "process")

    worker1 = SleepySyncWorker("foo")
    worker2 = SleepySyncWorker("bar")
    worker3 = SleepySyncWorker("baz")

    child_sup.register_workers(worker1, worker2)
    root_sup.register_workers(child_sup, worker3)

    root_sup.run()


def root_process_supervisor():
    """
    root supervisor
    ├── child supervisor (process)
    │   ├── foo (thread)
    │   └── bar (thread)
    └── baz (process)
    """
    worker1 = SleepySyncWorker("foo")
    worker2 = SleepySyncWorker("bar")
    worker3 = SleepySyncWorker("baz")

    root_sup = SyncSupervisor("root", "process")
    child_sup = SyncSupervisor("child", "thread")

    child_sup.register_workers(worker1, worker2)
    root_sup.register_workers(child_sup, worker3)

    root_sup.run()


if __name__ == "__main__":
    # root_thread_supervisor()
    root_process_supervisor()
