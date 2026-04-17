from examples.sleepy_worker import SleepyWorker
from pycrew.supervisor import SyncSupervisor

if __name__ == "__main__":
    supervisor = SyncSupervisor("my-supervisor", "thread")
    supervisor.register_workers(
        SleepyWorker("foo"),
        SleepyWorker("bar"),
    )
    supervisor.run()
