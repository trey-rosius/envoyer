from signal import SIGINT, SIGTERM, signal
from time import sleep
from workflow_runtime import wfr


def _graceful_exit(*_):
    print("→ Stopping WorkflowRuntime …")
    wfr.shutdown()
    exit(0)


if __name__ == "__main__":
    wfr.start()

    for sig in (SIGINT, SIGTERM):
        signal(sig, _graceful_exit)

    while True:
        sleep(3600)
