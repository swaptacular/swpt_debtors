import logging
import os
import time
import signal
import multiprocessing

HANDLED_SIGNALS = {signal.SIGINT, signal.SIGTERM}
if hasattr(signal, "SIGHUP"):  # pragma: no cover
    HANDLED_SIGNALS.add(signal.SIGHUP)
if hasattr(signal, "SIGBREAK"):  # pragma: no cover
    HANDLED_SIGNALS.add(signal.SIGBREAK)


def try_block_signals():
    """Blocks HANDLED_SIGNALS on platforms that support it."""
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)


def try_unblock_signals():
    """Unblocks HANDLED_SIGNALS on platforms that support it."""
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, HANDLED_SIGNALS)


def spawn_worker_processes(processes: int, target, **kwargs):
    """Spawns the specified number of processes, each executing the passed
    target function. In each worker process, the `target` function
    will be called with the passed keyword arguments (`kwargs`), and
    should performs its work ad infinitum.

    Note that each worker process inherits blocked SIGTERM and SIGINT
    signals from the parent process. The `target` function must
    unblock them at some point, by calling `try_unblock_signals()`.

    This function will not return until at least one of the worker
    processes has stopped. In this case, the rest of the workers will
    be terminated as well.

    """

    while processes < 1:  # pragma: no cover
        time.sleep(1)
    assert processes >= 1

    worker_processes = []
    worker_processes_have_been_terminated = False

    def worker(**kwargs):  # pragma: no cover
        try:
            target(**kwargs)
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception("Uncaught exception occured in worker with PID %i.", os.getpid())

    def terminate_worker_processes():
        nonlocal worker_processes_have_been_terminated
        if not worker_processes_have_been_terminated:
            for p in worker_processes:
                p.terminate()
            worker_processes_have_been_terminated = True

    def sighandler(signum, frame):  # pragma: no cover
        logger.info('Received "%s" signal. Shutting down...', signal.strsignal(signum))
        terminate_worker_processes()

    # To prevent the main process from exiting due to signals after
    # worker processes have been defined but before the signal
    # handling has been configured for the main process, block those
    # signals that the main process is expected to handle.
    try_block_signals()

    logger = logging.getLogger(__name__)
    logger.info('Spawning %i worker processes...', processes)

    for _ in range(processes):
        p = multiprocessing.Process(target=worker, kwargs=kwargs)
        p.start()
        worker_processes.append(p)

    for sig in HANDLED_SIGNALS:
        signal.signal(sig, sighandler)

    assert all(p.pid is not None for p in worker_processes)
    try_unblock_signals()

    # This loop waits until all worker processes have exited. However,
    # as soon as one worker process exits, all remaining worker
    # processes will be forcefully terminated.
    while any(p.exitcode is None for p in worker_processes):
        for p in worker_processes:
            p.join(timeout=1)
            if p.exitcode is not None and not worker_processes_have_been_terminated:
                logger.warning("Worker with PID %r exited unexpectedly. Shutting down...", p.pid)
                terminate_worker_processes()
                break
