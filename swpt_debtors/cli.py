import logging
import os
import sys
import signal
import pika
import multiprocessing
import click
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_debtors.models import MIN_INT64, MAX_INT64
from swpt_debtors import procedures
from swpt_debtors.extensions import db
from swpt_debtors.table_scanners import DebtorScanner

HANDLED_SIGNALS = {signal.SIGINT, signal.SIGTERM}
if hasattr(signal, "SIGHUP"):
    HANDLED_SIGNALS.add(signal.SIGHUP)
if hasattr(signal, "SIGBREAK"):
    HANDLED_SIGNALS.add(signal.SIGBREAK)


def try_block_signals():
    """Blocks HANDLED_SIGNALS on platforms that support it."""
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)


def try_unblock_signals():
    """Unblocks HANDLED_SIGNALS on platforms that support it."""
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, HANDLED_SIGNALS)


def consume(url, queue, threads, prefetch_size, prefetch_count):
    """Consume messages in a subprocess."""

    from swpt_debtors.actors import SmpConsumer, TerminatedConsumtion
    from swpt_debtors import create_app

    consumer = SmpConsumer(
        app=create_app(),
        config_prefix='PROTOCOL_BROKER',
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    for sig in HANDLED_SIGNALS:
        signal.signal(sig, consumer.stop)

    # Unblock the blocked signals inherited from the parent process
    # before we start any worker threads.
    try_unblock_signals()

    pid = os.getpid()
    logger = logging.getLogger(__name__)
    logger.info('Worker with PID %i started processing messages.', pid)

    try:
        consumer.start()
    except TerminatedConsumtion:
        pass

    logger.info('Worker with PID %i stopped processing messages.', pid)


@click.group('swpt_debtors')
def swpt_debtors():
    """Perform swpt_debtors specific operations."""


@swpt_debtors.command()
@with_appcontext
@click.argument('queue_name', default='')
@click.option('-r', '--routing-key', type=str, default='#', help='Specify a routing key (the default is "#").')
def subscribe(queue_name, routing_key):  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    QUEUE_NAME specifies the name of the queue. If not given, the
    value of the configuration variable PROTOCOL_BROKER_QUEUE will be
    taken. If it is not set, the default queue name is
    "swpt_debtors".

    """

    from .extensions import ACCOUNTS_IN_EXCHANGE, \
        TO_CREDITORS_EXCHANGE, TO_DEBTORS_EXCHANGE, TO_COORDINATORS_EXCHANGE, \
        DEBTORS_IN_EXCHANGE, DEBTORS_OUT_EXCHANGE, \
        CREDITORS_IN_EXCHANGE, CREDITORS_OUT_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = queue_name or current_app.config['PROTOCOL_BROKER_QUEUE']
    dead_letter_queue_name = queue_name + '.XQ'
    broker_url = current_app.config['PROTOCOL_BROKER_URL']
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(ACCOUNTS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(TO_CREDITORS_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(TO_DEBTORS_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(TO_COORDINATORS_EXCHANGE, exchange_type='headers', durable=True)
    channel.exchange_declare(CREDITORS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(CREDITORS_OUT_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(DEBTORS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(DEBTORS_OUT_EXCHANGE, exchange_type='fanout', durable=True)

    # declare exchange bindings
    channel.exchange_bind(source=TO_CREDITORS_EXCHANGE, destination=CREDITORS_IN_EXCHANGE, routing_key="#")
    channel.exchange_bind(source=TO_DEBTORS_EXCHANGE, destination=DEBTORS_IN_EXCHANGE, routing_key="#")
    channel.exchange_bind(source=TO_COORDINATORS_EXCHANGE, destination=TO_CREDITORS_EXCHANGE, arguments={
        "x-match": "all",
        "coordinator-type": "direct",
    })
    channel.exchange_bind(source=TO_COORDINATORS_EXCHANGE, destination=TO_DEBTORS_EXCHANGE, arguments={
        "x-match": "all",
        "coordinator-type": "issuing",
    })
    channel.exchange_bind(source=CREDITORS_OUT_EXCHANGE, destination=ACCOUNTS_IN_EXCHANGE, routing_key="#")
    channel.exchange_bind(source=DEBTORS_OUT_EXCHANGE, destination=ACCOUNTS_IN_EXCHANGE)

    # declare a corresponding dead-letter queue
    channel.queue_declare(dead_letter_queue_name, durable=True, arguments={
        'x-message-ttl': 604800000,
    })
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(queue_name, durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead_letter_queue_name,
    })
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(exchange=DEBTORS_IN_EXCHANGE, queue=queue_name, routing_key=routing_key)
    logger.info('Created a binding from "%s" to "%s" with routing key "%s".',
                DEBTORS_IN_EXCHANGE, queue_name, routing_key)


@swpt_debtors.command('configure_interval')
@with_appcontext
@click.argument('min_id', type=int)
@click.argument('max_id', type=int)
def configure_interval(min_id, max_id):
    """Configures the server to manage debtor IDs between MIN_ID and MAX_ID.

    The passed debtor IDs must be between -9223372036854775808 and
    9223372036854775807. Use "--" to pass negative integers. For
    example:

    $ flask swpt_debtors configure_interval -- -16 0

    """

    logger = logging.getLogger(__name__)

    def validate(value):
        if not MIN_INT64 <= value <= MAX_INT64:
            logger.error(f'{value} is not a valid debtor ID.')
            sys.exit(1)

    validate(min_id)
    validate(max_id)
    if min_id > max_id:
        logger.error('An invalid interval has been specified.')
        sys.exit(1)

    procedures.configure_node(min_debtor_id=min_id, max_debtor_id=max_id)


@swpt_debtors.command('scan_debtors')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_debtors(days, quit_early):
    """Start a process that garbage-collects inactive debtors.

    The specified number of days determines the intended duration of a
    single pass through the debtors table. If the number of days is
    not specified, the value of the configuration variable
    APP_DEBTORS_SCAN_DAYS is taken. If it is not set, the default
    number of days is 7.

    """

    logger = logging.getLogger(__name__)
    logger.info('Started debtors scanner.')
    days = days or current_app.config['APP_DEBTORS_SCAN_DAYS']
    assert days > 0.0
    scanner = DebtorScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_debtors.command('consume_messages')
@with_appcontext
@click.option('-u', '--url', type=str, help='The RabbitMQ connection URL.')
@click.option('-q', '--queue', type=str, help='The name the queue to consume from.')
@click.option('-p', '--processes', type=int, help='The number of worker processes.')
@click.option('-t', '--threads', type=int, help='The number of threads running in each process.')
@click.option('-s', '--prefetch-size', type=int, help='The prefetch window size in bytes.')
@click.option('-c', '--prefetch-count', type=int, help='The prefetch window in terms of whole messages.')
def consume_messages(url, queue, processes, threads, prefetch_size, prefetch_count):  # pragma: no cover
    """Consume and process incoming Swaptacular Messaging Protocol
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_debtors")

    * PROTOCOL_BROKER_PROCESSES (defalut 1)

    * PROTOCOL_BROKER_THREADS (defalut 1)

    * PROTOCOL_BROKER_PREFETCH_COUNT (default 1)

    * PROTOCOL_BROKER_PREFETCH_SIZE (default 0, meaning unlimited)

    """

    worker_processes = []
    worker_processes_have_been_terminated = False
    processes = processes or current_app.config['PROTOCOL_BROKER_PROCESSES']
    assert processes >= 1

    def worker(*args):
        try:
            consume(*args)
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception("Uncaught exception occured in worker with PID %i.", os.getpid())

    def terminate_worker_processes():
        nonlocal worker_processes_have_been_terminated
        if not worker_processes_have_been_terminated:
            for p in worker_processes:
                p.terminate()
            worker_processes_have_been_terminated = True

    def sighandler(signum, frame):
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
        p = multiprocessing.Process(target=worker, args=(url, queue, threads, prefetch_size, prefetch_count))
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
                logger.warn("Worker with PID %r exited unexpectedly. Shutting down...", p.pid)
                terminate_worker_processes()
                break

    sys.exit(1)
