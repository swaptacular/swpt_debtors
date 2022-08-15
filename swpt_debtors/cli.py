import logging
import os
import sys
import signal
import pika
import click
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_debtors.models import MIN_INT64, MAX_INT64
from swpt_debtors import procedures
from swpt_debtors.extensions import db
from swpt_debtors.table_scanners import DebtorScanner
from swpt_debtors.multiproc_utils import spawn_worker_processes, try_unblock_signals, HANDLED_SIGNALS


@click.group('swpt_debtors')
def swpt_debtors():
    """Perform swpt_debtors specific operations."""


@swpt_debtors.command()
@with_appcontext
def subscribe():  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    """

    from .extensions import ACCOUNTS_IN_EXCHANGE, \
        TO_CREDITORS_EXCHANGE, TO_DEBTORS_EXCHANGE, TO_COORDINATORS_EXCHANGE, \
        DEBTORS_IN_EXCHANGE, DEBTORS_OUT_EXCHANGE, \
        CREDITORS_IN_EXCHANGE, CREDITORS_OUT_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = current_app.config['PROTOCOL_BROKER_QUEUE']
    routing_key = current_app.config['PROTOCOL_BROKER_QUEUE_ROUTING_KEY']
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

    def _consume_messages(url, queue, threads, prefetch_size, prefetch_count):
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
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info('Worker with PID %i started processing messages.', pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info('Worker with PID %i stopped processing messages.', pid)

    spawn_worker_processes(
        processes=processes or current_app.config['PROTOCOL_BROKER_PROCESSES'],
        target=_consume_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)
