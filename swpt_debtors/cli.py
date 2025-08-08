import logging
import os
import time
import sys
import signal
import pika
import click
from typing import Optional, Any
from datetime import timedelta
from sqlalchemy import select
from flask import current_app
from flask.cli import with_appcontext
from flask_sqlalchemy.model import Model
from swpt_pythonlib.utils import ShardingRealm
from swpt_debtors.extensions import db
from swpt_debtors.table_scanners import DebtorScanner
from swpt_pythonlib.multiproc_utils import (
    spawn_worker_processes,
    try_unblock_signals,
    HANDLED_SIGNALS,
)
from swpt_pythonlib.flask_signalbus import SignalBus, get_models_to_flush


@click.group("swpt_debtors")
def swpt_debtors():
    """Perform swpt_debtors specific operations."""


@swpt_debtors.command()
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to declare and subscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
def subscribe(url, queue, queue_routing_key):  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_debtors")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")
    """

    from .extensions import DEBTORS_IN_EXCHANGE, DEBTORS_OUT_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = (
        queue_routing_key
        or current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    )
    dead_letter_queue_name = queue_name + ".XQ"
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(
        DEBTORS_IN_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        DEBTORS_OUT_EXCHANGE, exchange_type="fanout", durable=True
    )

    # declare a corresponding dead-letter queue
    channel.queue_declare(
        dead_letter_queue_name,
        durable=True,
        arguments={"x-queue-type": "stream"},
    )
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(
        queue_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "overflow": "reject-publish",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": dead_letter_queue_name,
        },
    )
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(
        exchange=DEBTORS_IN_EXCHANGE, queue=queue_name, routing_key=routing_key
    )
    logger.info(
        'Created a binding from "%s" to "%s" with routing key "%s".',
        DEBTORS_IN_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_debtors.command("unsubscribe")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to unsubscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
def unsubscribe(url, queue, queue_routing_key):  # pragma: no cover
    """Unsubscribe a RabbitMQ queue from receiving incoming messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_debtors")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")
    """

    from .extensions import DEBTORS_IN_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = (
        queue_routing_key
        or current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    )
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    channel.queue_unbind(
        exchange=DEBTORS_IN_EXCHANGE, queue=queue_name, routing_key=routing_key
    )
    logger.info(
        'Removed binding from "%s" to "%s" with routing key "%s".',
        DEBTORS_IN_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_debtors.command("delete_queue")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to delete.",
)
def delete_queue(url, queue):  # pragma: no cover
    """Try to safely delete a RabbitMQ queue.

    When the queue is not empty or is currently in use, this command
    will continuously try to delete the queue, until the deletion
    succeeds or fails for some other reason.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_debtors")
    """

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    REPLY_CODE_PRECONDITION_FAILED = 406

    while True:
        channel = connection.channel()
        try:
            channel.queue_delete(
                queue=queue_name,
                if_unused=True,
                if_empty=True,
            )
            logger.info('Deleted "%s" queue.', queue_name)
            break
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code != REPLY_CODE_PRECONDITION_FAILED:
                raise
            time.sleep(3.0)


@swpt_debtors.command("verify_shard_content")
@with_appcontext
def verify_shard_content():
    """Verify that the shard contains only records belonging to the
    shard.

    If the verification is successful, the exit code will be 0. If a
    record has been found that does not belong to the shard, the exit
    code will be 1.
    """

    from swpt_debtors.models import Debtor

    class InvalidRecord(Exception):
        """The record does not belong the shard."""

    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    yield_per = current_app.config["APP_VERIFY_SHARD_YIELD_PER"]
    sleep_seconds = current_app.config["APP_VERIFY_SHARD_SLEEP_SECONDS"]

    def verify_table(conn, *table_columns):
        with conn.execution_options(yield_per=yield_per).execute(
                select(*table_columns)
        ) as result:
            for n, row in enumerate(result):
                if n % yield_per == 0 and sleep_seconds > 0.0:
                    time.sleep(sleep_seconds)
                if not sharding_realm.match(*row):
                    raise InvalidRecord

    with db.engine.connect() as conn:
        logger = logging.getLogger(__name__)
        try:
            verify_table(conn, Debtor.debtor_id)
        except InvalidRecord:
            logger.error(
                "At least one record has been found that does not belong to"
                " the shard."
            )
            sys.exit(1)


@swpt_debtors.command("scan_debtors")
@with_appcontext
@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_debtors(days, quit_early):
    """Start a process that garbage-collects inactive debtors.

    The specified number of days determines the intended duration of a
    single pass through the debtors table. If the number of days is
    not specified, the value of the configuration variable
    APP_DEBTORS_SCAN_DAYS is taken. If it is not set, the default
    number of days is 7.

    """

    logger = logging.getLogger(__name__)
    logger.info("Started debtors scanner.")
    days = days or current_app.config["APP_DEBTORS_SCAN_DAYS"]
    assert days > 0.0
    scanner = DebtorScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_debtors.command("consume_messages")
@with_appcontext
@click.option("-u", "--url", type=str, help="The RabbitMQ connection URL.")
@click.option(
    "-q", "--queue", type=str, help="The name the queue to consume from."
)
@click.option(
    "-p", "--processes", type=int, help="The number of worker processes."
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="The number of threads running in each process.",
)
@click.option(
    "-s",
    "--prefetch-size",
    type=int,
    help="The prefetch window size in bytes.",
)
@click.option(
    "-c",
    "--prefetch-count",
    type=int,
    help="The prefetch window in terms of whole messages.",
)
@click.option(
    "--draining-mode",
    is_flag=True,
    help="Make periodic pauses to allow the queue to be deleted safely.",
)
def consume_messages(
    url, queue, processes, threads, prefetch_size, prefetch_count,
    draining_mode
):
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

    def _consume_messages(
        url, queue, threads, prefetch_size, prefetch_count
    ):  # pragma: no cover
        """Consume messages in a subprocess."""

        from swpt_debtors.actors import SmpConsumer, TerminatedConsumtion
        from swpt_debtors import create_app

        consumer = SmpConsumer(
            app=create_app(),
            config_prefix="PROTOCOL_BROKER",
            url=url,
            queue=queue,
            threads=threads,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
            draining_mode=draining_mode,
        )
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, consumer.stop)
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info("Worker with PID %i started processing messages.", pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info("Worker with PID %i stopped processing messages.", pid)

    spawn_worker_processes(
        processes=processes or current_app.config["PROTOCOL_BROKER_PROCESSES"],
        target=_consume_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_debtors.command("flush_messages")
@with_appcontext
@click.option(
    "-p",
    "--processes",
    type=int,
    help=(
        "Then umber of worker processes."
        " If not specified, the value of the FLUSH_PROCESSES environment"
        " variable will be used, defaulting to 1 if empty."
    ),
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Flush every FLOAT seconds."
        " If not specified, the value of the FLUSH_PERIOD environment"
        " variable will be used, defaulting to 2 seconds if empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
@click.argument("message_types", nargs=-1)
def flush_messages(
    message_types: list[str],
    processes: int,
    wait: float,
    quit_early: bool,
) -> None:
    """Send pending messages to the message broker.

    If a list of MESSAGE_TYPES is given, flushes only these types of
    messages. If no MESSAGE_TYPES are specified, flushes all messages.

    """
    logger = logging.getLogger(__name__)
    models_to_flush = get_models_to_flush(
        current_app.extensions["signalbus"], message_types
    )
    logger.info(
        "Started flushing %s.", ", ".join(m.__name__ for m in models_to_flush)
    )

    def _flush(
        models_to_flush: list[type[Model]],
        wait: Optional[float],
    ) -> None:  # pragma: no cover
        from swpt_debtors import create_app

        app = create_app()
        stopped = False

        def stop(signum: Any = None, frame: Any = None) -> None:
            nonlocal stopped
            stopped = True

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, stop)
        try_unblock_signals()

        with app.app_context():
            signalbus: SignalBus = current_app.extensions["signalbus"]
            while not stopped:
                started_at = time.time()
                try:
                    count = signalbus.flushmany(models_to_flush)
                except Exception:
                    logger.exception(
                        "Caught error while sending pending signals."
                    )
                    sys.exit(1)

                if count > 0:
                    logger.info(
                        "%i signals have been successfully processed.", count
                    )
                else:
                    logger.debug("0 signals have been processed.")

                if quit_early:
                    break
                time.sleep(max(0.0, wait + started_at - time.time()))

    spawn_worker_processes(
        processes=(
            processes
            if processes is not None
            else current_app.config["FLUSH_PROCESSES"]
        ),
        target=_flush,
        models_to_flush=models_to_flush,
        wait=(
            wait if wait is not None else current_app.config["FLUSH_PERIOD"]
        ),
    )
    sys.exit(1)
