from datetime import timedelta
import sys
import click
from os import environ
from flask import current_app
from flask.cli import with_appcontext
from swpt_debtors.models import MIN_INT64, MAX_INT64
from swpt_debtors import procedures
from swpt_debtors.extensions import db
from swpt_debtors.table_scanners import DebtorScanner


@click.group('swpt_debtors')
def swpt_debtors():
    """Perform swpt_debtors specific operations."""


@swpt_debtors.command()
@with_appcontext
@click.argument('queue_name')
def subscribe(queue_name):  # pragma: no cover
    """Subscribe a queue for the observed events and messages.

    QUEUE_NAME specifies the name of the queue.

    """

    from .extensions import broker, MAIN_EXCHANGE_NAME
    from . import actors  # noqa

    channel = broker.channel
    channel.exchange_declare(MAIN_EXCHANGE_NAME)
    click.echo(f'Declared "{MAIN_EXCHANGE_NAME}" direct exchange.')

    if environ.get('APP_USE_LOAD_BALANCING_EXCHANGE', '') not in ['', 'False']:
        bind = channel.exchange_bind
        unbind = channel.exchange_unbind
    else:
        bind = channel.queue_bind
        unbind = channel.queue_unbind
    bind(queue_name, MAIN_EXCHANGE_NAME, queue_name)
    click.echo(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{queue_name}".')

    for actor in [broker.get_actor(actor_name) for actor_name in broker.get_declared_actors()]:
        if 'event_subscription' in actor.options:
            routing_key = f'events.{actor.actor_name}'
            if actor.options['event_subscription']:
                bind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                click.echo(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{routing_key}".')
            else:
                unbind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                click.echo(f'Unsubscribed "{queue_name}" from "{MAIN_EXCHANGE_NAME}.{routing_key}".')


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

    def validate(value):
        if not MIN_INT64 <= value <= MAX_INT64:
            click.echo(f'Error: {value} is not a valid debtor ID.')
            sys.exit(1)

    validate(min_id)
    validate(max_id)
    if min_id > max_id:
        click.echo('Error: an invalid interval has been specified.')
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

    click.echo('Scanning debtors...')
    days = days or current_app.config['APP_DEBTORS_SCAN_DAYS']
    assert days > 0.0
    scanner = DebtorScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)
