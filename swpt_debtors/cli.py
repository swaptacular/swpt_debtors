from datetime import timedelta
import click
from os import environ
from flask import current_app
from flask.cli import with_appcontext
from .extensions import db
from .table_scanners import RunningTransfersScanner, AccountsScanner


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


@swpt_debtors.command('scan_running_transfers')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_running_transfers(days, quit_early):
    """Start a process that garbage-collects staled running transfers.

    The specified number of days determines the intended duration of a
    single pass through the running transfers table. If the number of
    days is not specified, the value of the environment variable
    APP_RUNNING_TRANSFERS_SCAN_DAYS is taken. If it is not set, the
    default number of days is 7.

    """

    click.echo('Scanning running transfers...')
    days = days or float(current_app.config['APP_RUNNING_TRANSFERS_SCAN_DAYS'])
    assert days > 0.0
    collector = RunningTransfersScanner()
    collector.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_debtors.command('scan_accounts')
@with_appcontext
@click.option('-h', '--hours', type=float, help='The number of hours.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_accounts(hours, quit_early):
    """Start a process that executes accounts maintenance operations.

    The specified number of hours determines the intended duration of
    a single pass through the accounts table. If the number of hours
    is not specified, the value of the environment variable
    APP_ACCOUNTS_SCAN_HOURS is taken. If it is not set, the default
    number of hours is 24.

    """

    click.echo('Scanning accounts...')
    hours = hours or float(current_app.config['APP_ACCOUNTS_SCAN_HOURS'])
    assert hours > 0.0
    scanner = AccountsScanner(hours)
    scanner.run(db.engine, timedelta(hours=hours), quit_early=quit_early)
