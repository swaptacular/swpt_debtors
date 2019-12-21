from datetime import timedelta
import click
from os import environ
from flask import current_app
from flask.cli import with_appcontext
from .extensions import db
from .table_scanners import RunningTransferCollector


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


@swpt_debtors.command('collect_running_transfers')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def collect_running_transfers(days, quit_early):
    """Start a process that garbage-collects finalized running transfers.

    Only finalized running transfers older than a given number of days
    are deleted. If the number of days is not specified, the value of
    the environment variable APP_RUNNING_TRANSFERS_GC_DAYS is
    taken. If it is not set, the default number of days is 14.

    """

    click.echo('Collecting running transfers...')
    days = days or current_app.config['APP_RUNNING_TRANSFERS_GC_DAYS']
    assert days > 0.0
    collector = RunningTransferCollector(days)
    collector.run(db.engine, timedelta(days=days / 2), quit_early=quit_early)
