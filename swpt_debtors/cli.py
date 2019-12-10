from datetime import datetime, timedelta, timezone
import click
from os import environ
from flask import current_app
from flask.cli import with_appcontext
from . import procedures


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


@swpt_debtors.command('flush_running_transfers')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
def flush_running_transfers(days):
    """Delete finalized running transfers older than a given number of days.

    If the number of days is not specified, the value of the
    environment variable APP_FLUSH_RUNNING_TRANSFERS_DAYS is taken. If
    it is not set, the default number of days is 14.

    """

    # TODO: Make sure running transfers are flushed periodically. Note
    # that the current method of flushing may consume considerable
    # amount of database resources for quite some time. This could
    # potentially be a problem.

    days = days or current_app.config['APP_FLUSH_RUNNING_TRANSFERS_DAYS']
    cutoff_ts = datetime.now(tz=timezone.utc) - timedelta(days=days)
    n = procedures.flush_running_transfers(cutoff_ts)
    if n == 1:
        click.echo(f'1 running transfer has been deleted.')
    elif n > 1:  # pragma: nocover
        click.echo(f'{n} running transfers have been deleted.')
