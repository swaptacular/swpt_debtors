import logging
from swpt_debtors import models as m


def test_configure_interval(app, db_session, current_ts, caplog):
    caplog.at_level(logging.ERROR)

    nc = m.NodeConfig.query.one_or_none()
    if nc and nc.min_debtor_id == m.MIN_INT64:
        min_debtor_id = m.MIN_INT64 + 1
        max_debtor_id = m.MAX_INT64
    else:
        min_debtor_id = m.MIN_INT64
        max_debtor_id = m.MAX_INT64
    runner = app.test_cli_runner()

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_debtors', 'configure_interval', '--', str(m.MIN_INT64 - 1), str(m.MAX_INT64)])
    assert result.exit_code != 0
    assert 'not a valid debtor ID' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_debtors', 'configure_interval', '--', str(m.MIN_INT64), str(m.MAX_INT64 + 1)])
    assert result.exit_code != 0
    assert 'not a valid debtor ID' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_debtors', 'configure_interval', '--', str(m.MAX_INT64), str(m.MIN_INT64)])
    assert result.exit_code != 0
    assert 'invalid interval' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_debtors', 'configure_interval', '--', str(min_debtor_id), str(max_debtor_id)])
    assert result.exit_code == 0
    assert not caplog.text
    nc = m.NodeConfig.query.one()
    assert nc.min_debtor_id == min_debtor_id
    assert nc.max_debtor_id == max_debtor_id

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_debtors', 'configure_interval', '--', str(min_debtor_id), str(max_debtor_id)])
    assert result.exit_code == 0
    assert not caplog.text
    nc = m.NodeConfig.query.one()
    assert nc.min_debtor_id == min_debtor_id
    assert nc.max_debtor_id == max_debtor_id


def test_spawn_worker_processes():
    from swpt_debtors.multiproc_utils import spawn_worker_processes, HANDLED_SIGNALS, try_unblock_signals

    def _quit():
        assert len(HANDLED_SIGNALS) > 0
        try_unblock_signals()

    spawn_worker_processes(
        processes=2,
        target=_quit,
    )


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'consume_messages', '--url=INVALID'])
    assert result.exit_code == 1
