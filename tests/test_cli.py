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
