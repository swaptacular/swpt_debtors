def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'consume_messages', '--url=INVALID'])
    assert result.exit_code == 1
