from ai.backend.util.db.auto_yzj.main import _run


def automatically_api(data):
    if data['strategy'] == 'automatically_add_targets':
        code = _run(data['db'], data['brand'], data['market'], data['user'])
        return code
