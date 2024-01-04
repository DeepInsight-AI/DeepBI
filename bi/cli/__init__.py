import click
import simplejson
from flask import current_app
from flask.cli import FlaskGroup, run_command, with_appcontext, AppGroup
from rq import Connection

from bi import __version__, create_app, settings, rq_redis_connection
from bi.cli import data_sources, database, groups, organization, queries, users, rq
from bi.monitor import get_status
from ai.backend.start_server import WSServer
from ai.backend.app2 import CustomApplication
import tornado.web

ai = AppGroup(help="ai")


@ai.command()
def run_ai():
    server_port = 8339
    s = WSServer(server_port)
    s.serve_forever()

@ai.command()
def run_ai_api():
    server_port = 8340
    app = CustomApplication()
    app.listen(server_port)
    tornado.ioloop.IOLoop.current().start()


def create(group):
    app = current_app or create_app()
    group.app = app

    @app.shell_context_processor
    def shell_context():
        from bi import models, settings

        return {"models": models, "settings": settings}

    return app


@click.group(cls=FlaskGroup, create_app=create)
def manager():
    """Management script for Bi"""


manager.add_command(database.manager, "database")
manager.add_command(users.manager, "users")
manager.add_command(groups.manager, "groups")
manager.add_command(data_sources.manager, "ds")
manager.add_command(organization.manager, "org")
manager.add_command(queries.manager, "queries")
manager.add_command(rq.manager, "rq")
manager.add_command(run_command, "runserver")
manager.add_command(run_ai, "run_ai")
manager.add_command(run_ai_api, "run_ai_api")


@manager.command()
def version():
    """Displays BI version."""
    print(__version__)


@manager.command()
def status():
    with Connection(rq_redis_connection):
        print(simplejson.dumps(get_status(), indent=2))


@manager.command()
def check_settings():
    """Show the settings as BI sees them (useful for debugging)."""
    for name, item in current_app.config.items():
        print("{} = {}".format(name, item))


@manager.command()
@click.argument("email", default=settings.MAIL_DEFAULT_SENDER, required=False)
def send_test_mail(email=None):
    """
    Send test message to EMAIL (default: the address you defined in MAIL_DEFAULT_SENDER)
    """
    from bi import mail
    from flask_mail import Message

    if email is None:
        email = settings.MAIL_DEFAULT_SENDER

    mail.send(
        Message(
            subject="Test Message from BI", recipients=[email], body="Test message."
        )
    )


@manager.command("shell")
@with_appcontext
def shell():
    import sys
    from ptpython import repl
    from flask.globals import _app_ctx_stack

    app = _app_ctx_stack.top.app

    repl.embed(globals=app.make_shell_context())
