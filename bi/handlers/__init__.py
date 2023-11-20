from flask import jsonify
from flask_login import login_required

from bi.handlers.api import api
from bi.handlers.base import routes
from bi.monitor import get_status
from bi.permissions import require_super_admin
from bi.security import talisman


@routes.route("/ping", methods=["GET"])
@talisman(force_https=False)
def ping():
    return "PONG."


@routes.route("/status.json")
@login_required
@require_super_admin
def status_api():
    status = get_status()
    return jsonify(status)


def init_app(app):
    from bi.handlers import (
        embed,
        queries,
        static,
        authentication,
        admin,
        setup,
        organization,
    )

    app.register_blueprint(routes)
    api.init_app(app)
