from flask import request
from flask_login import login_required, current_user

from bi import models, redis_connection
from bi.authentication import current_org
from bi.handlers import routes
from bi.handlers.base import json_response, record_event
from bi.permissions import require_super_admin
from bi.serializers import QuerySerializer
from bi.utils import json_loads
from bi.monitor import rq_status


@routes.route("/api/admin/queries/outdated", methods=["GET"])
@require_super_admin
@login_required
def outdated_queries():
    manager_status = redis_connection.hgetall("bi:status")
    query_ids = json_loads(manager_status.get("query_ids", "[]"))
    if query_ids:
        outdated_queries = (
            models.Query.query.outerjoin(models.QueryResult)
            .filter(models.Query.id.in_(query_ids))
            .order_by(models.Query.created_at.desc())
        )
    else:
        outdated_queries = []

    record_event(
        current_org,
        current_user._get_current_object(),
        {"action": "list", "object_type": "outdated_queries",},
    )

    response = {
        "queries": QuerySerializer(
            outdated_queries, with_stats=True, with_last_modified_by=False
        ).serialize(),
        "updated_at": manager_status["last_refresh_at"],
    }
    return json_response(response)


@routes.route("/api/admin/queries/rq_status", methods=["GET"])
@require_super_admin
@login_required
def queries_rq_status():
    record_event(
        current_org,
        current_user._get_current_object(),
        {"action": "list", "object_type": "rq_status"},
    )

    return json_response(rq_status())
