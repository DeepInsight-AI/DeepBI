from .general import (
    record_event,
    send_mail,
    sync_user_details,
)
from .queries import (
    enqueue_query,
    execute_query,
    refresh_queries,
    refresh_schemas,
    cleanup_query_results,
    empty_schedules,
    remove_ghost_locks,
)
from .alerts import check_alerts_for_query
from .failure_report import send_aggregated_errors
from .worker import Worker, Queue, Job
from .schedule import rq_scheduler, schedule_periodic_jobs, periodic_job_definitions

from bi import rq_redis_connection
from rq.connections import push_connection, pop_connection


def init_app(app):
    app.before_request(lambda: push_connection(rq_redis_connection))
    app.teardown_request(lambda _: pop_connection())

