import requests
from datetime import datetime

from flask_mail import Message
from bi import mail, models, settings
from bi.models import users
from bi.worker import job, get_job_logger
from bi.tasks.worker import Queue
from bi.query_runner import NotSupported

logger = get_job_logger(__name__)


@job("default")
def record_event(raw_event):
    event = models.Event.record(raw_event)
    models.db.session.commit()


@job("emails")
def send_mail(to, subject, html, text):
    try:
        message = Message(recipients=to, subject=subject, html=html, body=text)

        mail.send(message)
    except Exception:
        logger.exception("Failed sending message: %s", message.subject)


@job("queries", timeout=30, ttl=90)
def test_connection(data_source_id):
    try:
        data_source = models.DataSource.get_by_id(data_source_id)
        data_source.query_runner.test_connection()
    except Exception as e:
        return e
    else:
        return True


@job("schemas", queue_class=Queue, at_front=True, timeout=300, ttl=90)
def get_schema(data_source_id, refresh):
    try:
        data_source = models.DataSource.get_by_id(data_source_id)
        return data_source.get_schema(refresh)
    except NotSupported:
        return {
            "error": {
                "code": 1,
                "message": "Data source type does not support retrieving schema",
            }
        }
    except Exception as e:
        return {"error": {"code": 2, "message": "Error retrieving schema", "details": str(e)}}


def sync_user_details():
    users.sync_last_active_at()
