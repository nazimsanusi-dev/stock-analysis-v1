"""
Failure and SLA alerting callbacks for Airflow DAGs.

Usage
-----
Import ``failure_alert`` and pass it as ``on_failure_callback`` in default_args
or on individual task operators.
"""

import logging
from airflow.models import TaskInstance

logger = logging.getLogger(__name__)


def failure_alert(context: dict) -> None:
    """
    Called by Airflow when any task fails.

    Logs structured details and can be extended to:
    - Send a Slack/Teams webhook message.
    - Publish a PagerDuty alert.
    - Write to a monitoring database.

    Parameters
    ----------
    context : dict
        Airflow task context supplied automatically on failure.
    """
    ti: TaskInstance = context["task_instance"]
    dag_id      = context["dag"].dag_id
    task_id     = ti.task_id
    run_id      = context["run_id"]
    exec_date   = context["execution_date"]
    exception   = context.get("exception", "unknown")
    log_url     = ti.log_url

    logger.error(
        "TASK FAILED | dag=%s | task=%s | run=%s | exec_date=%s\n"
        "  exception : %s\n"
        "  logs      : %s",
        dag_id, task_id, run_id, exec_date, exception, log_url,
    )

    # ── Slack webhook (uncomment and set SLACK_WEBHOOK_URL secret) ────────
    # import os, requests
    # webhook = os.getenv("SLACK_WEBHOOK_URL")
    # if webhook:
    #     requests.post(webhook, json={
    #         "text": (
    #             f":red_circle: *Task Failed*\n"
    #             f">*DAG*: `{dag_id}`\n"
    #             f">*Task*: `{task_id}`\n"
    #             f">*Run*: `{run_id}`\n"
    #             f">*Date*: `{exec_date}`\n"
    #             f">*Error*: `{exception}`\n"
    #             f">*Logs*: {log_url}"
    #         )
    #     }, timeout=10)


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Called when a task misses its SLA window."""
    logger.warning(
        "SLA MISS | dag=%s | tasks=%s",
        dag.dag_id,
        [t.task_id for t in task_list],
    )
