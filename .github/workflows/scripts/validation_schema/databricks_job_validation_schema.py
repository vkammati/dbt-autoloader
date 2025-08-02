import re

from pydantic import BaseModel, EmailStr, Extra, PositiveInt, validator


class DbxJobSchedule(BaseModel, extra=Extra.forbid):
    quartz_cron_expression: str | None = None
    timezone_id: str | None = None
    pause_status: str | None = None

    @validator("quartz_cron_expression")
    def validate_cron(cls, v):
        if v is not None:
            # Quartz cron regex pattern
            pattern = (
                r"^(\?|\*|[0-5]?[0-9]|\*\/[0-5]?[0-9])"
                r"( (\?|\*|[0-5]?[0-9]|\*\/[0-5]?[0-9])){4}"
                r"( (\?|\*|[1-7]#\d|[1-7]L|[1-7]|[1-7]-[1-7]|\*|\*\/[1-7]))$"
            )
            if not re.match(pattern, v):
                raise ValueError("Invalid cron expression")
        return v


class DbxJobNotificationSettings(BaseModel, extra=Extra.forbid):
    no_alert_for_skipped_runs: bool | None = None
    no_alert_for_canceled_runs: bool | None = None


class DbxEmailNotificationsConfig(BaseModel, extra=Extra.forbid):
    on_failure: list[EmailStr] | None = None
    on_start: list[EmailStr] | None = None
    on_success: list[EmailStr] | None = None
    on_duration_warning_threshold_exceeded: list[EmailStr] | None = None


class DbxJobTaskConfig(BaseModel, extra=Extra.forbid):
    task_key: str
    max_retries: int | None = None
    timeout_seconds: PositiveInt | None = None
    package_name: str | None = None
    entry_point: str | None = None
    parameters: list[str] | None = None  # new added
    email_notifications: DbxEmailNotificationsConfig | None = None
    depends_on: list[str] | None = None


class DbxJobTagsConfig(BaseModel, extra=Extra.forbid):
    name: str
    value: str


class DbxJobConfig(BaseModel, extra=Extra.forbid):
    name: str
    tasks: list[DbxJobTaskConfig]
    max_concurrent_runs: PositiveInt | None = None
    email_notifications: DbxEmailNotificationsConfig | None = None
    notification_settings: DbxJobNotificationSettings | None = None
    schedule: DbxJobSchedule | None = None
    tags: list[str] | None = None
    cluster_tags: list[DbxJobTagsConfig] | None = None
    trigger_once_after_deploy: bool | None = None


class DbxJobsConfig(BaseModel, extra=Extra.forbid):
    jobs: list[DbxJobConfig]
