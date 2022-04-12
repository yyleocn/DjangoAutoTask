from dataclasses import dataclass, field
from django.conf import settings

config = {
}

if hasattr(settings, 'AUTO_TASK'):
    config.update(
        settings.AUTO_TASK
    )


@dataclass(frozen=True)
class AutoTaskConfig:
    host: str = 'localhost'
    port: int = 8898
    authKey: bytes = b'AuthKey'

    poolSize: int = 2

    taskTimeLimit: int = 30
    taskManagerTimeout: int = 60
    name: str = 'AutoTask'
    secretKey: str = 'SecretKey'


CONFIG = AutoTaskConfig(**config)

__all__ = (
    'CONFIG',
)
