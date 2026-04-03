"""Transport-specific exceptions for kubemq-celery.

Reserved for future transport-specific error wrappers.
The transport reuses KubeMQ SDK exception classes directly
(KubeMQConnectionError, KubeMQTimeoutError, etc.) for error
classification in connection_errors and channel_errors tuples.
"""

from __future__ import annotations
