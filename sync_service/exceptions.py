class RetryableError(Exception):
    """Transient error that should be retried."""
    pass

class RateLimitExceededError(RetryableError):
    """HTTP 429 Too Many Requests, may carry a retry_after value."""
    def __init__(self, message: str = "Rate limit exceeded", retry_after: float = None):
        super().__init__(message)
        self.retry_after = retry_after