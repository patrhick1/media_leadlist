"""Custom exceptions for API client interactions."""

class APIClientError(Exception):
    """Base class for all API client errors."""
    def __init__(self, message="An error occurred with the API client", status_code=None):
        self.status_code = status_code
        super().__init__(message)

class AuthenticationError(APIClientError):
    """Raised when API authentication fails (e.g., invalid API key)."""
    def __init__(self, message="API authentication failed", status_code=401):
        super().__init__(message, status_code)

class RateLimitError(APIClientError):
    """Raised when the API rate limit is exceeded."""
    def __init__(self, message="API rate limit exceeded", status_code=429, retry_after=None):
        self.retry_after = retry_after # Seconds to wait before retrying
        super().__init__(message, status_code)

class APIRequestError(APIClientError):
    """Raised for general API request errors (e.g., invalid parameters, server errors)."""
    pass

class APIParsingError(APIClientError):
    """Raised when the API response cannot be parsed correctly."""
    def __init__(self, message="Failed to parse API response"):
        super().__init__(message, status_code=None) 