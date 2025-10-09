#path: mountainash_settings/auth/database/exceptions.py

from typing import Optional

class DBAuthError(Exception):
    """Base exception for database authentication errors"""
    def __init__(self, message: str, provider: Optional[str] = None):
        self.provider = provider
        super().__init__(f"[{provider or 'unknown'}] {message}")

class DBAuthConfigError(DBAuthError):
    """Configuration error in database authentication settings"""
    def __init__(self, message: str, provider: Optional[str] = None, setting: Optional[str] = None):
        self.setting = setting
        super().__init__(
            f"Configuration error - {message}" + (f" (setting: {setting})" if setting else ""),
            provider
        )

class DBAuthConnectionError(DBAuthError):
    """Error establishing database connection"""
    def __init__(self, message: str, provider: Optional[str] = None, host: Optional[str] = None):
        self.host = host
        super().__init__(
            f"Connection error - {message}" + (f" (host: {host})" if host else ""),
            provider
        )

class DBAuthValidationError(DBAuthError):
    """Validation error in database authentication settings"""
    def __init__(self, message: str, provider: Optional[str] = None, validation_type: Optional[str] = None):
        self.validation_type = validation_type
        super().__init__(
            f"Validation error - {message}" + (f" (type: {validation_type})" if validation_type else ""),
            provider
        )

class DBAuthSecurityError(DBAuthError):
    """Security-related error in database authentication"""
    def __init__(self, message: str, provider: Optional[str] = None, security_check: Optional[str] = None):
        self.security_check = security_check
        super().__init__(
            f"Security error - {message}" + (f" (check: {security_check})" if security_check else ""),
            provider
        )

class DBAuthPoolError(DBAuthError):
    """Connection pool error"""
    def __init__(self, message: str, provider: Optional[str] = None, pool_operation: Optional[str] = None):
        self.pool_operation = pool_operation
        super().__init__(
            f"Pool error - {message}" + (f" (operation: {pool_operation})" if pool_operation else ""),
            provider
        )

class DBAuthTimeoutError(DBAuthError):
    """Timeout error in database operations"""
    def __init__(self, message: str, provider: Optional[str] = None, timeout_type: Optional[str] = None):
        self.timeout_type = timeout_type
        super().__init__(
            f"Timeout error - {message}" + (f" (type: {timeout_type})" if timeout_type else ""),
            provider
        )