class ConsulException(Exception):
    """Base Consul exception"""


class RequestError(ConsulException):
    """There was an error making the request to the consul server"""


class BadRequest(ConsulException):
    """There was an error in the request that was made to consul"""


class Forbidden(ConsulException):
    """Raised when the token does not validate"""


class ServerError(ConsulException):
    """An internal Consul server error occurred"""


class ConflictError(ConsulException):
    """Happens when there's a conflict in a transaction"""


class NotFound(ConsulException):
    """Raised when an operation is attempted with a value that can not be found."""


class LockFailure(ConsulException):
    """Raised if the lock can not be acquired."""


class ACLDisabled(ConsulException):
    """Raised when ACL related calls are made while ACLs are disabled"""
