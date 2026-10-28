import asyncio


class DeviceNotBoundError(Exception):
    """The device being used does not have it's device key set yet. Either provide one or bind the device"""

    pass


class DeviceTimeoutError(Exception):
    """The device timed out when trying to communicate"""

    pass


class DeviceOfflineError(asyncio.TimeoutError):
    """The cloud reports the device as offline, so no request was sent.

    Subclasses ``asyncio.TimeoutError`` so callers that already treat a
    status-poll timeout as "device unavailable" handle this the same way.
    """

    pass
