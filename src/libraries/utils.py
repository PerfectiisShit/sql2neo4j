# -*- coding: utf-8 -*-

from functools import wraps

from log import get_logger


def retry(max_retry=3, ex=Exception):
    def wrap(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            for _ in xrange(max_retry):
                try:
                    result = f(*args, **kwargs)
                    if result is False:
                        continue
                    else:
                        return result
                except ex as e:
                    get_logger().debug(str(e), traceback=True)
                    continue
            get_logger().error("Failed to execute function %s after %s retries" % (str(f), max_retry))
            return False
        return decorated
    return wrap
