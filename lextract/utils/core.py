def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            wrapper.result = f(*args, **kwargs)
        return wrapper.result

    wrapper.has_run = False
    wrapper.result = None
    return wrapper
