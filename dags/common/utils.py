from functools import wraps
import inspect
import logging


def log(func):
    """DECORATOR
    Logs entering the function, exiting the function, passed args and return result."""

    logger = logging.getLogger("airflow.task")

    def shorten_long_values(val):
        if isinstance(val, list):
                if len(val) >= 2:
                    val = f"[{val[0]},...({len(val)-1} more)]"
        elif isinstance(val, tuple):
            if len(val) >= 2:
                val = f"({val[0]},...({len(val)-1} more))"
        elif isinstance(val, dict):
            if len(val) >= 2:
                val = f"{{'{list(val.keys())[0]}': {list(val.values())[0]},...({len(val)-1} more)}}"
        elif isinstance(val, str):
            if len(val) >= 100:
                val = f"{val[:100]}...({len(val)-100} more)"
        else:
            # Check if value is any other iterable
            try:
                _ = iter(val)
            except TypeError:
                pass
            else:
                val = f"({type(val)} iterable)"
        return val

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Get all arguments passed to the function
        bound_args = inspect.signature(func).bind(*args, **kwargs)
        bound_args.apply_defaults()
        
        # Shorten long arguments
        formatted_args = []
        for key, val in dict(bound_args.arguments).items():
            val = shorten_long_values(val)
            formatted_args.append(f"{key}={val}")
        
        passed_args_str = ", ".join(formatted_args)
        func_repr = f"Function {func.__name__}({passed_args_str})"
        print(f"[START] {func_repr}")
        return_value = func(*args, **kwargs)
        return_value = shorten_long_values(return_value)
        print(f"[END] {func_repr}. Returned: {return_value}")
        return return_value
    return wrapper
