import time

def debug(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        print(f"Calling {func.__name__} with {args}, {kwargs} : {result}")
        return result
    return wrapper

def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Execution time of {func.__name__}: {end_time - start_time:.4f} seconds")
        return result
    return wrapper

@debug
@timeit
def multiply(a, b):
    time.sleep(1)
    return a * b

multiply(3, 5)
