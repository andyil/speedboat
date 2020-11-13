import boto3
from threading import local
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED


def slow(x):
    import time
    time.sleep(5)
    return f'x{x+1}'

class LazyThreadPool:

    def __init__(self):
        self.max_workers = 50
        self.max_pending = 200

    def do_all(self, f, iterator):
        with ThreadPoolExecutor(max_workers=self.max_workers) as tp:
            futures = set()
            for x in iterator:
                print(f'Putting {x}')
                future = tp.submit(f, x)
                futures.add(future)

                while len(futures) > self.max_pending:
                    done, futures = wait(futures, None, FIRST_COMPLETED)
                    for r in done:
                        yield r.result()

            while futures:
                done, futures = wait(futures, None, FIRST_COMPLETED)
                for r in done:
                    yield r.result()


