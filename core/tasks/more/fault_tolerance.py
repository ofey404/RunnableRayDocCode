#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/tasks/fault-tolerance.html

import numpy as np
import os
import ray
import time

ray.init(ignore_reinit_error=True)

# If a worker dies unexpectedly, Ray will rerun the task 
# until either the task succeeds or the maximum number of
# retries is exceeded. 

@ray.remote(max_retries=1)
def potentially_fail(failure_probability):
    time.sleep(0.2)
    if np.random.random() < failure_probability:
        print("potentially_fail failed!")
        os._exit(0)

    print("potentially_fail succeeded!")
    return 0

for _ in range(3):
    try:
        # If this task crashes, Ray will retry it up to one additional
        # time. If either of the attempts succeeds, the call to ray.get
        # below will return normally. Otherwise, it will raise an
        # exception.
        ray.get(potentially_fail.remote(0.5))
        print('SUCCESS')
    except ray.exceptions.WorkerCrashedError:
        print('FAILURE')