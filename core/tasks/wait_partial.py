#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/tasks.html

# After launching a number of tasks, you may want to
# know which ones have finished executing without 
# blocking on all of them, as in ray.get. This can 
# be done with wait (ray.wait). The function works 
# as follows.

import ray
import time

from slow_function import my_function, slow_function

if __name__ == "__main__":
    ray.init()

    t = time.time()
    object_refs = [my_function.remote(), slow_function.remote()]
    ready_refs, remaining_refs = ray.wait(object_refs, num_returns=1, timeout=None)

    print(f"ray.get(ready_refs) = {ray.get(ready_refs)} in {time.time() - t}s")