#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/tasks.html

# Remote functions can be canceled by calling ray.cancel 
# (docstring) on the returned Object ref.

import ray
import time

from ray.exceptions import TaskCancelledError


@ray.remote
def blocking_operation():
    print("Execute blocking_operation.")
    time.sleep(10e6)


if __name__ == "__main__":
    ray.init()

    obj_ref = blocking_operation.remote()
    # WARNING: If blocking operation is sleeping, cancel won't take effect.
    # time.sleep(1)
    ray.cancel(obj_ref)
    print("Tried to cancel task.")

    try:
        ray.get(obj_ref)
    except TaskCancelledError:
        print("Object reference was cancelled.")
