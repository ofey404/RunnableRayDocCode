#!/bin/env python

# https://docs.ray.io/en/latest/ray-core/tasks/using-ray-with-gpus.html

import ray
import time

import os

@ray.remote(num_gpus=1)
def use_gpu():
    print("ray.get_gpu_ids(): {}".format(ray.get_gpu_ids()))
    print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))


# ## Share GPU resource
# 
# If you want two tasks to share the same GPU, 
# then the tasks can each request half (or some
# other fraction) of a GPU.
# 
# It is the developer’s responsibility to make
# sure that the individual tasks don’t use more
# than their share of the GPU memory. 

@ray.remote(num_gpus=0.25)
def fractional_gpu():
    # It is the developer’s responsibility to make sure that the individual
    # tasks don’t use more than their share of the GPU memory. 
    time.sleep(1)


# ## How to release GPU resource
# 
# Setting max_calls=1 in the remote decorator,
# so that the worker automatically exits after 
# executing the task (thereby releasing the GPU
# resources).

worker_not_release_gpu = False
if worker_not_release_gpu:  # Use max_calls=1.
    import tensorflow as tf

    @ray.remote(num_gpus=1, max_calls=1)
    def leak_gpus():
        # This task will allocate memory on the GPU and then never release it, so
        # we include the max_calls argument to kill the worker and release the
        # resources.
        sess = tf.Session()

if __name__ == "__main__":
    ray.init()
    obj_ref = use_gpu.remote()
    # Waiting till use_gpu return.
    ray.get(obj_ref)