#!/bin/env python

# https://docs.ray.io/en/latest/ray-core/actors/named-actors.html

# An actor can be given a unique name within their namespace.

# ## PublicAPI of Ray remote
#
# https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-remote
#
# ray.remote_function.RemoteFunction.options(
#   self, args=None, kwargs=None, num_returns=None, num_cpus=None, num_gpus=None, memory=None, object_store_memory=None, accelerator_type=None, resources=None, max_retries=None, retry_exceptions=None, placement_group='default', placement_group_bundle_index=- 1, placement_group_capture_child_tasks=None, runtime_env=None, name='', scheduling_strategy: Union[None, str, ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy] = None
# )


import ray


@ray.remote
class Actor:
    pass


def retrieve_with_name():
    # Create an actor with a name
    actor = Actor.options(name="some_name").remote()

    print(f"Create an actor with handle: {actor}")

    # Retrieve the actor later somewhere
    handle = ray.get_actor("some_name")

    print(f"Get an actor by name, handle: {handle}")


def namespace():
    # Creates an actor, "orange" in the "colors" namespace.
    actor = Actor.options(
        name="orange", namespace="colors", lifetime="detached"
    ).remote()
    print(f"Create an actor with handle: {actor}")

    # driver_2.py
    # Job 2 is now connecting to a different namespace.
    # This fails because "orange" was defined in the "colors" namespace.
    try:
        ray.get_actor("orange", namespace="fruit")
    except ValueError:
        print("[error] Find 'orange' in a different namespace will fail.")

    # driver_3.py
    # Job 3 connects to the original "colors" namespace
    # This returns the "orange" actor we created in the first job.
    handle = ray.get_actor("orange", namespace="colors")
    print(f"Get it in the same namespace: {handle}")


if __name__ == "__main__":
    ray.init()
    print("## Retrieve actor by name:")
    retrieve_with_name()
    print()

    print("## Use namespace to control access:")
    namespace()
