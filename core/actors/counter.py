#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/actors.html

import ray


@ray.remote
class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def get_counter(self):
        return self.value


if __name__ == "__main__":
    ray.init()

    print("Create an actor and call it.")
    # Create an actor from this class.
    counter = Counter.remote()

    # Call the actor.
    obj_ref = counter.increment.remote()
    assert ray.get(obj_ref) == 1

    print("Create ten Counter actors.")
    counters = [Counter.remote() for _ in range(10)]

    # Increment each Counter once and get the results. These tasks all happen in
    # parallel.
    results = ray.get([c.increment.remote() for c in counters])
    print(
        f"Increment each Counter once, results: {results}"
    )  # prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

    # Increment the first Counter five times. These tasks are executed serially
    # and share state.
    results = ray.get([counters[0].increment.remote() for _ in range(5)])
    print(
        f"Increment the first Counter five times, result for each time: {results}"
    )  # prints [2, 3, 4, 5, 6]

    results = ray.get([c.get_counter.remote() for c in counters])
    print(
        f"Increment each Counter once, results: {results}"
    )  # prints [6, 1, 1, 1, 1, 1, 1, 1, 1, 1]
