# How to keep per-worker state with actors

[<- back to the main README](../../README.md)

Some tasks load something expensive before they do any work:
a model, a database connection or a large table.
A load that runs once per task wastes most of the run.
Register an **actor class** instead.
Each worker creates one actor from the class, once, at startup.
You pass method names (as strings) instead of functions.

## Write the actor class

Put the expensive work in `__init__`,
and the per-task work in a method:

```python
# my_pkg/model.py
class Model:
    def __init__(self):
        self.model = load_expensive_model()   # runs once per worker

    def predict(self, x):
        return self.model(x)

    def close(self):                           # optional cleanup hook
        self.model.release()
```

The worker calls an optional `close()` when Slurm ends the pilot job,
and when `restart_jobs` restarts the worker.
But a SIGKILL or a node failure can cut `close()` short, or skip it.
So keep `close()` short.
Do not rely on `close()` for anything the next run needs.
For when `close()` runs, see
[The `slurm-pilot-worker` entry point](../reference/executor.md#the-slurm-pilot-worker-entry-point).

The class must be importable on the compute node.
By default, each worker adds the driver's current working directory
to its own `sys.path`.
That directory is the one that is current when you call `define_job_group`.
Add more paths with `python_paths=[...]`.

## Name the class when you define the job group

```python
executor.define_job_group(
    name="gpu",
    sbatch_args=["-A my_alloc", "-p gpu", "--gres=gpu:1", "-t 02:00:00"],
    setup_script=SETUP_SCRIPT,
    actor_class_name="my_pkg.model.Model",
)
executor.scale_jobs("gpu", 2)
```

## Map with a method name instead of a function

Give `map` the method name as its `map_fn`:

```python
predictions = executor.map("gpu", "predict", dataset, desc="predict")
```

The `map` call then runs against the actor, not against a shipped function.
`map_reduce` takes a method name for its `map_fn` too,
and its `reduce_fn` stays a callable.
For the rest, see
[`map`](../reference/map.md#mapping-with-an-actors-method)
and [`map_reduce`](../reference/map-reduce.md#mapping-with-an-actors-method).

## If you submit tasks one at a time

Pass the method name to `submit` in place of a function:

```python
tasks = [executor.submit("gpu", "predict", item) for item in dataset]
executor.wait(tasks, desc="predict")
```

If you also have work that needs no actor,
submit it to this same job group.
The worker reads a string as a method name, and a callable as itself.
So a job group with an actor serves ordinary tasks as well.

## If the class takes constructor arguments

Pass them with `actor_class_args` and `actor_class_kwargs`:

```python
class Model:
    def __init__(self, checkpoint, device="cpu"):
        self.model = load_expensive_model(checkpoint, device)

executor.define_job_group(
    name="gpu",
    sbatch_args=["-A my_alloc", "-p gpu", "--gres=gpu:1", "-t 02:00:00"],
    actor_class_name="my_pkg.model.Model",
    actor_class_args=["/project/checkpoints/v3.pt"],
    actor_class_kwargs={"device": "cuda"},
)
```

The constructor arguments travel through the server, so they must be picklable.
Anything they refer to must be importable on the compute node,
exactly as for the actor class itself.

## If you change the arguments mid-run

Each worker creates its actor once, at startup.
So only the workers that start after you redefine the job group
read the new arguments.
To rebuild the actors in the running pilot jobs,
call [`restart_jobs`](../reference/executor.md#restart_jobs),
as [How to update worker code without resubmitting](update-worker-code-without-resubmitting.md) shows.

## Related

- [`define_job_group` options](../reference/executor.md#define_job_group-options)
- [How to troubleshoot a failing run](troubleshoot-a-failing-run.md),
    for a `ModuleNotFoundError` from an actor's constructor
