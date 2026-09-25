# How to keep per-worker state with actors

[<- back to the main README](../../README.md)

Some tasks load something expensive before they do any work:
a model, a database connection or a large table.
A load that runs once per task wastes most of the run.
Register an **actor class** instead.
Each worker creates it once at startup,
and you dispatch **method names** (as strings) instead of functions.

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

Slurm ends a pilot job, at its time limit or through `scancel`,
without the worker calling `close()`.

The class must be importable on the compute node.
By default, each worker adds the executor's current working directory
to its own `sys.path`.
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

## Submit method names instead of functions

```python
tasks = [executor.submit("gpu", "predict", item) for item in dataset]
executor.wait(tasks, desc="predict")
```

If you also have work that needs no actor,
submit it to this same job group.
The worker reads a string as a method name, and a callable as itself.
So a job group with an actor serves ordinary tasks as well.

## If you want `mapreduce` to map with the actor

Give `mapreduce` a method name for its `map_fn`.
The map then runs against the actor, not against a shipped function.
`reduce_fn` stays a callable, because that fold also runs on the driver.
For the rest, see
[How to fold results across workers](fold-results-across-workers.md).

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

They travel through the server, so they must be picklable.
Anything they refer to must be importable on the compute node,
exactly as for the actor class itself.

## If you change the arguments mid-run

Each worker creates its actor once, at startup.
You can redefine a job group with different actor arguments,
but only the workers that start after that call read the new values.
Scale the job group down and back up to rebuild the actors.
A redefinition with different `sbatch_args` raises an `AssertionError` instead.

## Related

- [`define_job_group` options](../reference/executor.md#define_job_group-options)
- [How to fold results across workers](fold-results-across-workers.md)
- [How to troubleshoot a failing run](troubleshoot-a-failing-run.md),
    for a `ModuleNotFoundError` from an actor's constructor
