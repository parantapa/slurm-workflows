# How to keep per-worker state with actors

[<- back to the main README](../../README.md)

Some tasks load something expensive before they do any work:
a model, a database connection or a large table.
A load that runs once per task wastes most of the run.
Register an **actor class** instead.
Each worker creates it once at startup,
and you dispatch **method names** (as strings) instead of functions.

## 1. Write the class

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

The class must be importable on the compute node.
By default, each worker adds the executor's current working directory
to its own `sys.path`.
Add more paths with `python_paths=[...]`.

## 2. Name the class when you define the worker group

```python
executor.define_worker(
    name="gpu",
    sbatch_args=["-A my_alloc", "-p gpu", "--gres=gpu:1", "-t 02:00:00"],
    setup_script=SETUP_SCRIPT,
    actor_class_name="my_pkg.model.Model",
)
executor.scale_workers("gpu", 2)
```

## 3. Submit method names instead of functions

```python
tasks = [executor.submit("gpu", "predict", item) for item in dataset]
executor.wait(tasks, desc="predict")
```

## If the class takes constructor arguments

Pass them with `actor_class_args` and `actor_class_kwargs`:

```python
class Model:
    def __init__(self, checkpoint, device="cpu"):
        self.model = load_expensive_model(checkpoint, device)

executor.define_worker(
    name="gpu",
    sbatch_args=["-A my_alloc", "-p gpu", "--gres=gpu:1", "-t 02:00:00"],
    actor_class_name="my_pkg.model.Model",
    actor_class_args=["/project/checkpoints/v3.pt"],
    actor_class_kwargs={"device": "cuda"},
)
```

They travel through the queue server, so they must be picklable.
Anything they refer to must be importable on the compute node,
exactly as for the actor class itself.

## If you change the arguments mid-run

You can redefine a group with different actor arguments.
Different `sbatch_args` raise an `AssertionError` instead.
Only the workers that start after that call read the new values.
The reason is that each worker creates its actor once, at startup.
Scale the group down and back up to rebuild the actors.

## Related

- [`define_worker` options](../reference/executor.md#define_worker-options)
- [How to troubleshoot a failing run](troubleshoot-a-failing-run.md),
    for a `ModuleNotFoundError` from an actor's constructor
