# Reference

[<- back to the main README](../README.md)

The three classes a program built on `slurm-workflows` is written against:

| Class | Reach for it when |
| --- | --- |
| `SlurmPilotExecutor` | You know what the tasks are and want them run on the cluster |
| `ExploreSpaceSobolQMC` | The work is "evaluate this function over this space" |
| `OptimizeSpaceBotorch` | The work is "find where this function is smallest" |

All three, and everything they take as arguments,
can be imported from the top-level `slurm_workflows` package.

The two space classes are built on the first:
both take an executor and submit through it,
so a program always starts by building one.

For the executor in full - worker groups, actors, logs, troubleshooting -
see [Concepts and usage](concepts.md);
for worked programs, the tutorials listed in the
[README](../README.md#documentation).

## `SlurmPilotExecutor`

```python
from slurm_workflows import SlurmPilotExecutor

executor = SlurmPilotExecutor(name, server_address, work_dir=None)
```

`name` identifies the run: it prefixes task ids and Slurm job names,
so two executors on one cluster need two names.
`server_address` is the `host:port` of a running `ds-service` server.
Give each executor a server of its own:
everything on a server is taken to belong to one run,
so two executors pointed at one of them share a queue namespace.
`work_dir` defaults to a timestamped directory in your cache dir,
and everything the run writes lands there.

Use it as a context manager.
Leaving the block cancels every pilot job,
including when the block raises:

```python
with SlurmPilotExecutor("my-run", address) as executor:
    ...
```

| Method | What it does |
| --- | --- |
| `define_worker(name, sbatch_args, ...)` | Describe a kind of worker. Submits nothing. The group name is also the queue name. |
| `scale_workers(name, count)` | Submit or cancel pilot jobs so the group has `count` of them. |
| `submit(queue, fn, *args, **kwargs)` | Enqueue one task and return a `Task` straight away. `fn` is a callable, or a method name for actor workers. |
| `set_task_name(task, name)` | Give a task a name to be read by, `swtop` included. |
| `as_completed(tasks, desc, unit="task", raise_on_error=...)` | Yield tasks as their results arrive. `desc` and `unit` label the progress `swtop` draws. |
| `wait(tasks, desc, unit="task", raise_on_error=...)` | Block until every task is done, labelled the same way. |
| `stop()` | Cancel the pilot jobs, keep the executor usable. |
| `close()` | Cancel the pilot jobs and close the connection. |

A whole run is those five calls:

```python
with SlurmPilotExecutor("my-run", address) as executor:
    executor.define_worker(name="cpu", sbatch_args=SBATCH_ARGS, setup_script=SETUP)
    executor.scale_workers("cpu", 1)

    tasks = [executor.submit("cpu", square, i) for i in range(100)]
    executor.wait(tasks, desc="squaring")

results = [task.output for task in tasks]
```

Submitting before the workers exist is fine:
tasks wait on the queue until something pulls them.

**What comes back.** `submit` returns a `Task`.
Its `output` is a sentinel until the task completes,
then the function's return value,
or a `RemoteExecutionError` if the worker raised.
`wait` and `as_completed` fill those in,
and what they do about a failure is the `raise_on_error` argument:
see [`RaiseOnError`](concepts.md#raiseonerror).

## `ExploreSpaceSobolQMC`

```python
from slurm_workflows import ExplorationTask, ExploreSpaceSobolQMC

sweep = ExploreSpaceSobolQMC(tasks, executor, num_exploration_points=None)
```

Draws a Sobol' design over each space it is given,
evaluates every point of every design across the pool,
and keeps what came back.
`tasks` is a **list** of `ExplorationTask`s, one per space:
they are submitted together, so a small sweep does not wait for a large one.
`num_exploration_points` is the count for tasks that do not carry their own.

Needs neither botorch nor torch, on the driver or on the workers.
Use it for a first look at a space,
for a baseline to judge a search against,
when one wave of evaluations is the whole budget,
or as the first phase of a search.

An `ExplorationTask` is:

| Field | Meaning |
| --- | --- |
| `name` | Names the task. Keys the results and labels the tasks on the queue. |
| `space` | The search space: one entry per objective argument. |
| `objective` | The function to evaluate. Its argument names must match the space's keys. |
| `objective_queue` | Queue, or queues, the evaluations go to. |
| `num_exploration_points` | Points to draw. Truncated down to a power of two. Optional if the sweep carries a default. |
| `seed` | Optional. The same seed redraws the same design; without one, a seed is drawn and printed. |
| `objective_key` | Which entry of the objective's result is the value. `"objective"` by default. |
| `extra_objective_kwargs` | Extra arguments passed to the objective and not varied. |

### The objective

The same contract holds for both space classes.

The objective runs on a worker, once per point.
Its argument names must match the keys of `space`,
and it receives them as keyword arguments.
It is cloudpickled like any other task,
so a closure or a lambda is fine,
as long as what it imports exists on the compute node.

It returns a **mapping**, not a bare number.
The entry under `objective_key` is the value,
lower being better, so negate a score you would rather maximize.
Only that entry is ranked or modelled;
every other entry is recorded, which is where a runtime,
a checkpoint path or an unoptimized metric goes.

A bare float, a mapping without the key,
or a value that is not a finite float
each raise rather than being coerced -
`NaN` and `inf` included, since either one silently poisons a GP fit.

`extra_objective_kwargs` carries what the objective needs
but the search must not vary.
It may not shadow a key of `space`, which is rejected when the run is built.

### Failures

Both classes block until every point in flight has come back.
A worker that raises does not raise on the driver,
so both wait with `RaiseOnError.RAISE_AFTER_COMPLETED`
and turn what came back into a `RuntimeError` naming the tasks that failed,
rather than feeding a `RemoteExecutionError` into a model.
One bad evaluation therefore does not hide the rest of its batch,
and what did come back is recorded before the exception is raised,
so `save()` still holds the good points and the next run resumes from them.

| Method | What it does |
| --- | --- |
| `design(name)` | The points a task will evaluate, without evaluating them. |
| `dim(name)` | How many dimensions a task's space has. |
| `run()` | Submit every point of every task and block until all are back. |
| `best_point(name)` | `(params, value)` of the lowest value the task measured. |
| `best_output(name)` | The objective's whole result at that point. |
| `save(path)` | Write the points, values and outputs to a gzipped pickle. |

```python
sweep = ExploreSpaceSobolQMC(
    [ExplorationTask("sweep", SPACE, objective, "cpu", 4096, seed=1)],
    executor,
)
sweep.run()
sweep.save("sweep.pkl.gz")

result = sweep.results["sweep"]     # points, values, outputs, unit_points
```

`sweep.results[name]` holds four index-aligned lists,
in submission order:
the `points` evaluated, the `values` ranked,
the whole `outputs`, and `unit_points`, the points in the unit cube.
`sweep.tasks` is the task list with the point count and seed filled in;
the caller's own `ExplorationTask` objects are left alone.

Calling `run()` again re-evaluates the same design:
the seed decides the draw, so there is no "next 4096 points".
Pass a different seed for fresh ground.

The point count is floored to a power of two
because that is the prefix length at which a Sobol' sequence is balanced.
64 workers asking for 100 points would evaluate 64
and leave the rest idle, so size a sweep in powers of two on purpose.

Read a saved file back with `load_results(paths)`,
which merges several files by task name.
The file is a gzipped plain pickle - measurements, not code -
keyed by task name, each holding `points`, `values` and `outputs`
as index-aligned lists in submission order:

```python
import gzip, pickle

with gzip.open("sweep.pkl.gz", "rb") as fobj:
    results = pickle.load(fobj)

results["sweep"]["points"]     # the parameters of each evaluation
```

## `OptimizeSpaceBotorch`

```python
from slurm_workflows import OptimizationTask, OptimizeSpaceBotorch

opt = OptimizeSpaceBotorch(tasks, executor, files, search_parallelism=None)
```

Fits a Gaussian process to everything measured so far,
asks it for a batch of points at once, evaluates that batch, and repeats.
It needs botorch, which is an optional dependency:

```sh
pip install -U "slurm-workflows[botorch]"
```

`tasks` is a **list**, as for a sweep:
several spaces are searched in the same rounds
rather than one waiting for the other to finish with the pool,
and each drops out when it meets its own stopping rule.

It never explores.
`files` are results files to start from, as written by
`ExploreSpaceSobolQMC.save` or by this class's own `save`,
and a task is modelled on the observations they hold **under its name**,
so an optimization task has to carry the name its exploration ran under.
Passing the exploration file and every search file since
is how a run is resumed.

A task with nothing under its name in any file is an error
rather than a search with no model,
and the points are re-checked against the space the task declares:
a parameter missing or one too many,
or a range since narrowed past a saved point,
is reported rather than fitted on.

**What a round is.** `run()` starts rounds until the search stops improving.
Each round fits a `SingleTaskGP` to every point measured so far,
asks `qLogNoisyExpectedImprovement` for the whole batch in one call,
submits all of it, and waits.
The batch is chosen jointly rather than a point at a time,
so the proposals do not stack on one spot.
The noisy variant reads the incumbent off the posterior
at the points already evaluated,
so an evaluation that came back lucky does not become a target
the search chases; it also carries every point measured so far,
which is why a fit costs more every round.
`run()` can be called again for another set of rounds,
modelling everything the earlier calls measured.

**When it stops.** A round is *stalled* when it fails to improve
the best value by `min_improvement`, a fraction of the incumbent's
magnitude, so the setting means the same thing
whether the objective is in seconds or in dollars.
`patience` stalled rounds **in a row** end the search,
and an improving round resets the streak.
`min_search_iterations` is a floor on rounds *run*, not on rounds counted:
a stalled round below it still counts towards `patience`,
it just cannot be the round that ends the search,
so the earliest stop is `max(min_search_iterations, patience)` rounds
and a search that never improves stops at the floor exactly.
`max_search_iterations` is reached even while still improving.
Each round says which bound is binding and how far it has to go.

**Where the work runs.** Nothing heavy runs on the driver.
A round is two kinds of task on two queues,
each kind submitted for every task at once and waited for once:

| Queue | Tasks per round | Needs |
| --- | --- | --- |
| `objective_queue` | `search_parallelism` evaluations | whatever the objective needs |
| `optimizer_queue` | one fit-and-propose: the GP fit and the acquisition optimization | botorch, cores, and memory for a GP over every point measured so far |

Two arguments because the two want different nodes:
an evaluation is one call, `search_parallelism` at a time,
while the fit is a single task that threads across cores
and grows superlinearly with the number of observations.
Pointing both at one queue is supported and cannot deadlock,
since a round never has both kinds in flight at once;
the fit then waits for a slot in a pool sized for the objective.

botorch has to be importable on the driver
and in the `optimizer_queue` workers' environment.
Workers serving only `objective_queue` need neither botorch nor torch.
A fit that fails to import it raises on the driver naming the queue,
with the traceback in a worker log under `executor.work_dir`.

An `OptimizationTask` is the exploration task's fields,
minus the design ones, plus the search:

| Field | Default | Meaning |
| --- | --- | --- |
| `name`, `space`, `objective`, `objective_queue` | | As for `ExplorationTask`. The value is **minimized**. |
| `optimizer_queue` | | Queue the model fit and the proposal run on, one task per round. |
| `search_parallelism` | | Points evaluated per round. Match it to the pool. Optional if the search carries a default. |
| `min_search_iterations` | `5` | Rounds that always run. |
| `max_search_iterations` | `30` | Hard ceiling on rounds. |
| `patience` | `3` | Consecutive rounds without improvement that end the search. |
| `min_improvement` | `0.05` | Fraction a round must beat the incumbent by to count as improving. |
| `objective_key` | `"objective"` | Which entry of the result is minimized. |
| `num_restarts`, `raw_samples`, `mc_samples`, `acqf_timeout_s` | `10`, `128`, `128`, `10.0` | Tuning for the proposal step. |
| `extra_objective_kwargs` | `{}` | Extra arguments passed to the objective and not varied. |

| Method | What it does |
| --- | --- |
| `run()` | Run rounds until every task stops, by its patience or its ceiling. |
| `best_point(name)` | `(params, value)` of the best point the task knows, files included. |
| `best_output(name)` | The objective's whole result at that point. |
| `observations(name)`, `num_observations(name)` | What the task's model is fit on, and how much of it. |
| `dim(name)` | How many dimensions a task's space has. |
| `save(path)` | Write **this run's** points to a gzipped pickle. |

```python
opt = OptimizeSpaceBotorch(
    [OptimizationTask("sweep", SPACE, objective, "cpu", "opt", 40)],
    executor,
    ["sweep.pkl.gz"],
)
opt.run()
opt.save("search.pkl.gz")

params, value = opt.best_point("sweep")
```

The next run carries on from both files:

```python
OptimizeSpaceBotorch(tasks, executor, ["sweep.pkl.gz", "search.pkl.gz"])
```

`save` writes only what this instance evaluated,
so passing both counts every point once.
`opt.results[name]` is that same set of points as lists
and `opt.prior[name]` is what the files held, in the same shape;
`best_point` and `observations` cover both.
`opt.tasks` is the task list with the parallelism filled in.

**Tuning the proposal.** Four task arguments tune the acquisition
optimization. They are settings of one run, kept on the task
and passed to every fit, so nothing has to be redeployed to change them:

| Argument | Default | What it buys |
| --- | --- | --- |
| `num_restarts` | `10` | Multi-start count. The acquisition surface is multimodal, so a single start routinely lands in a local optimum. |
| `raw_samples` | `128` | Candidates drawn to pick those starting points from. |
| `mc_samples` | `128` | Quasi-MC draws per acquisition evaluation. Sobol' draws are stratified, so they carry further than the same number of independent normal draws. |
| `acqf_timeout_s` | `10.0` | Wall-clock budget for one proposal. Hitting it is not an error: what comes back is a full batch, finite and inside the bounds, just less thoroughly optimized. Raise it when one evaluation is expensive enough that a better batch is worth the minutes. |

```python
OptimizationTask(..., num_restarts=20, acqf_timeout_s=60.0)
```

The run reports itself as it goes:
the best point after every round,
how long each fit and each proposal took,
and why a task stopped.

## Search spaces

Both space classes take the same kind of space:
a mapping from **objective argument name** to a range.

```python
from slurm_workflows import IntRange, FloatRange, CategoricalRange

SPACE = {
    "layers": IntRange(1, 8),
    "learning_rate": FloatRange(1e-5, 1e-1, log_range=True),
    "optimizer": CategoricalRange(2),
}
```

| Range | The objective receives | Notes |
| --- | --- | --- |
| `IntRange(min, max)` | An `int` in `[min, max]` | |
| `FloatRange(min, max)` | A `float` in `[min, max]` | |
| `FloatRange(min, max, log_range=True)` | A `float` in `[min, max]` | Searched in log space, so each decade gets equal budget. Needs `min > 0`. |
| `CategoricalRange(n)` | An `int` in `[0, n - 1]` | An index into your own list of values. `n = 1` is allowed but is a dead dimension: drop the parameter and pass the value through `extra_objective_kwargs`. |

One space may mix all three.
A parameter the search should not vary
belongs in `extra_objective_kwargs` rather than in the space.

Every parameter is mapped into `[0, 1]` before a model sees it
and mapped back for the objective,
which is what lets one model span all the kinds at once.
Integer and categorical parameters come back by rounding
a continuous proposal, so on a mostly-discrete space with few levels
expect a search to re-propose points it has already evaluated.
What is recorded is where the objective actually ran, after rounding,
not the continuous proposal.
