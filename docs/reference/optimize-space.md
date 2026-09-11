# `OptimizeSpaceBotorch`

[<- back to the main README](../../README.md)

`slurm_workflows.optimize_space_botorch`:
the batch Bayesian search, its task dataclass, and its settings.

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

`tasks` is a **list**, as for a sweep.
`OptimizeSpaceBotorch` searches several spaces in the same rounds,
and each drops out when it meets its own stopping rule.

It never explores.
`files` are results files to start from,
which `ExploreSpaceSobolQMC.save` or this class's own `save` wrote.
`OptimizeSpaceBotorch` models a task
on the observations they hold **under its name**,
so an optimization task must carry the name its exploration ran under.
To resume a run, pass the exploration file and every search file since.
See [How to resume a search](../how-to-guides/resume-a-search.md).

A task with nothing under its name in any file is an error.
`OptimizeSpaceBotorch` re-checks the points against the space the task declares.
It reports a parameter missing, one too many,
or a range since narrowed past a saved point,
rather than fit on them.

The objective contract is the same as for a sweep:
see [The objective](explore-space.md#the-objective).

## What a round is

`run()` starts rounds until the search stops improving.
Each round fits a `SingleTaskGP` to every point measured so far.
Then it asks `qLogNoisyExpectedImprovement` for the whole batch in one call,
submits all of it, and waits.
It chooses the batch jointly rather than a point at a time.

`qLogNoisyExpectedImprovement` reads its incumbent off the posterior
at the points already evaluated.
It carries every point measured so far, so a fit costs more every round.

You can call `run()` again for another set of rounds.
The new rounds model everything the earlier calls measured.

Why a round chooses the whole batch at once, and why the fit runs on a worker,
is in
[About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md).

## When it stops

A round is *stalled* when it fails to improve the best value by `min_improvement`,
a fraction of the incumbent's magnitude.
`patience` stalled rounds **in a row** end the search,
and an improving round resets the streak.

`min_search_iterations` is a floor on rounds *run*, not on rounds counted.
A stalled round below it still counts toward `patience`,
but it cannot be the round that ends the search.
The earliest stop is therefore `max(min_search_iterations, patience)` rounds,
and a search that never improves stops there exactly.
`max_search_iterations` stops the search even while it still improves.
Each stalled round reports how far it has to go,
under whichever bound is further away.

## Where the work runs

Nothing heavy runs on the driver.
A round is two kinds of task on two queues.
`OptimizeSpaceBotorch` submits each kind for every task at once,
and waits for it once:

| Queue | Tasks per round | Needs |
| --- | --- | --- |
| `objective_queue` | `search_parallelism` evaluations | whatever the objective needs |
| `optimizer_queue` | one fit-and-propose: the GP fit and the acquisition optimization | botorch, cores, and memory for a GP over every point measured so far |

You can point both at one queue, and that cannot deadlock,
because a round never has both kinds in flight at once.

botorch must be importable on the driver
and in the `optimizer_queue` workers' environment.
Workers that serve only `objective_queue` need neither botorch nor torch.
A fit that fails to import it raises on the driver and names the queue.
The traceback is in a worker log under `executor.work_dir`.

## `OptimizationTask`

The exploration task's fields, minus the design ones, plus the search:

| Field | Default | Meaning |
| --- | --- | --- |
| `name`, `space`, `objective`, `objective_queue` | | As for `ExplorationTask`. The value is **minimized**. |
| `optimizer_queue` | | Queue the model fit and the proposal run on, one task per round. |
| `search_parallelism` | | Points evaluated per round. Optional if the search carries a default. |
| `min_search_iterations` | `5` | Rounds that always run. |
| `max_search_iterations` | `30` | Hard ceiling on rounds. |
| `patience` | `3` | Consecutive rounds without improvement that end the search. |
| `min_improvement` | `0.05` | Fraction a round must beat the incumbent by to count as improving. |
| `objective_key` | `"objective"` | Which entry of the result is minimized. |
| `num_restarts`, `raw_samples`, `mc_samples`, `acqf_timeout_s` | `10`, `128`, `128`, `10.0` | Tuning for the proposal step. |
| `extra_objective_kwargs` | `{}` | Extra arguments passed to the objective and not varied. |

## Methods

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

`save` writes only what this instance evaluated,
so you can pass an earlier file alongside it and count every point once.
`opt.results[name]` is that same set of points as lists,
and `opt.prior[name]` is what the files held, in the same shape.
`best_point` and `observations` cover both.
`opt.tasks` is the task list with the parallelism filled in.

## Tuning the proposal

Four task arguments tune the acquisition optimization.
They are settings of one run.
`OptimizeSpaceBotorch` keeps them on the task and passes them to every fit:

| Argument | Default | What it controls |
| --- | --- | --- |
| `num_restarts` | `10` | Multi-start count for the acquisition optimization. |
| `raw_samples` | `128` | Candidates drawn to pick those starting points from. |
| `mc_samples` | `128` | Quasi-MC draws per acquisition evaluation. |
| `acqf_timeout_s` | `10.0` | Wall-clock budget for one proposal. Hitting it is not an error: what comes back is a full batch, finite and inside the bounds, less thoroughly optimized. |

```python
OptimizationTask(..., num_restarts=20, acqf_timeout_s=60.0)
```

The run reports itself as it goes.
It gives the best point after every round,
how long each fit and each proposal took,
and why a task stopped.
