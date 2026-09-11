# How to resume a search

[<- back to the main README](../../README.md)

A search that ran out of walltime does not have to start again.
You construct `OptimizeSpaceBotorch` from results files.
So the state that must survive is a file, not an object.

## Save every phase

```python
sweep.save(EXPLORE_RESULTS)
...
opt.save(SEARCH_RESULTS)
```

`opt.save` holds only the points this run evaluated,
not the ones it read from the files it started with.

## Start the next run from every file so far

```python
OptimizeSpaceBotorch(tasks, executor, [EXPLORE_RESULTS, SEARCH_RESULTS])
```

Each `save` writes only its own run's points.
For this reason, the full list of files counts every point once.
A third run passes three files, and so on.

## Keep the task name the same

The optimizer models a task on the observations the files hold under its name.
So the `name` on the `OptimizationTask` must be the name
the exploration and the earlier searches ran under.
A task with nothing under its name in any file is an error,
not a search with no model.

## Keep the space the same

The constructor re-checks every saved point against the space the task declares.
Three kinds of mismatch raise `RuntimeError`.
The first is a point whose parameters do not match the space.
The second is a point outside a range the task now declares.
The third is a point a log range cannot place, at or below zero.
You can widen a range.
Do not narrow one past a point you already measured.

## Related

- [`OptimizeSpaceBotorch`](../reference/optimize-space.md)
- [Optimizing Himmelblau's function](../tutorials/optimizing-himmelblau.md)
