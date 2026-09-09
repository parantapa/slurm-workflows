# How to resume a search

[<- back to the main README](../../README.md)

A search that ran out of walltime does not have to start again.
`OptimizeSpaceBotorch` is constructed from results files,
so the state that has to survive is a file, not an object.

## Save every phase

```python
sweep.save(EXPLORE_RESULTS)
...
opt.save(SEARCH_RESULTS)
```

`opt.save` holds only the points **this run** evaluated,
not the ones it read from the files it started with.

## Start the next run from every file so far

```python
OptimizeSpaceBotorch(tasks, executor, [EXPLORE_RESULTS, SEARCH_RESULTS])
```

Because each `save` writes only its own run's points,
passing all of them counts every point once.
A third run passes three files, and so on.

## Keep the task name the same

A task is modelled on the observations the files hold **under its name**,
so the `name` on the `OptimizationTask` must be the name
the exploration and the earlier searches ran under.
A task with nothing under its name in any file is an error,
not a search with no model.

## Keep the space the same

The saved points are re-checked against the space the task declares.
A parameter missing or one too many,
or a range since narrowed past a saved point,
is reported rather than fitted on.
Widening a range is safe;
narrowing one past points you have already measured is not.

## Related

- [`OptimizeSpaceBotorch`](../reference/optimize-space.md)
- [Tutorial: optimizing Himmelblau's function](../tutorials/optimizing-himmelblau.md)
