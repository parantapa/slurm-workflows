# How to resume a search

[<- back to the main README](../../README.md)

A search that ran out of its time limit does not have to start again.
You construct `OptimizeSpaceBotorch` from results files.
So the state that must survive is a file, not an object.

## Give every run its own results file

`save` overwrites the path you hand it,
and it writes only the points of the run that called it.
Where a run saves over a file it started from,
it destroys the points that file held.

Number the files, and keep every one of them:

```python
EXPLORE_RESULTS = Path("explore.pkl.gz")
SEARCH_RESULTS = Path(f"search-{run_number}.pkl.gz")
```

## Save every phase

```python
with SlurmPilotExecutor("search", address) as executor:
    exploration = ExploreSpaceSobolQMC(exploration_studies, executor)
    exploration.run()
    exploration.save(EXPLORE_RESULTS)

    opt = OptimizeSpaceBotorch(optimization_studies, executor, [EXPLORE_RESULTS])
    opt.run()
    opt.save(SEARCH_RESULTS)
```

`opt.save` holds only the points this run evaluated,
not the ones it read from the files it started with.

## Start the next run from every file so far

```python
opt = OptimizeSpaceBotorch(
    optimization_studies, executor, [EXPLORE_RESULTS, *earlier_search_results]
)
opt.run()
opt.save(SEARCH_RESULTS)
```

Each `save` writes only its own run's points.
For this reason, the full list of files counts every point once.
A third run passes three files, and so on.

## Keep the study name the same

The optimizer models a study on the observations the files hold under its name.
So the `name` on the `OptimizationStudy` must be the name
the exploration and the earlier searches ran under.
A study with nothing under its name in any file is an error,
not a search with no model.

## Keep the space the same

The constructor re-checks every saved point against the space the study
declares, and three kinds of mismatch raise `RuntimeError`:

- A point whose parameters do not match the space.
- A point outside a range the study now declares.
- A point a log range cannot place, at or below zero.

You can widen a range.
Do not narrow one past a point you already measured.

## Related

- [`OptimizeSpaceBotorch`](../reference/optimize-space.md)
- [Optimizing Himmelblau's function](../tutorials/optimizing-himmelblau.md)
