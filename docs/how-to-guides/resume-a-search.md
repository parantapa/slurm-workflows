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

## Save the exploration and the search

```python
with SlurmPilotExecutor("search", address) as executor:
    exploration = ExploreSpaceSobolQMC(exploration_studies, executor)
    exploration.run()
    exploration.save(EXPLORE_RESULTS)

    opt = OptimizeSpaceBotorch(optimization_studies, executor, [EXPLORE_RESULTS])
    opt.run()
    opt.save(SEARCH_RESULTS)
```

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

Give the `OptimizationStudy` the `name`
that the exploration and the earlier searches ran under.
The optimizer models a study on the observations the files hold under its name.
A study with nothing under its name in any file is an error,
not a search with no model.

## Keep the space the same

The constructor re-checks every saved point against the space the study declares.
A mismatch raises `RuntimeError`.

- Keep the parameters of the space the same as those of the saved points.
- You can widen a range.
    Do not narrow one past a point you already measured.
- Do not make a range a log range
    if a saved point in it is at or below zero.

## Related

- [`OptimizeSpaceBotorch`](../reference/optimize-space.md)
- [Optimizing Himmelblau's function](../tutorials/optimizing-himmelblau.md)
