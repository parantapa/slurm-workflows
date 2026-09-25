# Batch Bayesian optimization

[<- back to the main README](../../README.md)

## Why a search has rounds

An exploration knows every task up front and can submit them at once.
A search cannot: which point is worth trying next
depends on what the previous points returned.

So a run is a sequence of **rounds**.
Each round fits a Gaussian process to everything measured so far.
Then it asks the model for a whole batch of points at once,
evaluates that batch across the pool of workers, and refits.
The batch is what keeps the pool busy:
a one-point-at-a-time optimizer leaves all but one worker idle.

That takes two phases,
because a model needs something to fit before it can choose anything.
First, an `ExploreSpaceSobolQMC` exploration measures a first design and saves it.
Then `OptimizeSpaceBotorch` reads that file and searches on from it.
The optimizer never explores,
which is what makes a search resumable.
The state that must survive a time limit is a file, not an object.

Each round is a barrier: fit, propose a batch, evaluate, refit.
That is the cost of choosing a batch jointly.
For this reason, a round must be as wide as the pool.
A batch bigger than the pool queues behind the workers.
A smaller one leaves workers idle.

## Why the batch is chosen jointly

`OptimizeSpaceBotorch` asks [`qLogNoisyExpectedImprovement`](https://botorch.org/docs/acquisition)
for the whole batch in one [`optimize_acqf`](https://botorch.org/api/optim.html) call,
rather than a point at a time.
The usual advice for a large batch is to pick the points greedily,
and that advice is wrong here.
The greedy path pays the restart cost of a multi-start
once per point instead of once per batch.
One joint call also keeps the candidates from stacking on one spot.

The noisy variant reads its incumbent off the posterior
at the points already evaluated,
not off the best measured value.
An evaluation that came back lucky therefore does not become a target
the search chases.
The cost is that the variant carries every point measured so far,
which is why a fit gets more expensive every round.

## Why the fit runs on a worker

`OptimizeSpaceBotorch` submits the GP fit and the acquisition optimization
as one task per study per round.
The driver never runs them itself.
A fitted GP costs more to ship back to the driver than it cost to fit.
The driver is also usually a login node,
where a multi-core torch job is not welcome.

That is why there are two queue arguments and not one,
`objective_queue` and `optimizer_queue`.
An evaluation is a cheap single call, `search_parallelism` at a time.
The fit is a single task that threads across cores
and grows superlinearly with the number of observations.
The two want different nodes:
an evaluation wants many workers,
and a fit wants a whole node to itself.

Both arguments can point at one queue without risking deadlock,
because a round never has both kinds of task in flight at once.
The fit then waits for a worker in a pool sized for the objective.
[Where the work runs](../reference/optimize-space.md#where-the-work-runs)
says which workers need botorch.

## Why the budget is counted in rounds

The stopping rule counts rounds, not points,
because a round is the unit that costs something:
one model fit plus one full pool of evaluations.

A round is *stalled* when it fails to improve the best value
by `min_improvement`.
That setting is a fraction of the incumbent's magnitude,
so it means the same thing
whether the objective is in seconds or in dollars.
`patience` stalled rounds in a row end the search.

The floor is `min_search_rounds`, the rounds that always run.
The ceiling is `max_search_rounds`.
The floor and the patience interact.
[When it stops](../reference/optimize-space.md#when-it-stops)
gives the arithmetic.
The floor exists so that a slow start does not look like a finished search.
That risk is real when the first design was small.

## When it is worth the overhead

Bayesian optimization earns its overhead when one evaluation costs minutes.
Below that, the model fits dominate the runtime.
A plain Sobol' exploration of the same total size finishes sooner,
and it is easier to reason about.
An exploration is also the right choice in three cases:

- a first look at a space
- a baseline to judge a search against
- a budget of one round of evaluations

When the best value stops moving while the fits grow,
the budget goes to the model rather than to the search.
That is the signal to stop, not to raise the ceiling.

## What rounding costs on a discrete space

`OptimizeSpaceBotorch` maps every parameter into the unit cube
before a model sees it.
One GP can therefore span integer, float and categorical parameters at once.
The search space rounds a continuous candidate
back to an integer or a categorical level.

On a mostly-discrete space with few levels,
a search re-proposes points it already evaluated.
Several distinct continuous candidates round to the same grid point.
The search records where the objective actually ran, after rounding,
never the continuous candidate.
The reason is that a record of a place the objective never visited
corrupts the model rather than enriches it.

## Why an objective returns a mapping

The objective returns a mapping rather than a number,
so the value the search optimizes can travel
alongside everything else the evaluation happened to learn.
That extra can be a runtime, an intermediate metric or a checkpoint path.
[The objective](../reference/objective.md)
covers what the two space classes do with the mapping.

## Minimizing on top of a maximizer

The search **minimizes** the entry under `objective_key`,
so a quantity to be maximized enters the search negated.
Internally the search fits the model to `-f`,
because botorch maximizes,
and every acquisition value lives in that negated space too.

## Related

- [`OptimizeSpaceBotorch`](../reference/optimize-space.md)
- [`ExploreSpaceSobolQMC`](../reference/explore-space.md)
- [Search spaces](../reference/search-space.md),
    for the range types
- [The objective](../reference/objective.md),
    for the objective contract
- [How to resume a search](../how-to-guides/resume-a-search.md)
- [Optimizing Himmelblau's function](../tutorials/optimizing-himmelblau.md)
