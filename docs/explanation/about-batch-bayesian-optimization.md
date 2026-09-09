# About batch Bayesian optimization on a worker pool

[<- back to the main README](../../README.md)

## Why a search has rounds

A sweep knows every task up front and can submit them in one go.
A search cannot: which point is worth trying next
depends on what the previous points returned.

So a run is a sequence of **rounds**.
Each round fits a Gaussian process to everything measured so far,
asks it for a whole batch of points at once,
evaluates that batch across the pool, and refits.
The batch is what keeps the pool busy:
a one-point-at-a-time optimizer would leave all but one worker idle.

That takes two phases, because a model needs something to fit
before it can choose anything:
an `ExploreSpaceSobolQMC` sweep measures a first design and saves it,
and `OptimizeSpaceBotorch` is handed that file and searches on from it.
The optimizer never explores,
which is what makes a search resumable:
the state that has to survive a walltime limit is a file, not an object.

Each round is a barrier: fit, propose a batch, evaluate, refit.
That is the cost of choosing a batch jointly,
and the reason a round should be as wide as the pool.
A batch bigger than the pool queues behind the workers;
a smaller one leaves workers idle.

## Why the batch is chosen jointly

`qLogNoisyExpectedImprovement` is asked for the whole batch
in one `optimize_acqf` call rather than a point at a time.
Choosing greedily is the usual advice for large batches,
and it is the wrong advice here:
it was measured 10 to 15 times *slower* on a low-dimensional space,
because the greedy path pays the multi-start restart cost
once per point instead of once per batch.
Asking jointly also keeps the proposals from stacking on one spot.

The noisy variant reads its incumbent off the posterior
at the points already evaluated,
rather than taking the best measured value as a target.
An evaluation that came back lucky therefore does not become a target
the search chases.
The cost is that it carries every point measured so far,
which is why a fit gets more expensive every round.

## Why the fit runs on a worker

The GP fit and the acquisition optimization are submitted as a task,
one per round, rather than run on the driver.
Shipping a fitted GP back to the driver would cost more than the fit did,
and the driver is usually a login node,
where a multi-core torch job is not welcome.

That is why there are two queue arguments and not one.
An evaluation is a cheap single call, `search_parallelism` at a time;
the fit is a single task that threads across cores
and grows superlinearly with the number of observations.
They want different nodes:
one wants many slots, the other wants a whole node to itself.

Pointing both at one queue is supported and cannot deadlock,
since a round never has both kinds of task in flight at once.
The fit then simply waits for a slot in a pool sized for the objective.
Only the `optimizer_queue` workers need botorch;
workers serving only the objective need neither botorch nor torch.

## Why the budget is counted in rounds

The stopping rule counts rounds, not points,
because a round is the unit that costs something:
one model fit plus one full pool of evaluations.

A round is *stalled* when it fails to improve the best value
by `min_improvement`,
expressed as a fraction of the incumbent's magnitude
so that the setting means the same thing
whether the objective is in seconds or in dollars.
`patience` stalled rounds in a row end the search.

The floor and the patience interact in a way worth stating.
The stall counter runs from the first round,
but `min_search_iterations` gates the *stop*, not the counting,
so a stalled round below the floor still counts towards `patience`
and simply cannot be the round that ends the search.
The earliest possible stop is therefore
`max(min_search_iterations, patience)` rounds,
and a search that never improves at all stops at the floor exactly.
The floor exists so that a slow start is not mistaken for a finished search,
which is a real risk when the first design was small.

## When it is worth the overhead

Bayesian optimization earns its overhead when one evaluation costs minutes.
Below that, the model fits dominate the runtime,
and a plain Sobol' sweep of the same total size will finish sooner
and be easier to reason about.
A sweep is also the right choice for a first look at a space,
for a baseline to judge a search against,
and whenever one wave of evaluations is the whole budget.

A best value that stops moving while the fits keep growing
means the budget is going to the model rather than to the search.
That is the signal to stop, not to raise the ceiling.

## What rounding costs on a discrete space

Every parameter is mapped into the unit cube before a model sees it,
which is what lets one GP span integer, float and categorical parameters at once.
Integer and categorical parameters come back by rounding a continuous proposal.

On a mostly-discrete space with few levels,
expect a search to re-propose points it has already evaluated:
several distinct continuous proposals round to the same grid point.
What gets recorded is where the objective actually ran, after rounding,
never the continuous proposal,
since telling the GP about a location the objective never visited
would corrupt the model rather than enrich it.

## Why an objective returns a mapping

The objective returns a mapping rather than a number
so that the one value being optimized can travel
alongside everything else the evaluation happened to learn -
a runtime, an intermediate metric, a checkpoint path.
Only the entry under `objective_key` is ranked or modelled;
the rest is recorded.

That entry is **minimized**,
so a score you would rather maximize is negated.
Internally the model is fit to `-f`, because botorch maximizes,
and every acquisition value lives in that negated space too.

## Related

- [`OptimizeSpaceBotorch`](../reference/optimize-space.md)
- [`ExploreSpaceSobolQMC`](../reference/explore-space.md)
- [Tutorial: optimizing Himmelblau's function](../tutorials/optimizing-himmelblau.md)
