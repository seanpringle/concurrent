# concurrent

Run concurrent instances of a command, distributing batches of input lines and neatly interleaving output.

```
$ ./concurrent --help
concurrent [-l N] [-b N] [-r N] [-i] COMMAND
-l N : Limit concurrency to N jobs (default: #cores)
-b N : Batch size of N lines (default: 1)
-r N : Retry failed jobs N times (default: 0)
-i   : Ignore job failures (default: abort)
-v   : Verbose logging (default: off)
```

## vs `xargs --max-procs`

* Output lines from *stdout* and *stderr* are interleaved and grouped cleanly.
* Orthogonal usage that has nothing to do with argument manipulation or translating stdin to arguments.

Quite easy to use `concurrent` and `xargs` together in creative ways (or to fork-bomb yourself).

## vs GNU Parallel

* Much narrower use case. Unix *do-one-thing-well* approach. Light weight.
* Similar automatic interleaving of output so that individual lines do not break.
* Similar automatic grouping of output lines from individual jobs for readabilty.
* But no attempt to preserve output order in job order. Faster jobs are not held back.
