# How to run the task-queue server

[<- back to the main README](../../README.md)

The executor and workers communicate only through a `ds-service` server,
and each executor needs one of its own.
Starting it from the driver is the simplest way to get that,
and ties the server's life to the run's.

## Start it from the driver

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

with DsServiceServer(interface="ib0", port=5051) as ds:
    ds.wait_until_ready()      # blocks until it accepts connections

    executor = SlurmPilotExecutor(name="my-run", server_address=ds.address)
    ...
```

`DsServiceServer` comes from the `ds-service-client` package,
and the constructor spawns the process,
so the server is already coming up when it returns.
Call `wait_until_ready()` before handing the address to anything.
Omit `port` to get an arbitrary free one.

## Choose an interface the compute nodes can reach

The server must be reachable from the compute nodes,
so it is bound to the IPv4 address of the `interface` you name -
`ib0` above, the login node's Infiniband interface -
and `ds.address` is the `host:port` the workers then connect to.

Naming an interface that does not exist on that node,
or that has no IPv4 address, raises `ValueError` at construction.

## If the binary is not on your `PATH`

`DsServiceServer` runs `ds-service` from your `PATH`.
Pass `ds_service_bin`, or set the `DS_SERVICE_BIN` environment variable,
to override that.

To install it in the first place, see
[How to install slurm-workflows on Rivanna](install-on-rivanna.md).

## Related

- [How to watch a run with `swtop`](watch-a-run-with-swtop.md),
    which reads the same server
- [About the pilot-job model](../explanation/about-the-pilot-job-model.md),
    for why one server belongs to one executor
