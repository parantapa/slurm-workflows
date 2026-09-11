# How to run the task-queue server

[<- back to the main README](../../README.md)

The executor and the workers communicate only through a `ds-service` server,
and each executor needs one of its own.
The simplest way is to start the server from the driver.
The server then lives exactly as long as the run.

## Start it from the driver

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

with DsServiceServer(interface="ib0", port=5051) as ds:
    ds.wait_until_ready()      # blocks until it accepts connections

    executor = SlurmPilotExecutor(name="my-run", server_address=ds.address)
    ...
```

`DsServiceServer` comes from the `ds-service-client` package.
The constructor spawns the process,
so the server starts before the constructor returns.
Call `wait_until_ready()` before you hand the address to anything.
Omit `port` to get an arbitrary free one.

## Choose an interface the compute nodes can reach

The server must be reachable from the compute nodes.
`DsServiceServer` binds it to the IPv4 address of the `interface` you name.
The example uses `ib0`, the login node's Infiniband interface.
`ds.address` is then the `host:port` the workers connect to.

If you name an interface that the node does not have,
the constructor raises `ValueError`.
An interface with no IPv4 address raises the same error.

## If the binary is not on your `PATH`

`DsServiceServer` runs `ds-service` from your `PATH`.
To override that, pass `ds_service_bin`,
or set the `DS_SERVICE_BIN` environment variable.

To install it, see
[How to install slurm-workflows on Rivanna](install-on-rivanna.md).

## Related

- [How to watch a run with `swtop`](watch-a-run-with-swtop.md),
    which reads the same server
- [About the pilot-job model](../explanation/about-the-pilot-job-model.md),
    for why one server belongs to one executor
