# How to run the `ds-service` server

[<- back to the main README](../../README.md)

Nothing in a run works
until the executor and the workers have a server to meet on.
Each executor needs one of its own.
Start it from the driver, in the `with` block that owns the run.
The server then lives exactly as long as the run.

## Start the server from the driver

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

with DsServiceServer(interface="ib0", port=5051) as ds:
    ds.wait_until_ready()      # blocks until it accepts connections

    executor = SlurmPilotExecutor(name="my-run", server_address=ds.address)
    ...
```

Call `wait_until_ready()` before you hand `ds.address` to anything.
The constructor returns as soon as it spawns the process,
which is well before that process accepts a connection.

`DsServiceServer` comes from the `ds-service-client` package.
Omit `port` to get an arbitrary free one.
If something already holds the port you name,
the constructor raises `OSError`.

## Choose an interface the compute nodes can reach

`DsServiceServer` binds to the IPv4 address of the `interface` you name.
`ds.address` is then the `host:port` that the workers connect to.

The driver can run on a login node,
or on the compute node of its own Slurm job.
In both cases, name `ib0`, the InfiniBand interface of that node,
as the example does.

If the node does not have the interface you name,
the constructor raises `ValueError`.
If the interface has no IPv4 address,
the constructor also raises `ValueError`.
Run `ip -br addr` on that node.
Then name an interface the compute nodes can route to.

## If the binary is not on your `PATH`

`DsServiceServer` runs `ds-service` from your `PATH`.
To override that, pass `ds_service_bin`,
or set the `DS_SERVICE_BIN` environment variable.

To install the binary, see
[How to install slurm-workflows on Rivanna](install-on-rivanna.md).

## Related

- [How to watch a run with `swtop`](watch-a-run-with-swtop.md),
    which reads the same server
- [The pilot-job model](../explanation/pilot-job-model.md),
    for why one server belongs to one executor
