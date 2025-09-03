# Kusto Go sample (Azure Data Explorer)

This is a minimal sample showing how to query Azure Data Explorer (Kusto) using the Go SDK `github.com/Azure/azure-kusto-go/azkustodata`.
The program emits NDJSON (one JSON object per line) so you can pipe to tools like `jq`.

## Prerequisites
- Go 1.21+
- Access to an Azure subscription and permission to create a Kusto cluster
- You must be authenticated to Azure (e.g., via Azure CLI `az login`) before using the script

## Manage the cluster with kusto.sh
Use `kusto.sh` to create and delete a Kusto cluster by name. Fixed settings used by the script:
- LOCATION: `eastus`
- RESOURCE GROUP: `rg-kusto-sample`
- DATABASE: `sampledb`
- SKU: `Dev(No SLA)_Standard_D11_v2` (tier `Basic`, capacity `1`)
 - Sample table: `ProbeTest` (with one row: `Message="kusto-sample-ok"`, `When=now()`)

Usage:
```bash
# Create a cluster
./kusto.sh create <cluster-name>

# Delete a cluster
./kusto.sh delete <cluster-name>

# Probe connectivity (endpoint, DB, data access)
./kusto.sh probe <cluster-name>
```
`create` initializes a tiny sample table so the probe can verify expected data is present. `probe` also ensures the sample table exists before running checks.

After creation, the cluster URI will be:
```
https://<cluster-name>.eastus.kusto.windows.net
```

## Quick Start
- Create: `./kusto.sh create <cluster-name>`
- Probe: `./kusto.sh probe <cluster-name>`
- Delete: `./kusto.sh delete <cluster-name>`

Probe validates:
- Endpoint reachability and auth (mgmt `.show version`)
- Database access (query `print 1` against `sampledb`)
- Data presence in sample table `ProbeTest` (expected `Message="kusto-sample-ok"`)

Example:
```bash
export KUSTO_CLUSTER="https://<cluster-name>.eastus.kusto.windows.net"
export KUSTO_DATABASE="sampledb"
# Optional (simple query)
# export KUSTO_QUERY="print 1"
```

## Probe mode
Run a fast preflight (only cluster name required):
```bash
./kusto.sh probe <cluster-name>
```
Or directly without the script (defaults to region `eastus`, db `sampledb`):
```bash
go run . probe <cluster-name>
```

Example output (healthy):
```
OK mgmt (120ms): cluster reachable
OK database (80ms): db ok: sampledb
OK data-sample (85ms): sample table ok: ProbeTest contains expected data
OK probe: endpoint, db, and data access validated
```
If a step fails, the output includes a suggestion, e.g.:
```
FAIL database (210ms): basic query failed: ...
SUGGEST database: Database 'sampledb' not found. Verify KUSTO_DATABASE or create it (see kusto.sh).
```

## Advanced: Query sample (NDJSON)
If you want to use the general KQL sample outside of the probe, set two env vars and run:
```bash
export KUSTO_CLUSTER="https://<cluster-name>.eastus.kusto.windows.net"
export KUSTO_DATABASE="sampledb"
# optional: export KUSTO_QUERY="print 1"
go run . | jq
```

## Sample output
Below is sample NDJSON produced by running with:
```bash
KUSTO_QUERY="print 1" go run .
```

```json
{"_kind":"PrimaryResult","_rowIndex":0,"_table":"PrimaryResult","print_0":1}
{"_kind":"QueryProperties","_rowIndex":0,"_table":"@ExtendedProperties","Key":"Visualization","Value":{"Accumulate":false,"AnomalyColumns":null,"IsQuerySorted":false,"Kind":null,"Legend":null,"Series":null,"Title":null,"Visualization":null,"XAxis":null,"XColumn":null,"XTitle":null,"Xmax":null,"Xmin":null,"YAxis":null,"YColumns":null,"YSplit":null,"YTitle":null,"Ymax":"NaN","Ymin":"NaN"}}
{"_kind":"QueryCompletionInformation","_rowIndex":0,"_table":"QueryCompletionInformation","EventTypeName":"QueryInfo","StatusCodeName":"S_OK (0)"}
```

## What it does
- Authenticates with `WithDefaultAzureCredential()`
- Executes the KQL and streams results as NDJSON (dynamic columns are parsed when possible)

For more SDK usage (iterative vs buffered results, mapping rows to structs, ingestion), see: [Azure/azure-kusto-go](https://github.com/Azure/azure-kusto-go).
