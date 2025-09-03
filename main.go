package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "log"
    "net"
    "os"
    "strings"
    "time"

    "github.com/Azure/azure-kusto-go/azkustodata"
    "github.com/Azure/azure-kusto-go/azkustodata/kql"
    "github.com/Azure/azure-kusto-go/azkustodata/types"
)

// This sample demonstrates a minimal query using the Azure Data Explorer (Kusto) Go SDK v1+ packages.
// It authenticates with DefaultAzureCredential and runs a simple KQL against the given database.
func main() {
    if len(os.Args) > 1 {
        switch os.Args[1] {
        case "probe":
            var clusterArg string
            if len(os.Args) > 2 {
                clusterArg = os.Args[2]
            }
            runProbe(clusterArg)
            return
        case "init-sample":
            var clusterArg string
            if len(os.Args) > 2 {
                clusterArg = os.Args[2]
            }
            runInitSample(clusterArg)
            return
        }
    }
    cluster := getenvOrExit("KUSTO_CLUSTER", "https://<cluster>.<region>.kusto.windows.net")
    database := getenvOrExit("KUSTO_DATABASE", "<database>")
    queryText := getenv("KUSTO_QUERY", "cluster('help').database('Samples').StormEvents | take 5")

	// Build connection string and client using DefaultAzureCredential.
	kcsb := azkustodata.NewConnectionStringBuilder(cluster).WithDefaultAzureCredential()

	client, err := azkustodata.New(kcsb)
	if err != nil {
		log.Fatalf("failed creating Kusto client: %v", err)
	}
	defer client.Close()

	// Build the KQL query.
	// kql.New requires a compile-time string literal or a string built via safe builders.
	// When the query comes from env, use a builder and AddUnsafe explicitly.
	var q *kql.Builder
	if os.Getenv("KUSTO_QUERY") == "" {
		q = kql.New("cluster('help').database('Samples').StormEvents | take 5")
	} else {
		q = (&kql.Builder{}).AddUnsafe(queryText)
	}

	// Use a timeout to avoid hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Execute query and stream tables/rows iteratively (lower memory footprint for large results).
	dataset, err := client.IterativeQuery(ctx, database, q)
	if err != nil {
		log.Fatalf("query submission failed: %v", err)
	}
	defer dataset.Close()

	tables := dataset.Tables()
	for tableResult := range tables {
		if tableResult.Err() != nil {
			log.Fatalf("table error: %v", tableResult.Err())
		}

		table := tableResult.Table()
		cols := table.Columns()

		for rowResult := range table.Rows() {
			if rowResult.Err() != nil {
				log.Fatalf("row error: %v", rowResult.Err())
			}
			row := rowResult.Row()
			vals := row.Values()

			obj := make(map[string]interface{}, len(cols)+3)
			obj["_table"] = table.Name()
			obj["_kind"] = table.Kind()
			obj["_rowIndex"] = row.Index()

			for i, c := range cols {
				if i >= len(vals) {
					continue
				}
				v := vals[i]
				if v == nil {
					obj[c.Name()] = nil
					continue
				}
				if c.Type() == types.Dynamic {
					if b, ok := v.GetValue().([]byte); ok {
						var any interface{}
						if err := json.Unmarshal(b, &any); err == nil {
							obj[c.Name()] = any
							continue
						}
						obj[c.Name()] = string(b)
						continue
					}
					if pb, ok := v.GetValue().(*[]byte); ok {
						if pb == nil {
							obj[c.Name()] = nil
							continue
						}
						var any interface{}
						if err := json.Unmarshal(*pb, &any); err == nil {
							obj[c.Name()] = any
							continue
						}
						obj[c.Name()] = string(*pb)
						continue
					}
				}
				obj[c.Name()] = v.GetValue()
			}

			enc, err := json.Marshal(obj)
			if err != nil {
				log.Fatalf("failed to marshal row as JSON: %v", err)
			}
			fmt.Println(string(enc))
		}
	}
}

// runProbe validates endpoint reachability, database access, and basic data permissions.
// Steps:
//  1) Management probe (cluster-level): .show version
//  2) Database probe (query-level): print 1
//  3) Data probe (query-level): take 1 from a known table (configurable)
// Output: concise status lines and remediation suggestions. Non-zero exit on failure.
func runProbe(clusterArg string) {
    cluster := resolveClusterURL(clusterArg)
    database := getenv("KUSTO_DATABASE", "sampledb")
    sampleTable := getenv("KUSTO_SAMPLE_TABLE", "ProbeTest")
    expectMsg := getenv("KUSTO_PROBE_EXPECT_MESSAGE", "kusto-sample-ok")

    // Small overall timeout per step to keep latency low for healthy contexts.
    stepTimeout := getDurationEnv("KUSTO_PROBE_TIMEOUT", 3*time.Second)

    kcsb := azkustodata.NewConnectionStringBuilder(cluster).WithDefaultAzureCredential()
    client, err := azkustodata.New(kcsb)
    if err != nil {
        fail("auth/client", "failed to create Kusto client", err, suggestionForAuth(err))
    }
    defer client.Close()

    // Step 1: Management probe (cluster-level)
    {
        ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
        start := time.Now()
        _, mgmtErr := client.Mgmt(ctx, "", kql.New(".show version"))
        cancel()
        if mgmtErr != nil {
            failTimed("mgmt", time.Since(start), ".show version failed", mgmtErr, suggestionForEndpointOrAuth(mgmtErr))
        }
        okTimed("mgmt", time.Since(start), "cluster reachable")
    }

    // Step 2: Database probe (query-level)
    {
        ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
        start := time.Now()
        _, qErr := client.Query(ctx, database, kql.New("print 1"))
        cancel()
        if qErr != nil {
            failTimed("database", time.Since(start), "basic query failed", qErr, suggestionForDatabase(qErr, database))
        }
        okTimed("database", time.Since(start), fmt.Sprintf("db ok: %s", database))
    }

    // Step 3: Sample data probe (verify expected content exists)
    {
        q := (&kql.Builder{}).AddUnsafe(
            fmt.Sprintf("%s | where Message == '%s' | take 1", sampleTable, strings.ReplaceAll(expectMsg, "'", "''")),
        )
        ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
        start := time.Now()
        has, derr := queryHasAnyRow(ctx, client, database, q)
        cancel()
        if derr != nil {
            if isTableNotFound(derr) {
                failTimed("data-sample", time.Since(start), fmt.Sprintf("sample table not found: %s", sampleTable), derr, "Run kusto.sh probe or create to initialize the sample table.")
            }
            if isPermissionErr(derr) {
                failTimed("data-sample", time.Since(start), fmt.Sprintf("no access to sample table: %s", sampleTable), derr, suggestionForPermissions())
            }
            failTimed("data-sample", time.Since(start), "query failed for sample table", derr, suggestionForQuery(sampleTable))
        }
        if !has {
            failTimed("data-sample", time.Since(start), fmt.Sprintf("expected row not found in %s (Message=='%s')", sampleTable, expectMsg), nil, "Initialize sample data via kusto.sh or verify ingestion.")
        }
        okTimed("data-sample", time.Since(start), fmt.Sprintf("sample table ok: %s contains expected data", sampleTable))
        // All good; no need to probe additional tables.
        fmt.Println("OK probe: endpoint, db, and data access validated")
        return
    }

    // All good
    fmt.Println("OK probe: endpoint, db, and data access validated")
}

func okTimed(step string, d time.Duration, msg string) {
    fmt.Printf("OK %s (%dms): %s\n", step, d.Milliseconds(), msg)
}

func infoTimed(step string, d time.Duration, msg string) {
    fmt.Printf("INFO %s (%dms): %s\n", step, d.Milliseconds(), msg)
}

func failTimed(step string, d time.Duration, msg string, err error, suggest string) {
    fmt.Printf("FAIL %s (%dms): %s: %v\n", step, d.Milliseconds(), msg, err)
    if suggest != "" {
        fmt.Printf("SUGGEST %s: %s\n", step, suggest)
    }
    os.Exit(1)
}

func fail(step, msg string, err error, suggest string) {
    if err != nil {
        fmt.Printf("FAIL %s: %s: %v\n", step, msg, err)
    } else {
        fmt.Printf("FAIL %s: %s\n", step, msg)
    }
    if suggest != "" {
        fmt.Printf("SUGGEST %s: %s\n", step, suggest)
    }
    os.Exit(1)
}

func suggestionForAuth(err error) string {
    return "Ensure Azure auth is available: run 'az login' or configure DefaultAzureCredential (AZURE_TENANT_ID, AZURE_CLIENT_ID/SECRET)."
}

func suggestionForEndpointOrAuth(err error) string {
    if isNetworkErr(err) {
        return "Verify KUSTO_CLUSTER endpoint is correct (https://<cluster>.<region>.kusto.windows.net) and reachable."
    }
    if looksLikeAAD(err) || isAuthErr(err) {
        return suggestionForAuth(err)
    }
    return "Check endpoint and authentication."
}

func suggestionForDatabase(err error, db string) string {
    if isDatabaseNotFound(err) {
        return fmt.Sprintf("Database '%s' not found. Verify KUSTO_DATABASE or create it (see kusto.sh).", db)
    }
    if isPermissionErr(err) {
        return "You may lack database permissions. Ensure your identity has access (e.g., Admin/User role)."
    }
    return "Verify KUSTO_DATABASE and your permissions."
}

func suggestionForPermissions() string {
    return "Grant your identity read access to the database/table (e.g., Admin/User role)."
}

func suggestionForQuery(table string) string {
    return fmt.Sprintf("Investigate query or connectivity issues for table '%s'.", table)
}

func isNetworkErr(err error) bool {
    var nErr net.Error
    if errors.As(err, &nErr) {
        return true
    }
    msg := strings.ToLower(err.Error())
    return strings.Contains(msg, "no such host") || strings.Contains(msg, "connection refused") || strings.Contains(msg, "timeout")
}

func looksLikeAAD(err error) bool {
    msg := strings.ToLower(err.Error())
    return strings.Contains(msg, "aadsts") || strings.Contains(msg, "token") || strings.Contains(msg, "credential")
}

func isAuthErr(err error) bool {
    msg := strings.ToLower(err.Error())
    return strings.Contains(msg, "unauthorized") || strings.Contains(msg, "401") || strings.Contains(msg, "authorization")
}

func isPermissionErr(err error) bool {
    msg := strings.ToLower(err.Error())
    return strings.Contains(msg, "forbidden") || strings.Contains(msg, "403") || strings.Contains(msg, "insufficient") || strings.Contains(msg, "permission")
}

func isDatabaseNotFound(err error) bool {
    msg := strings.ToLower(err.Error())
    return strings.Contains(msg, "database") && strings.Contains(msg, "not found")
}

func isTableNotFound(err error) bool {
    msg := strings.ToLower(err.Error())
    // Heuristics for semantic errors indicating missing table
    return strings.Contains(msg, "semantic") && (strings.Contains(msg, "table") || strings.Contains(msg, "name")) && strings.Contains(msg, "not")
}

// queryHasAnyRow runs a query and returns true if the primary result has at least one row.
func queryHasAnyRow(ctx context.Context, client *azkustodata.Client, db string, q *kql.Builder) (bool, error) {
    ds, err := client.IterativeQuery(ctx, db, q)
    if err != nil {
        return false, err
    }
    defer ds.Close()
    tables := ds.Tables()
    for tr := range tables {
        if tr.Err() != nil {
            return false, tr.Err()
        }
        t := tr.Table()
        if t.Name() != "PrimaryResult" {
            continue
        }
        for rr := range t.Rows() {
            if rr.Err() != nil {
                return false, rr.Err()
            }
            // Found at least one row
            return true, nil
        }
    }
    return false, nil
}

func splitCSV(s string) []string {
    parts := strings.Split(s, ",")
    out := make([]string, 0, len(parts))
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p != "" {
            out = append(out, p)
        }
    }
    return out
}

func getDurationEnv(key string, def time.Duration) time.Duration {
    v := strings.TrimSpace(os.Getenv(key))
    if v == "" {
        return def
    }
    if d, err := time.ParseDuration(v); err == nil {
        return d
    }
    return def
}

func getenvOrExit(key, hint string) string {
    v := os.Getenv(key)
    if v == "" {
        log.Fatalf("environment variable %s is required (e.g., %s)", key, hint)
    }
    return v
}

func getenv(key, def string) string {
    v := os.Getenv(key)
    if v == "" {
        return def
    }
    return v
}

// runInitSample ensures a small sample table exists and contains a known row.
// It creates/merges the table schema and appends a single row with the expected message.
func runInitSample(clusterArg string) {
    cluster := resolveClusterURL(clusterArg)
    database := getenv("KUSTO_DATABASE", "sampledb")
    sampleTable := getenv("KUSTO_SAMPLE_TABLE", "ProbeTest")
    expectMsg := getenv("KUSTO_PROBE_EXPECT_MESSAGE", "kusto-sample-ok")

    kcsb := azkustodata.NewConnectionStringBuilder(cluster).WithDefaultAzureCredential()
    client, err := azkustodata.New(kcsb)
    if err != nil {
        log.Fatalf("failed creating Kusto client: %v", err)
    }
    defer client.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Create or merge table schema
    qCreate := (&kql.Builder{}).AddUnsafe(fmt.Sprintf(".create-merge table %s (Message:string, When:datetime)", sampleTable))
    if _, err := client.Mgmt(ctx, database, qCreate); err != nil {
        log.Fatalf("failed to create/merge sample table: %v", err)
    }

    // Append a single sample row
    qAppend := (&kql.Builder{}).AddUnsafe(fmt.Sprintf(".set-or-append %s <| print Message='%s', When=now()", sampleTable, strings.ReplaceAll(expectMsg, "'", "''")))
    if _, err := client.Mgmt(ctx, database, qAppend); err != nil {
        log.Fatalf("failed to append sample row: %v", err)
    }

    fmt.Printf("Initialized sample table %s with message '%s'\n", sampleTable, expectMsg)
}

// resolveClusterURL builds the cluster URI from a provided name, or falls back to env KUSTO_CLUSTER.
// The region is hard-coded to eastus to match kusto.sh cluster creation settings.
func resolveClusterURL(clusterName string) string {
    if strings.TrimSpace(clusterName) != "" {
        return fmt.Sprintf("https://%s.eastus.kusto.windows.net", strings.TrimSpace(clusterName))
    }
    v := strings.TrimSpace(os.Getenv("KUSTO_CLUSTER"))
    if v != "" {
        return v
    }
    log.Fatalf("cluster not provided. Usage: 'probe <cluster-name>' or set KUSTO_CLUSTER to full URI")
    return ""
}
