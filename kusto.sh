#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./kusto.sh create <cluster-name>
#   ./kusto.sh delete <cluster-name>
#   ./kusto.sh probe  <cluster-name>
#
# Fixed settings (keep intact):
LOCATION="eastus"
RG="rg-kusto-sample"
DB="sampledb"
SKU_NAME="Dev(No SLA)_Standard_D11_v2"
SKU_TIER="Basic"

# Sample table settings
SAMPLE_TABLE="ProbeTest"
SAMPLE_EXPECT_MESSAGE="kusto-sample-ok"

require_az() {
	if ! command -v az >/dev/null 2>&1; then
		echo "Azure CLI (az) not found" >&2
		exit 1
	fi
	az account show -o none >/dev/null 2>&1 || {
		echo "Please run 'az login' first" >&2
		exit 1
	}
}

# Create or refresh a small sample table with expected data using the Go helper
ensure_sample_table() {
    local cluster="$1"
    KUSTO_SAMPLE_TABLE="$SAMPLE_TABLE" KUSTO_PROBE_EXPECT_MESSAGE="$SAMPLE_EXPECT_MESSAGE" \
    go run . init-sample "$cluster" >/dev/null 2>&1 || true
}

ensure_rg_and_provider() {
	az group create -n "$RG" -l "$LOCATION" -o none
	az provider register -n Microsoft.Kusto --wait -o none
}

wait_for_state() {
	local cluster="$1"
	local want_state="$2"   # e.g. Running or Deleted
	local timeout=${3:-900}  # seconds
	local interval=15
	local elapsed=0
	while true; do
		if [[ "$want_state" == "Deleted" ]]; then
			if ! az kusto cluster show -g "$RG" -n "$cluster" -o none >/dev/null 2>&1; then
				break
			fi
		else
			state=$(az kusto cluster show -g "$RG" -n "$cluster" --query state -o tsv 2>/dev/null || echo "")
			if [[ "$state" == "$want_state" ]]; then
				break
			fi
		fi
		if (( elapsed >= timeout )); then
			echo "Timed out waiting for cluster '$cluster' to reach state '$want_state'" >&2
			exit 1
		fi
		sleep "$interval"
		elapsed=$((elapsed+interval))
	done
}

create_cluster() {
    local cluster="$1"
    require_az
    ensure_rg_and_provider
    az kusto cluster create \
		-g "$RG" \
		-n "$cluster" \
		-l "$LOCATION" \
		--sku name="$SKU_NAME" capacity=1 tier="$SKU_TIER" \
		-o none
	wait_for_state "$cluster" "Running" 1200
	az kusto database create \
		--cluster-name "$cluster" \
		--database-name "$DB" \
		-g "$RG" \
		--read-write-database location="$LOCATION" soft-delete-period="P7D" \
		-o none
	# Grant current user Admin (ignore error if already exists)
	set +e
	az kusto database-principal-assignment create \
		-g "$RG" \
		--cluster-name "$cluster" \
		--database-name "$DB" \
		--principal-id "$(az ad signed-in-user show --query id -o tsv)" \
		--principal-type User \
		--principal-assignment-name "db-admin-$(date +%s)" \
		--role Admin \
		--tenant-id "$(az account show --query tenantId -o tsv)" \
		-o none >/dev/null 2>&1
    set -e
    uri="https://$cluster.$LOCATION.kusto.windows.net"
    echo "Cluster ready: $uri (DB=$DB)"

    # Ensure sample table with one row exists
    ensure_sample_table "$cluster"
}

delete_cluster() {
	local cluster="$1"
	require_az
	# Delete cluster only; keep RG as-is
	if az kusto cluster show -g "$RG" -n "$cluster" -o none >/dev/null 2>&1; then
		az kusto cluster delete -g "$RG" -n "$cluster" --yes -o none
		wait_for_state "$cluster" "Deleted" 1800
		echo "Cluster deleted: $cluster"
	else
		echo "Cluster not found: $cluster (nothing to delete)"
	fi
}

probe_cluster() {
    local cluster="$1"
    require_az
    # Verify cluster exists
    if ! az kusto cluster show -g "$RG" -n "$cluster" -o none >/dev/null 2>&1; then
        echo "Cluster not found: $cluster" >&2
        exit 1
    fi
    # Ensure sample table is present for probe
    ensure_sample_table "$cluster"
    local uri="https://$cluster.$LOCATION.kusto.windows.net"
    echo "Probing Kusto endpoint: $uri (DB=$DB)"
    KUSTO_SAMPLE_TABLE="$SAMPLE_TABLE" KUSTO_PROBE_EXPECT_MESSAGE="$SAMPLE_EXPECT_MESSAGE" \
    go run . probe "$cluster" || {
        echo "Probe failed for $uri" >&2
        exit 1
    }
}

main() {
    cmd=${1:-}
    name=${2:-}
    if [[ -z "$cmd" || -z "$name" ]]; then
        echo "Usage: $0 {create|delete|probe} <cluster-name>" >&2
        exit 1
    fi
    case "$cmd" in
        create) create_cluster "$name" ;;
        delete) delete_cluster "$name" ;;
        probe)  probe_cluster  "$name" ;;
        *) echo "Unknown command: $cmd" >&2; exit 1 ;;
    esac
}

main "$@"
