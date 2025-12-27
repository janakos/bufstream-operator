# Bufstream Operator

Kubernetes operator for managing Bufstream resources (Kafka-compatible streaming). Built with Kubebuilder, it manages four CRDs:
- **Cluster**: Connection configuration to a Bufstream instance
- **Topic**: Kafka topics with partitions and replication
- **User**: SCRAM authentication credentials
- **ACL**: Access control lists for authorization

## Common Commands

```bash
# Build and verify
go build ./...
go vet ./...
make test                    # Run unit tests

# Generate code after modifying *_types.go files
make generate                # Generate DeepCopy methods
make manifests               # Generate CRDs and RBAC

# Install CRDs to cluster
make install

# Run operator locally (requires port-forward to bufstream)
BUFSTREAM_DEV_MODE=true make run

# Development environment
make setup-test-e2e          # Create kind cluster
make cleanup-test-e2e        # Delete kind cluster
```

## Architecture

```
api/v1alpha1/           # CRD type definitions
  cluster_types.go      # Cluster spec/status
  topic_types.go        # Topic spec/status
  user_types.go         # User spec/status
  acl_types.go          # ACL spec/status

internal/
  bufstream/            # Kafka client library (franz-go wrapper)
    client.go           # Client creation, SASL auth, health check
    topic.go            # Topic CRUD operations
    user.go             # SCRAM credential operations
    acl.go              # ACL operations
  controller/           # Kubernetes reconcilers
    cluster_controller.go   # Health monitoring
    topic_controller.go     # Topic lifecycle
    user_controller.go      # User credential lifecycle
    acl_controller.go       # ACL lifecycle
    cluster_helpers.go      # Shared cluster lookup/auth helpers

cmd/main.go             # Operator entrypoint, registers all controllers

helm/                   # Local dev Bufstream deployment
config/samples/         # Example CRD manifests
```

## Key Patterns

**Cluster Reference**: Topics, Users, and ACLs reference a Cluster by name. The Cluster provides bootstrap servers and admin credentials for SASL authentication.

**Admin Credentials**: The Cluster's `adminCredentialsRef` points to a Secret with `username` and `password` keys. All controllers use `GetAdminCredentials()` helper to authenticate.

**Dev Mode**: Set `BUFSTREAM_DEV_MODE=true` to redirect cluster DNS (*.svc.cluster.local) to localhost, enabling local development with port-forwarding.

**Finalizers**: All resources use finalizers to clean up Kafka resources before deletion.

## Local Development

1. Start kind cluster: `make setup-test-e2e`
2. Install Bufstream: `cd helm && helm dependency update && helm install bufstream ./helm -n bufstream --create-namespace`
3. Port-forward: `kubectl port-forward -n bufstream svc/bufstream 9092:9092`
4. Add hosts entry: `127.0.0.1 bufstream.bufstream.svc.cluster.local`
5. Run operator: `BUFSTREAM_DEV_MODE=true make run`

## Testing with kcat

```bash
# Produce (requires User + ACL with Write permission)
echo "message" | kcat -b localhost:9092 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=<user> \
  -X sasl.password=<pass> \
  -P -t <topic>
```
