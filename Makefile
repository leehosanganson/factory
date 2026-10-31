.PHONY: build container-build test fmt vet test-postgres-up test-postgres test-postgres-down clean help

build:
	mkdir -p bin
	go build -o bin/factory ./cmd/factory

container-build:
	docker build --build-arg VERSION=$$(git describe --tags --always --dirty 2>/dev/null || echo dev) -t factory-web:local .

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

vet:
	go vet ./...

test-postgres-up:
	docker compose -f docker-compose.postgres-test.yml up -d --wait

test-postgres: test-postgres-up
	@port="$$(docker compose -f docker-compose.postgres-test.yml port postgres-test 5432 | awk -F: '{print $$NF}')"; \
	  test -n "$$port" || { echo 'could not resolve published PostgreSQL test port' >&2; exit 1; }; \
	  FACTORY_TEST_POSTGRES_URL="postgres://factory:factory@127.0.0.1:$$port/factory_test?sslmode=disable" FACTORY_REQUIRE_POSTGRES_TEST=1 \
	  go test ./internal/restjobs ./internal/restserver/runtime ./internal/issuepoller -run '^(TestPostgresOwnershipIntegration|TestPostgresStoreIntegration|TestPostgresWorkerIdentityAndSafeProgressIntegration|TestPostgresThreadNotificationAtomicTransitionAndConcurrentStores|TestPostgresWorkerSessionIntegration|TestPostgresWorkerSessionAndWebhookMigrationsUpgradeFromV10|TestPostgresGitHubWebhookAdmissionIsAtomicAndDeliveryIdempotent|TestRunConfiguredPostgresStoreIntegration|TestPostgresControlPlaneAndSeparateWorkerProcess|TestIssuePollerPostgresGuardAndAdmissionIntegration|TestPostgresProvenanceConflictQuarantinesAndAdvancesScan|TestPostgresIssueAdmissionCreatesOneThreadAtomically)$$' -count=1 -v

test-postgres-down:
	docker compose -f docker-compose.postgres-test.yml down --volumes

clean:
	rm -rf bin

help:
	@printf '%-8s %s\n' \
		build 'Compile the Factory CLI' \
		container-build 'Build the REST control-plane image' \
		test 'Run all Go tests' \
		fmt 'Format Go source files' \
		vet 'Run go vet' \
		test-postgres-up 'Start the disposable PostgreSQL 18 test service' \
		test-postgres 'Run PostgreSQL 18 Store, worker, webhook, migration, and issue-poller integration tests' \
		test-postgres-down 'Stop the PostgreSQL 18 test service' \
		clean 'Remove build artifacts' \
		help 'List available Make targets'
