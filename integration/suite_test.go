package integration_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/ledger"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/metering"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/outbox"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/provider"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/recovery"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/scheduler"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

type testEnvironment struct {
	database     *storage.Database
	databasePath string
	clock        *clock.Manual
	auth         *auth.Service
	providers    *provider.Service
	workloads    *workload.Service
	capacitys    *capacity.Service
	meterings    *metering.Service
	ledgers      *ledger.Service
	scheduler    *scheduler.Service
	outbox       *outbox.Service
	recovery     *recovery.Service
	admin        auth.Principal
	operator     auth.Principal
	reviewer     auth.Principal
	steward      auth.Principal
	worker       auth.Principal
}

func newEnvironment(t *testing.T) *testEnvironment {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "integration.db")
	database, err := storage.Open(context.Background(), storage.Options{Path: databasePath, MaxOpenConns: 8, BusyTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close integration database: %v", err)
		}
	})
	manualClock := clock.NewManual(time.Date(2026, time.August, 24, 2, 0, 0, 0, time.UTC))
	environment := &testEnvironment{
		database:     database,
		databasePath: databasePath,
		clock:        manualClock,
		auth:         auth.NewService(database, manualClock, 8*time.Hour),
		providers:    provider.NewService(database, manualClock, 2*time.Minute),
		workloads:    workload.NewService(database, manualClock, 2*time.Minute),
		capacitys:    capacity.NewService(database, manualClock),
		meterings:    metering.NewService(database, manualClock, 2*time.Minute),
		ledgers:      ledger.NewService(database, manualClock),
		scheduler:    scheduler.NewService(database, manualClock, 2*time.Minute, time.Second, 3),
		outbox:       outbox.NewService(database, manualClock, 2*time.Minute, time.Second),
		recovery:     recovery.NewService(database, manualClock),
	}
	tenant, adminUser, err := environment.auth.Bootstrap(context.Background(), auth.BootstrapInput{
		TenantName:  "Motion Lab",
		Email:       "admin@motion.test",
		DisplayName: "Admin",
		Password:    "test-password-admin",
	})
	if err != nil {
		t.Fatal(err)
	}
	environment.admin = auth.Principal{TenantID: tenant.ID, UserID: adminUser.ID, Role: adminUser.Role, SessionID: "fixture-admin"}
	environment.operator = environment.createPrincipal(t, "operator@motion.test", "Workload Operator", domain.RoleOperator)
	environment.reviewer = environment.createPrincipal(t, "reviewer@motion.test", "Data Reviewer", domain.RoleReviewer)
	environment.steward = environment.createPrincipal(t, "steward@motion.test", "Data Steward", domain.RoleDataSteward)
	environment.worker = environment.createPrincipal(t, "worker@motion.test", "Scheduler Worker", domain.RoleWorker)
	return environment
}

func (e *testEnvironment) createPrincipal(t *testing.T, email, name string, role domain.Role) auth.Principal {
	t.Helper()
	user, err := e.auth.CreateUser(context.Background(), e.admin, email, name, "test-password-user", role, "fixture-user-"+string(role))
	if err != nil {
		t.Fatal(err)
	}
	return auth.Principal{TenantID: e.admin.TenantID, UserID: user.ID, Role: user.Role, SessionID: "fixture-" + string(role)}
}

type workloadFixture struct {
	provider       domain.Provider
	pool           domain.ComputePool
	capacity_offer domain.CapacityOffer
	plan           workload.PlanResult
	pose           domain.CapacityStream
	force          domain.CapacityStream
}

func (e *testEnvironment) settledWorkload(t *testing.T) workloadFixture {
	t.Helper()
	ctx := context.Background()
	providerValue, err := e.providers.CreateProvider(ctx, e.admin, "Precision Lab", "Asia/Shanghai", "fixture-provider")
	if err != nil {
		t.Fatal(err)
	}
	capabilities := domain.CapabilityGPU | domain.CapabilityCPU | domain.CapabilityMemory
	pool, err := e.providers.CreatePool(ctx, e.admin, providerValue.ID, "Pool Alpha", capabilities, "fixture-pool")
	if err != nil {
		t.Fatal(err)
	}
	capacity_offer, err := e.providers.CreateCapacityOffer(ctx, e.admin, "Shelf grasp", "retail", domain.CapabilityGPU|domain.CapabilityCPU, "fixture-capacity_offer")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := e.workloads.Plan(ctx, e.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: capacity_offer.ID, PoolID: pool.ID, ReservationRef: "consent-fixture", IdempotencyKey: "fixture-workload", RequestID: "fixture-plan"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.workloads.MarkReady(ctx, e.operator, plan.Workload.ID, "fixture-ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.workloads.Start(ctx, e.operator, plan.Workload.ID, "fixture-start"); err != nil {
		t.Fatal(err)
	}
	pose, err := e.capacitys.OpenStream(ctx, e.operator, plan.Workload.ID, domain.CapacityGPU, "fixture-pose")
	if err != nil {
		t.Fatal(err)
	}
	force, err := e.capacitys.OpenStream(ctx, e.operator, plan.Workload.ID, domain.CapacityCPU, "fixture-force")
	if err != nil {
		t.Fatal(err)
	}
	for _, manifest := range []domain.CapacityStream{pose, force} {
		inputs := []capacity.SegmentInput{
			{Sequence: 0, StartNanos: 1_000_000, EndNanos: 2_000_000, ObjectURI: "s3://fixture/" + manifest.ID + "/0", Checksum: checksum("first-" + manifest.ID), IdempotencyKey: "segment-0"},
			{Sequence: 1, StartNanos: 2_000_000, EndNanos: 3_000_000, ObjectURI: "s3://fixture/" + manifest.ID + "/1", Checksum: checksum("second-" + manifest.ID), IdempotencyKey: "segment-1"},
		}
		if _, err := e.capacitys.Append(ctx, e.operator, manifest.ID, "fixture-append", inputs); err != nil {
			t.Fatal(err)
		}
		if _, err := e.capacitys.Seal(ctx, e.operator, manifest.ID, "fixture-seal"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.capacitys.AlignWorkload(ctx, e.operator, plan.Workload.ID, "fixture-align", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := e.workloads.Submit(ctx, e.operator, plan.Workload.ID, "fixture-submit"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.workloads.Settle(ctx, e.reviewer, plan.Workload.ID, "fixture-validate"); err != nil {
		t.Fatal(err)
	}
	return workloadFixture{provider: providerValue, pool: pool, capacity_offer: capacity_offer, plan: plan, pose: pose, force: force}
}

func checksum(value string) string {
	return domain.Fingerprint(value)
}

func assertErrorKind(t *testing.T, err, kind error) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("error = %v, want kind %v", err, kind)
	}
}
