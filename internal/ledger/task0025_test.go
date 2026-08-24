package ledger

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/provider"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestConcurrentCreditReservationCannotOverdraw(t *testing.T) {
	db, err := storage.Open(context.Background(), storage.Options{Path: filepath.Join(t.TempDir(), "task0025.db"), MaxOpenConns: 8, BusyTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	c := clock.NewManual(time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))
	authService := auth.NewService(db, c, time.Hour)
	tenant, user, err := authService.Bootstrap(ctx, auth.BootstrapInput{TenantName: "Concurrent Compute Cooperative", Email: "task0025-admin@gridbank.test", DisplayName: "Admin", Password: "test-password-admin"})
	if err != nil {
		t.Fatal(err)
	}
	admin := auth.Principal{TenantID: tenant.ID, UserID: user.ID, Role: user.Role}
	service := NewService(db, c)
	account, err := service.OpenCreditAccount(ctx, admin, "gpu", "task0025-open")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deposit(ctx, admin, account.ID, 100, "task0025-deposit", "task0025-deposit-request"); err != nil {
		t.Fatal(err)
	}
	operator := admin
	operator.Role = domain.RoleOperator
	providers := provider.NewService(db, c, 2*time.Minute)
	providerValue, err := providers.CreateProvider(ctx, admin, "Concurrent Exchange", "Asia/Shanghai", "task0025-provider")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := providers.CreateCapacityOffer(ctx, admin, "GPU Hour", "settlement", domain.CapabilityGPU, "task0025-offer")
	if err != nil {
		t.Fatal(err)
	}
	workloads := workload.NewService(db, c, 2*time.Minute)
	workloadIDs := make([]string, 0, 2)
	for index, key := range []string{"task0025-plan-a", "task0025-plan-b"} {
		pool, err := providers.CreatePool(ctx, admin, providerValue.ID, "GPU Pool "+key, domain.CapabilityGPU, "task0025-pool-"+key)
		if err != nil {
			t.Fatalf("create pool %d: %v", index, err)
		}
		planned, err := workloads.Plan(ctx, operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: key, IdempotencyKey: key, RequestID: key})
		if err != nil {
			t.Fatalf("plan workload %d: %v", index, err)
		}
		workloadIDs = append(workloadIDs, planned.Workload.ID)
	}

	start := make(chan struct{})
	results := make(chan error, len(workloadIDs))
	var wait sync.WaitGroup
	for index, workloadID := range workloadIDs {
		index, workloadID := index, workloadID
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := service.Reserve(ctx, operator, account.ID, workloadID, 75, "task0025-hold-"+string(rune('a'+index)), "task0025-reserve-"+string(rune('a'+index)))
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes := 0
	for reserveErr := range results {
		if reserveErr == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent credit reservations succeeded %d times, want exactly one", successes)
	}
	current, err := service.Account(ctx, admin, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Balance != 100 || current.Held != 75 {
		t.Fatalf("credit account balance/held = %d/%d, want 100/75", current.Balance, current.Held)
	}
	var holdCount int
	var holdTotal int64
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM credit_entries WHERE tenant_id = ? AND account_id = ? AND kind = 'hold'`, tenant.ID, account.ID).Scan(&holdCount, &holdTotal); err != nil {
		t.Fatal(err)
	}
	if holdCount != 1 || holdTotal != current.Held {
		t.Fatalf("durable hold entries = %d/%d credits, account held = %d; rejected reservation left a financial entry", holdCount, holdTotal, current.Held)
	}
}
