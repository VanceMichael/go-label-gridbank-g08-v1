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

func TestCreditReservationKeepsBalanceAndHeldAtomic(t *testing.T) {
	db, err := storage.Open(context.Background(), storage.Options{Path: filepath.Join(t.TempDir(), "credits.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := clock.NewManual(time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))
	authService := auth.NewService(db, c, time.Hour)
	tenant, user, err := authService.Bootstrap(context.Background(), auth.BootstrapInput{TenantName: "Compute Cooperative", Email: "admin@gridbank.test", DisplayName: "Admin", Password: "test-password-admin"})
	if err != nil {
		t.Fatal(err)
	}
	admin := auth.Principal{TenantID: tenant.ID, UserID: user.ID, Role: user.Role}
	service := NewService(db, c)
	account, err := service.OpenCreditAccount(context.Background(), admin, "gpu", "open-account")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deposit(context.Background(), admin, account.ID, 100, "deposit-1", "deposit-request"); err != nil {
		t.Fatal(err)
	}
	operator := admin
	operator.Role = domain.RoleOperator
	providers := provider.NewService(db, c, 2*time.Minute)
	providerValue, err := providers.CreateProvider(context.Background(), admin, "North Compute Exchange", "Asia/Shanghai", "provider-request")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := providers.CreatePool(context.Background(), admin, providerValue.ID, "GPU Pool", domain.CapabilityGPU, "pool-request")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := providers.CreateCapacityOffer(context.Background(), admin, "GPU Hour", "edge", domain.CapabilityGPU, "offer-request")
	if err != nil {
		t.Fatal(err)
	}
	workloads := workload.NewService(db, c, 2*time.Minute)
	jobA, err := workloads.Plan(context.Background(), operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "resource-a", IdempotencyKey: "plan-a", RequestID: "plan-a"})
	if err != nil {
		t.Fatal(err)
	}
	workloadIDs := []string{jobA.Workload.ID, jobA.Workload.ID}
	start := make(chan struct{})
	var wait sync.WaitGroup
	results := make(chan error, 2)
	wait.Add(2)
	for i, workloadID := range workloadIDs {
		go func(id string, index int) {
			defer wait.Done()
			<-start
			_, err := service.Reserve(context.Background(), operator, account.ID, id, 75, "hold-"+string(rune('a'+index)), "reserve-"+string(rune('a'+index)))
			results <- err
		}(workloadID, i)
	}
	close(start)
	wait.Wait()
	close(results)
	successes := 0
	var observed []error
	for err := range results {
		if err == nil {
			successes++
		} else {
			observed = append(observed, err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent credit reservations succeeded %d times; errors=%v", successes, observed)
	}
	current, err := service.Account(context.Background(), admin, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Balance != 100 || current.Held != 75 {
		t.Fatalf("balance/held = %d/%d", current.Balance, current.Held)
	}
}
