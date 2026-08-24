package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestConcurrentPoolReservationHasOneOwner(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Concurrent Claim Provider", "UTC", "claim-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Concurrent Claim Pool", domain.CapabilityGPU, "claim-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Concurrent Claim Offer", "edge", domain.CapabilityGPU, "claim-offer")
	if err != nil {
		t.Fatal(err)
	}
	secondOperator := environment.createPrincipal(t, "claim-operator-2@motion.test", "Claim Operator Two", domain.RoleOperator)
	operators := []auth.Principal{environment.operator, secondOperator}
	start := make(chan struct{})
	results := make(chan workload.PlanResult, 2)
	errorsCh := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for index, operator := range operators {
		go func(index int, operator auth.Principal) {
			ready.Done()
			<-start
			result, err := environment.workloads.Plan(ctx, operator, workload.PlanInput{
				ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID,
				ReservationRef: "concurrent-claim", IdempotencyKey: "concurrent-claim-" + string(rune('a'+index)), RequestID: "concurrent-claim",
			})
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}(index, operator)
	}
	ready.Wait()
	close(start)
	var successCount, conflictCount int
	var winner workload.PlanResult
	for index := 0; index < 2; index++ {
		select {
		case result := <-results:
			successCount++
			winner = result
		case err := <-errorsCh:
			if errors.Is(err, domain.ErrConflict) {
				conflictCount++
			} else {
				t.Fatalf("unexpected reservation error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent reservations did not finish")
		}
	}
	if successCount != 1 || conflictCount != 1 {
		t.Fatalf("reservation outcomes success=%d conflict=%d, want 1/1", successCount, conflictCount)
	}
	var active int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE id = ? AND released_at IS NULL`, winner.Lease.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("winning reservation active lease count = %d", active)
	}
}
