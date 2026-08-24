package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestCanceledWorkloadTransitionDoesNotCommit(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Canceled Submit Provider", "UTC", "canceled-submit-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Canceled Submit Pool", domain.CapabilityGPU|domain.CapabilityCPU, "canceled-submit-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Canceled Submit Offer", "edge", domain.CapabilityGPU, "canceled-submit-offer")
	if err != nil {
		t.Fatal(err)
	}
	planned, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "canceled-submit", IdempotencyKey: "canceled-submit", RequestID: "canceled-submit"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.MarkReady(ctx, environment.operator, planned.Workload.ID, "canceled-submit-ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.Start(ctx, environment.operator, planned.Workload.ID, "canceled-submit-start"); err != nil {
		t.Fatal(err)
	}
	for index, kind := range []domain.CapacityKind{domain.CapacityGPU, domain.CapacityCPU} {
		stream, err := environment.capacitys.OpenStream(ctx, environment.operator, planned.Workload.ID, kind, "canceled-submit-stream-"+string(rune('a'+index)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := environment.capacitys.Append(ctx, environment.operator, stream.ID, "canceled-submit-append", []capacity.SegmentInput{{Sequence: 0, StartNanos: 1, EndNanos: 2, ObjectURI: "s3://cancel/" + stream.ID, Checksum: domain.Fingerprint(stream.ID), IdempotencyKey: "canceled-segment-" + string(rune('a'+index))}}); err != nil {
			t.Fatal(err)
		}
		if _, err := environment.capacitys.Seal(ctx, environment.operator, stream.ID, "canceled-submit-seal"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.capacitys.AlignWorkload(ctx, environment.operator, planned.Workload.ID, "canceled-submit-align", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = environment.workloads.Submit(canceled, environment.operator, planned.Workload.ID, "canceled-submit-request")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submit error = %v, want context canceled", err)
	}
	current, err := environment.workloads.Get(ctx, environment.operator, planned.Workload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.WorkloadRunning {
		t.Fatalf("canceled submit changed workload status to %s", current.Status)
	}
	var events int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic = 'workload.submit'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("canceled submit wrote %d outbox events", events)
	}
}
