package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestCanceledUsageIngestLeavesNoSegments(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Canceled Usage Provider", "UTC", "canceled-usage-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Canceled Usage Pool", domain.CapabilityGPU, "canceled-usage-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Canceled Usage Offer", "edge", domain.CapabilityGPU, "canceled-usage-offer")
	if err != nil {
		t.Fatal(err)
	}
	planned, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "canceled-usage", IdempotencyKey: "canceled-usage", RequestID: "canceled-usage"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.MarkReady(ctx, environment.operator, planned.Workload.ID, "canceled-usage-ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.Start(ctx, environment.operator, planned.Workload.ID, "canceled-usage-start"); err != nil {
		t.Fatal(err)
	}
	stream, err := environment.capacitys.OpenStream(ctx, environment.operator, planned.Workload.ID, domain.CapacityGPU, "canceled-usage-stream")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = environment.capacitys.Append(canceled, environment.operator, stream.ID, "canceled-usage-append", []capacity.SegmentInput{{Sequence: 0, StartNanos: 1, EndNanos: 2, ObjectURI: "s3://canceled-usage", Checksum: domain.Fingerprint("canceled-usage"), IdempotencyKey: "canceled-usage-segment"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled usage append error = %v, want context canceled", err)
	}
	var segments int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_records WHERE stream_id = ?`, stream.ID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if segments != 0 {
		t.Fatalf("canceled usage append left %d segment(s)", segments)
	}
}
