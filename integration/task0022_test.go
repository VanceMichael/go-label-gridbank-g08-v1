package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestUsageBatchFailureRollsBackAllSegments(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Task 0022 Provider", "UTC", "task0022-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Task 0022 Pool", domain.CapabilityGPU, "task0022-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Task 0022 Offer", "batch", domain.CapabilityGPU, "task0022-offer")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := environment.workloads.Plan(ctx, environment.operator, workloadPlanInputForTask0022(providerValue.ID, offer.ID, pool.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.MarkReady(ctx, environment.operator, plan.Workload.ID, "task0022-ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.Start(ctx, environment.operator, plan.Workload.ID, "task0022-start"); err != nil {
		t.Fatal(err)
	}
	stream, err := environment.capacitys.OpenStream(ctx, environment.operator, plan.Workload.ID, domain.CapacityGPU, "task0022-open")
	if err != nil {
		t.Fatal(err)
	}
	inputs := []capacity.SegmentInput{
		{Sequence: 0, StartNanos: 1, EndNanos: 2, ObjectURI: "s3://task0022/0", Checksum: checksum("task0022-0"), IdempotencyKey: "task0022-0"},
		{Sequence: 1, StartNanos: 2, EndNanos: 3, ObjectURI: "s3://task0022/1", Checksum: checksum("task0022-1"), IdempotencyKey: "task0022-1"},
	}
	if _, err := environment.database.SQL().Exec(`
		CREATE TRIGGER fail_task0022_second_segment
		BEFORE INSERT ON usage_records
		WHEN NEW.sequence = 1
		BEGIN SELECT RAISE(ABORT, 'forced second segment failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.capacitys.Append(ctx, environment.operator, stream.ID, "task0022-failing-append", inputs); err == nil {
		t.Fatal("usage batch unexpectedly succeeded while the second segment was rejected")
	}
	var segments int
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM usage_records WHERE stream_id = ?`, stream.ID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if segments != 0 {
		t.Fatalf("failed usage batch left %d segment(s)", segments)
	}
	if _, err := environment.database.SQL().Exec(`DROP TRIGGER fail_task0022_second_segment`); err != nil {
		t.Fatal(err)
	}
	result, err := environment.capacitys.Append(ctx, environment.operator, stream.ID, "task0022-retry", inputs)
	if err != nil {
		t.Fatal(err)
	}
	if result.Inserted != 2 || result.Replayed != 0 {
		t.Fatalf("retry result = inserted %d replayed %d", result.Inserted, result.Replayed)
	}
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM usage_records WHERE stream_id = ?`, stream.ID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if segments != 2 {
		t.Fatalf("successful retry left %d segment(s), want 2", segments)
	}
}

func workloadPlanInputForTask0022(providerID, offerID, poolID string) workload.PlanInput {
	return workload.PlanInput{ProviderID: providerID, CapacityOfferID: offerID, PoolID: poolID, ReservationRef: "task0022-reservation", IdempotencyKey: "task0022-workload", RequestID: "task0022-plan"}
}
