package domain

import (
	"errors"
	"testing"
)

func TestWorkloadTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		from JobWorkloadStatus
		to   JobWorkloadStatus
		ok   bool
	}{
		{name: "planned ready", from: WorkloadQueued, to: WorkloadAllocated, ok: true},
		{name: "planned canceled", from: WorkloadQueued, to: WorkloadCanceled, ok: true},
		{name: "ready executing", from: WorkloadAllocated, to: WorkloadRunning, ok: true},
		{name: "ready canceled", from: WorkloadAllocated, to: WorkloadCanceled, ok: true},
		{name: "executing metering", from: WorkloadRunning, to: WorkloadMetering, ok: true},
		{name: "executing canceled", from: WorkloadRunning, to: WorkloadCanceled, ok: true},
		{name: "metering settled", from: WorkloadMetering, to: WorkloadSettled, ok: true},
		{name: "metering rejected", from: WorkloadMetering, to: WorkloadFailed, ok: true},
		{name: "rejected ready", from: WorkloadFailed, to: WorkloadAllocated, ok: true},
		{name: "rejected archived", from: WorkloadFailed, to: WorkloadArchived, ok: true},
		{name: "settled archived", from: WorkloadSettled, to: WorkloadArchived, ok: true},
		{name: "canceled archived", from: WorkloadCanceled, to: WorkloadArchived, ok: true},
		{name: "planned metering", from: WorkloadQueued, to: WorkloadMetering},
		{name: "ready settled", from: WorkloadAllocated, to: WorkloadSettled},
		{name: "executing ready", from: WorkloadRunning, to: WorkloadAllocated},
		{name: "metering executing", from: WorkloadMetering, to: WorkloadRunning},
		{name: "settled rejected", from: WorkloadSettled, to: WorkloadFailed},
		{name: "archived ready", from: WorkloadArchived, to: WorkloadAllocated},
		{name: "unknown ready", from: JobWorkloadStatus("unknown"), to: WorkloadAllocated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.from.Transition(test.to)
			if test.ok && err != nil {
				t.Fatalf("expected transition to succeed: %v", err)
			}
			if !test.ok && !errors.Is(err, ErrPrecondition) {
				t.Fatalf("expected precondition error, got %v", err)
			}
		})
	}
}

func TestManifestTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		from StreamStatus
		to   StreamStatus
		ok   bool
	}{
		{StreamOpen, StreamSealed, true},
		{StreamSealed, StreamAligned, true},
		{StreamSealed, StreamInvalid, true},
		{StreamInvalid, StreamOpen, true},
		{StreamOpen, StreamAligned, false},
		{StreamAligned, StreamOpen, false},
		{StreamInvalid, StreamAligned, false},
	}
	for _, test := range tests {
		err := test.from.Transition(test.to)
		if test.ok && err != nil {
			t.Errorf("%s -> %s unexpectedly failed: %v", test.from, test.to, err)
		}
		if !test.ok && !errors.Is(err, ErrPrecondition) {
			t.Errorf("%s -> %s should be rejected, got %v", test.from, test.to, err)
		}
	}
}

func TestMeteringTransitions(t *testing.T) {
	t.Parallel()
	allowed := map[MeteringStatus][]MeteringStatus{
		MeteringOpen:      {MeteringClaimed, MeteringCanceled},
		MeteringClaimed:   {MeteringSubmitted, MeteringOpen},
		MeteringSubmitted: {MeteringAccepted, MeteringRework},
		MeteringRework:    {MeteringClaimed, MeteringCanceled},
	}
	states := []MeteringStatus{MeteringOpen, MeteringClaimed, MeteringSubmitted, MeteringAccepted, MeteringRework, MeteringCanceled}
	for _, from := range states {
		for _, to := range states {
			want := false
			for _, candidate := range allowed[from] {
				if candidate == to {
					want = true
				}
			}
			err := from.Transition(to)
			if want && err != nil {
				t.Errorf("%s -> %s unexpectedly failed: %v", from, to, err)
			}
			if !want && !errors.Is(err, ErrPrecondition) {
				t.Errorf("%s -> %s should fail with precondition, got %v", from, to, err)
			}
		}
	}
}

func TestLedgerTransitions(t *testing.T) {
	t.Parallel()
	allowed := map[LedgerStatus][]LedgerStatus{
		LedgerStatusDraft:     {LedgerStatusFrozen, LedgerStatusArchived},
		LedgerStatusFrozen:    {LedgerStatusApproved, LedgerStatusDraft},
		LedgerStatusApproved:  {LedgerStatusPublished, LedgerStatusDraft},
		LedgerStatusPublished: {LedgerStatusRevoked},
		LedgerStatusRevoked:   {LedgerStatusArchived},
	}
	states := []LedgerStatus{LedgerStatusDraft, LedgerStatusFrozen, LedgerStatusApproved, LedgerStatusPublished, LedgerStatusRevoked, LedgerStatusArchived}
	for _, from := range states {
		for _, to := range states {
			want := false
			for _, candidate := range allowed[from] {
				want = want || candidate == to
			}
			err := from.Transition(to)
			if want != (err == nil) {
				t.Errorf("transition %s -> %s: wanted success=%v, got %v", from, to, want, err)
			}
		}
	}
}

func TestJobTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		from JobStatus
		to   JobStatus
		ok   bool
	}{
		{JobQueued, JobRunning, true},
		{JobQueued, JobCanceled, true},
		{JobRunning, JobSucceeded, true},
		{JobRunning, JobRetrying, true},
		{JobRunning, JobFailed, true},
		{JobRunning, JobCanceled, true},
		{JobRetrying, JobRunning, true},
		{JobRetrying, JobFailed, true},
		{JobRetrying, JobCanceled, true},
		{JobQueued, JobSucceeded, false},
		{JobSucceeded, JobRunning, false},
		{JobFailed, JobRetrying, false},
	}
	for _, test := range tests {
		err := test.from.Transition(test.to)
		if test.ok && err != nil {
			t.Errorf("expected %s -> %s to succeed: %v", test.from, test.to, err)
		}
		if !test.ok && !errors.Is(err, ErrPrecondition) {
			t.Errorf("expected %s -> %s to be rejected, got %v", test.from, test.to, err)
		}
	}
}

func TestRolesAndCapacityKinds(t *testing.T) {
	t.Parallel()
	roles := []struct {
		value Role
		valid bool
	}{
		{RoleTenantAdmin, true},
		{RoleOperator, true},
		{RoleReviewer, true},
		{RoleDataSteward, true},
		{RoleWorker, true},
		{Role("root"), false},
		{Role(""), false},
	}
	for _, role := range roles {
		if got := role.value.Valid(); got != role.valid {
			t.Errorf("role %q validity = %v, want %v", role.value, got, role.valid)
		}
	}
	kinds := []struct {
		value CapacityKind
		valid bool
	}{
		{CapacityGPU, true},
		{CapacityCPU, true},
		{CapacityMemory, true},
		{CapacityStorage, true},
		{CapacityNetwork, true},
		{CapacityKind("audio"), false},
	}
	for _, kind := range kinds {
		if got := kind.value.Valid(); got != kind.valid {
			t.Errorf("capacity kind %q validity = %v, want %v", kind.value, got, kind.valid)
		}
	}
}

func TestPoolSupportsAllRequiredCapabilities(t *testing.T) {
	t.Parallel()
	pool := ComputePool{Capabilities: CapabilityGPU | CapabilityCPU | CapabilityMemory | CapabilityStorage}
	if !pool.Supports(CapabilityGPU | CapabilityCPU) {
		t.Fatal("pool should support required pose and force capabilities")
	}
	if pool.Supports(CapabilityGPU | CapabilityRDMA) {
		t.Fatal("pool must reject requirements containing an unsupported capability")
	}
	if pool.Supports(0) {
		t.Fatal("an empty requirement must not be treated as a usable workload capability")
	}
}
