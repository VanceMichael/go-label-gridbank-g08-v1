package domain

import "fmt"

type JobWorkloadStatus string

const (
	WorkloadQueued    JobWorkloadStatus = "planned"
	WorkloadAllocated JobWorkloadStatus = "ready"
	WorkloadRunning   JobWorkloadStatus = "executing"
	WorkloadMetering  JobWorkloadStatus = "metering"
	WorkloadSettled   JobWorkloadStatus = "settled"
	WorkloadFailed    JobWorkloadStatus = "failed"
	WorkloadCanceled  JobWorkloadStatus = "canceled"
	WorkloadArchived  JobWorkloadStatus = "archived"
)

var workloadTransitions = map[JobWorkloadStatus]map[JobWorkloadStatus]bool{
	WorkloadQueued:    {WorkloadAllocated: true, WorkloadCanceled: true},
	WorkloadAllocated: {WorkloadRunning: true, WorkloadCanceled: true},
	WorkloadRunning:   {WorkloadMetering: true, WorkloadCanceled: true},
	WorkloadMetering:  {WorkloadSettled: true, WorkloadFailed: true},
	WorkloadFailed:    {WorkloadAllocated: true, WorkloadArchived: true},
	WorkloadSettled:   {WorkloadArchived: true},
	WorkloadCanceled:  {WorkloadArchived: true},
}

func (s JobWorkloadStatus) Transition(to JobWorkloadStatus) error {
	if workloadTransitions[s][to] {
		return nil
	}
	return Precondition("job_workload.transition", "workload", "", fmt.Sprintf("cannot transition from %s to %s", s, to))
}

type StreamStatus string

const (
	StreamOpen    StreamStatus = "open"
	StreamSealed  StreamStatus = "sealed"
	StreamAligned StreamStatus = "aligned"
	StreamInvalid StreamStatus = "invalid"
)

func (s StreamStatus) Transition(to StreamStatus) error {
	valid := (s == StreamOpen && to == StreamSealed) ||
		(s == StreamSealed && (to == StreamAligned || to == StreamInvalid)) ||
		(s == StreamInvalid && to == StreamOpen)
	if !valid {
		return Precondition("capacity_stream.transition", "capacity_stream", "", fmt.Sprintf("cannot transition from %s to %s", s, to))
	}
	return nil
}

type MeteringStatus string

const (
	MeteringOpen      MeteringStatus = "open"
	MeteringClaimed   MeteringStatus = "claimed"
	MeteringSubmitted MeteringStatus = "submitted"
	MeteringAccepted  MeteringStatus = "accepted"
	MeteringRework    MeteringStatus = "rework"
	MeteringCanceled  MeteringStatus = "canceled"
)

func (s MeteringStatus) Transition(to MeteringStatus) error {
	allowed := map[MeteringStatus]map[MeteringStatus]bool{
		MeteringOpen:      {MeteringClaimed: true, MeteringCanceled: true},
		MeteringClaimed:   {MeteringSubmitted: true, MeteringOpen: true},
		MeteringSubmitted: {MeteringAccepted: true, MeteringRework: true},
		MeteringRework:    {MeteringClaimed: true, MeteringCanceled: true},
	}
	if !allowed[s][to] {
		return Precondition("metering.transition", "metering_batch", "", fmt.Sprintf("cannot transition from %s to %s", s, to))
	}
	return nil
}

type LedgerStatus string

const (
	LedgerStatusDraft     LedgerStatus = "draft"
	LedgerStatusFrozen    LedgerStatus = "frozen"
	LedgerStatusApproved  LedgerStatus = "approved"
	LedgerStatusPublished LedgerStatus = "published"
	LedgerStatusRevoked   LedgerStatus = "revoked"
	LedgerStatusArchived  LedgerStatus = "archived"
)

func (s LedgerStatus) Transition(to LedgerStatus) error {
	allowed := map[LedgerStatus]map[LedgerStatus]bool{
		LedgerStatusDraft:     {LedgerStatusFrozen: true, LedgerStatusArchived: true},
		LedgerStatusFrozen:    {LedgerStatusApproved: true, LedgerStatusDraft: true},
		LedgerStatusApproved:  {LedgerStatusPublished: true, LedgerStatusDraft: true},
		LedgerStatusPublished: {LedgerStatusRevoked: true},
		LedgerStatusRevoked:   {LedgerStatusArchived: true},
	}
	if !allowed[s][to] {
		return Precondition("ledger.transition", "ledger", "", fmt.Sprintf("cannot transition from %s to %s", s, to))
	}
	return nil
}

type JobStatus string

const (
	JobQueued    JobStatus = "queued"
	JobRunning   JobStatus = "running"
	JobRetrying  JobStatus = "retrying"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
	JobCanceled  JobStatus = "canceled"
)

func (s JobStatus) Transition(to JobStatus) error {
	allowed := map[JobStatus]map[JobStatus]bool{
		JobQueued:   {JobRunning: true, JobCanceled: true},
		JobRunning:  {JobSucceeded: true, JobRetrying: true, JobFailed: true, JobCanceled: true},
		JobRetrying: {JobRunning: true, JobFailed: true, JobCanceled: true},
	}
	if !allowed[s][to] {
		return Precondition("job.transition", "scheduler_job", "", fmt.Sprintf("cannot transition from %s to %s", s, to))
	}
	return nil
}

type OutboxStatus string

const (
	OutboxPending    OutboxStatus = "pending"
	OutboxDelivering OutboxStatus = "delivering"
	OutboxDelivered  OutboxStatus = "delivered"
	OutboxDead       OutboxStatus = "dead"
)
