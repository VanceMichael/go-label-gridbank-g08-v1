package domain

import "time"

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}

type Role string

const (
	RoleTenantAdmin Role = "tenant_admin"
	RoleOperator    Role = "operator"
	RoleReviewer    Role = "reviewer"
	RoleDataSteward Role = "data_steward"
	RoleWorker      Role = "worker"
)

func (r Role) Valid() bool {
	switch r {
	case RoleTenantAdmin, RoleOperator, RoleReviewer, RoleDataSteward, RoleWorker:
		return true
	default:
		return false
	}
}

type User struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	Email        string    `json:"email"`
	DisplayName  string    `json:"display_name"`
	PasswordHash string    `json:"-"`
	Role         Role      `json:"role"`
	Active       bool      `json:"active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	Version      int64     `json:"version"`
}

type AuthSession struct {
	ID        string
	TenantID  string
	UserID    string
	TokenHash string
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedAt time.Time
	Version   int64
}

func (s AuthSession) UsableAt(now time.Time) bool {
	return s.RevokedAt == nil && now.Before(s.ExpiresAt)
}

type Provider struct {
	ID        string
	TenantID  string
	Name      string
	Timezone  string
	Active    bool
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}

type PoolCapability uint64

const (
	CapabilityGPU PoolCapability = 1 << iota
	CapabilityCPU
	CapabilityMemory
	CapabilityStorage
	CapabilityNetwork
	CapabilityRDMA
	CapabilityConfidentialCompute
)

type ComputePool struct {
	ID           string
	TenantID     string
	ProviderID   string
	Name         string
	Capabilities PoolCapability
	Active       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Version      int64
}

func (r ComputePool) Supports(required PoolCapability) bool {
	return required != 0 && r.Capabilities&required == required
}

type PoolLease struct {
	ID         string
	TenantID   string
	PoolID     string
	WorkloadID string
	Owner      string
	Token      string
	ExpiresAt  time.Time
	ReleasedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Version    int64
}

func (l PoolLease) ActiveAt(now time.Time) bool {
	return l.ReleasedAt == nil && now.Before(l.ExpiresAt)
}

type CapacityOffer struct {
	ID                   string
	TenantID             string
	Name                 string
	Environment          string
	RequiredCapabilities PoolCapability
	Active               bool
	CreatedAt            time.Time
	UpdatedAt            time.Time
	Version              int64
}

type WorkloadSession struct {
	ID              string
	TenantID        string
	ProviderID      string
	CapacityOfferID string
	PoolID          string
	OperatorID      string
	Status          JobWorkloadStatus
	Revision        int64
	ReservationRef  string
	StartedAt       *time.Time
	SubmittedAt     *time.Time
	SettledAt       *time.Time
	CanceledAt      *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Version         int64
}

type CapacityKind string

const (
	CapacityGPU     CapacityKind = "gpu"
	CapacityCPU     CapacityKind = "cpu"
	CapacityMemory  CapacityKind = "memory"
	CapacityStorage CapacityKind = "storage"
	CapacityNetwork CapacityKind = "network"
)

func (k CapacityKind) Valid() bool {
	switch k {
	case CapacityGPU, CapacityCPU, CapacityMemory, CapacityStorage, CapacityNetwork:
		return true
	default:
		return false
	}
}

type CapacityStream struct {
	ID           string
	TenantID     string
	WorkloadID   string
	Kind         CapacityKind
	Status       StreamStatus
	SegmentCount int
	FirstNanos   int64
	LastNanos    int64
	Digest       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Version      int64
}

type CapacitySegment struct {
	ID             string
	TenantID       string
	StreamID       string
	Sequence       int
	StartNanos     int64
	EndNanos       int64
	ObjectURI      string
	Checksum       string
	IdempotencyKey string
	CreatedAt      time.Time
}

type MeteringBatch struct {
	ID             string
	TenantID       string
	WorkloadID     string
	Status         MeteringStatus
	Owner          string
	LeaseToken     string
	LeaseExpiresAt *time.Time
	SubmittedAt    *time.Time
	ReviewedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int64
}

type MeteringItem struct {
	ID        string
	TenantID  string
	BatchID   string
	SegmentID string
	Label     string
	Payload   string
	Complete  bool
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}

type QualityReview struct {
	ID         string
	TenantID   string
	ObjectType string
	ObjectID   string
	ReviewerID string
	Outcome    string
	Reason     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Version    int64
}

type LedgerDraft struct {
	ID        string
	TenantID  string
	Name      string
	Status    LedgerStatus
	Revision  int64
	Digest    string
	ItemCount int
	CreatedAt time.Time
	UpdatedAt time.Time
	FrozenAt  *time.Time
	Version   int64
}

type LedgerItem struct {
	ID         string
	TenantID   string
	LedgerID   string
	WorkloadID string
	Revision   int64
	CreatedAt  time.Time
}

type LedgerRelease struct {
	ID          string
	TenantID    string
	LedgerID    string
	Revision    int64
	Digest      string
	Status      LedgerStatus
	PublishedAt *time.Time
	RevokedAt   *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Version     int64
}

type SchedulerJob struct {
	ID             string
	TenantID       string
	ReleaseID      string
	Status         JobStatus
	Owner          string
	LeaseToken     string
	LeaseExpiresAt *time.Time
	AttemptCount   int
	MaxAttempts    int
	Checkpoint     string
	OutputURI      string
	LastError      string
	NextAttemptAt  time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int64
}

type JobAttempt struct {
	ID         string
	TenantID   string
	JobID      string
	Attempt    int
	WorkerID   string
	StartedAt  time.Time
	FinishedAt *time.Time
	Outcome    string
	ErrorText  string
}

type OutboxEvent struct {
	ID             string
	TenantID       string
	Topic          string
	AggregateType  string
	AggregateID    string
	Payload        string
	Status         OutboxStatus
	Owner          string
	LeaseToken     string
	LeaseExpiresAt *time.Time
	AttemptCount   int
	MaxAttempts    int
	NextAttemptAt  time.Time
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int64
}

type AuditEvent struct {
	ID         string
	TenantID   string
	ActorID    string
	Action     string
	ObjectType string
	ObjectID   string
	Outcome    string
	RequestID  string
	Detail     string
	CreatedAt  time.Time
}

type IdempotencyRecord struct {
	ID          string
	TenantID    string
	Method      string
	Path        string
	Key         string
	Fingerprint string
	StatusCode  int
	Response    []byte
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

// CreditAccount is the tenant's durable compute-credit balance. Amounts are
// integer micro-units so reservation and settlement do not depend on floats.
type CreditAccount struct {
	ID        string
	TenantID  string
	Currency  string
	Balance   int64
	Held      int64
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}

type CreditEntryKind string

const (
	CreditDeposit  CreditEntryKind = "deposit"
	CreditHold     CreditEntryKind = "hold"
	CreditRelease  CreditEntryKind = "release"
	CreditCharge   CreditEntryKind = "charge"
	CreditReversal CreditEntryKind = "reversal"
)

type CreditEntry struct {
	ID             string
	TenantID       string
	AccountID      string
	WorkloadID     string
	SettlementID   string
	Kind           CreditEntryKind
	Amount         int64
	IdempotencyKey string
	CreatedAt      time.Time
}

type SettlementStatus string

const (
	SettlementPending   SettlementStatus = "pending"
	SettlementRunning   SettlementStatus = "running"
	SettlementCompleted SettlementStatus = "completed"
	SettlementFailed    SettlementStatus = "failed"
	SettlementReversed  SettlementStatus = "reversed"
)

type SettlementRun struct {
	ID         string
	TenantID   string
	WorkloadID string
	AccountID  string
	Status     SettlementStatus
	Amount     int64
	ErrorText  string
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Version    int64
}
