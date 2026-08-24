package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/ledger"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/metering"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/provider"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/scheduler"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

type Dependencies struct {
	Database  *storage.Database
	Auth      *auth.Service
	Providers *provider.Service
	Workloads *workload.Service
	Capacitys *capacity.Service
	Meterings *metering.Service
	Ledgers   *ledger.Service
	Scheduler *scheduler.Service
	Logger    *slog.Logger
}

type API struct {
	database  *storage.Database
	auth      *auth.Service
	providers *provider.Service
	workloads *workload.Service
	capacitys *capacity.Service
	meterings *metering.Service
	ledgers   *ledger.Service
	scheduler *scheduler.Service
	logger    *slog.Logger
}

func New(deps Dependencies) (*API, error) {
	if deps.Database == nil || deps.Auth == nil || deps.Providers == nil || deps.Workloads == nil || deps.Capacitys == nil || deps.Meterings == nil || deps.Ledgers == nil || deps.Scheduler == nil {
		return nil, errors.New("all API dependencies are required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &API{database: deps.Database, auth: deps.Auth, providers: deps.Providers, workloads: deps.Workloads, capacitys: deps.Capacitys, meterings: deps.Meterings, ledgers: deps.Ledgers, scheduler: deps.Scheduler, logger: deps.Logger}, nil
}

func (a *API) Handler() http.Handler {
	public := http.NewServeMux()
	public.HandleFunc("GET /healthz", a.health)
	public.HandleFunc("GET /readyz", a.ready)
	public.HandleFunc("POST /api/v1/bootstrap", a.bootstrap)
	public.HandleFunc("POST /api/v1/auth/login", a.login)

	protected := http.NewServeMux()
	protected.HandleFunc("POST /api/v1/auth/logout", a.logout)
	protected.HandleFunc("POST /api/v1/users", a.createUser)
	protected.HandleFunc("POST /api/v1/providers", a.createProvider)
	protected.HandleFunc("POST /api/v1/pools", a.createPool)
	protected.HandleFunc("POST /api/v1/capacity_offers", a.createCapacityOffer)
	protected.HandleFunc("POST /api/v1/workloads", a.planWorkload)
	protected.HandleFunc("GET /api/v1/workloads/{workload_id}", a.getWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/ready", a.readyWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/start", a.startWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/submit", a.submitWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/validate", a.settleWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/reject", a.rejectWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/cancel", a.cancelWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/reopen", a.reopenWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/streams", a.openStream)
	protected.HandleFunc("GET /api/v1/workloads/{workload_id}/streams", a.listStreams)
	protected.HandleFunc("POST /api/v1/streams/{stream_id}/segments", a.appendSegments)
	protected.HandleFunc("POST /api/v1/streams/{stream_id}/seal", a.sealStream)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/align", a.alignWorkload)
	protected.HandleFunc("POST /api/v1/workloads/{workload_id}/metering-batches", a.createMeteringBatch)
	protected.HandleFunc("GET /api/v1/metering-batches/{batch_id}", a.getMeteringBatch)
	protected.HandleFunc("POST /api/v1/metering-batches/{batch_id}/claim", a.claimMeteringBatch)
	protected.HandleFunc("POST /api/v1/metering-batches/{batch_id}/renew", a.renewMeteringBatch)
	protected.HandleFunc("POST /api/v1/metering-batches/{batch_id}/items/{item_id}", a.recordMeteringItem)
	protected.HandleFunc("POST /api/v1/metering-batches/{batch_id}/submit", a.submitMeteringBatch)
	protected.HandleFunc("POST /api/v1/metering-batches/{batch_id}/review", a.reviewMeteringBatch)
	protected.HandleFunc("POST /api/v1/ledgers", a.createLedger)
	protected.HandleFunc("GET /api/v1/ledgers", a.listLedgers)
	protected.HandleFunc("GET /api/v1/ledgers/{ledger_id}", a.getLedger)
	protected.HandleFunc("POST /api/v1/ledgers/{ledger_id}/workloads", a.addLedgerWorkloads)
	protected.HandleFunc("POST /api/v1/ledgers/{ledger_id}/freeze", a.freezeLedger)
	protected.HandleFunc("POST /api/v1/ledgers/{ledger_id}/review", a.reviewLedger)
	protected.HandleFunc("POST /api/v1/ledgers/{ledger_id}/publish", a.publishLedger)
	protected.HandleFunc("POST /api/v1/releases/{release_id}/revoke", a.revokeRelease)
	protected.HandleFunc("POST /api/v1/releases/{release_id}/scheduler-jobs", a.enqueueScheduler)
	protected.HandleFunc("POST /api/v1/workers/scheduler/claim", a.claimScheduler)
	protected.HandleFunc("GET /api/v1/scheduler-jobs/{job_id}", a.getScheduler)
	protected.HandleFunc("POST /api/v1/scheduler-jobs/{job_id}/renew", a.renewScheduler)
	protected.HandleFunc("POST /api/v1/scheduler-jobs/{job_id}/checkpoint", a.checkpointScheduler)
	protected.HandleFunc("POST /api/v1/scheduler-jobs/{job_id}/complete", a.completeScheduler)
	protected.HandleFunc("POST /api/v1/scheduler-jobs/{job_id}/fail", a.failScheduler)

	root := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if isPublic(request.Method, request.URL.Path) {
			public.ServeHTTP(writer, request)
			return
		}
		a.authenticate(protected).ServeHTTP(writer, request)
	})
	return withRequestID(recoverPanic(a.logger, requestLog(a.logger, root)))
}

func isPublic(method, path string) bool {
	return method == http.MethodGet && (path == "/healthz" || path == "/readyz") ||
		method == http.MethodPost && (path == "/api/v1/bootstrap" || path == "/api/v1/auth/login")
}

func (a *API) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "alive"})
}

func (a *API) ready(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := a.database.Ping(ctx); err != nil {
		writeError(a.logger, writer, request, domain.Wrap(domain.ErrUnavailable, "http.ready", "", "", "database is unavailable", err))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return domain.Validation("http.decode", "invalid JSON body: "+err.Error())
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.Validation("http.decode", "request body must contain one JSON object")
	}
	return nil
}

func parseIntQuery(request *http.Request, key string, fallback int) (int, error) {
	raw := request.URL.Query().Get(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, domain.Validation("http.query", key+" must be an integer")
	}
	return value, nil
}

func parseLedgerStatuses(raw string) ([]domain.LedgerStatus, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	values := strings.Split(raw, ",")
	statuses := make([]domain.LedgerStatus, 0, len(values))
	for _, value := range values {
		status := domain.LedgerStatus(strings.TrimSpace(value))
		switch status {
		case domain.LedgerStatusDraft, domain.LedgerStatusFrozen, domain.LedgerStatusApproved,
			domain.LedgerStatusPublished, domain.LedgerStatusRevoked, domain.LedgerStatusArchived:
			statuses = append(statuses, status)
		default:
			return nil, domain.Validation("http.query", "status contains an unsupported value")
		}
	}
	return statuses, nil
}
