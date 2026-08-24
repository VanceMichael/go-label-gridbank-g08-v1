package httpapi

import (
	"net/http"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/ledger"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/scheduler"
)

func (a *API) createLedger(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		Name string `json:"name"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.LedgerDraft
	if err == nil {
		value, err = a.ledgers.Create(request.Context(), principal, input.Name, requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, value)
}

func (a *API) listLedgers(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	limit, limitErr := parseIntQuery(request, "limit", 50)
	offset, offsetErr := parseIntQuery(request, "offset", 0)
	statuses, statusErr := parseLedgerStatuses(request.URL.Query().Get("status"))
	if err == nil {
		err = limitErr
	}
	if err == nil {
		err = offsetErr
	}
	if err == nil {
		err = statusErr
	}
	var values []domain.LedgerDraft
	var total int
	if err == nil {
		values, total, err = a.ledgers.List(request.Context(), principal, ledger.ListFilter{Statuses: statuses, Search: request.URL.Query().Get("search"), Limit: limit, Offset: offset})
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": values, "total": total, "limit": limit, "offset": offset})
}

func (a *API) getLedger(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var draft domain.LedgerDraft
	var items []domain.LedgerItem
	if err == nil {
		draft, items, err = a.ledgers.Get(request.Context(), principal, request.PathValue("ledger_id"))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ledger": draft, "items": items})
}

func (a *API) addLedgerWorkloads(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		WorkloadIDs []string `json:"workload_ids"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var draft domain.LedgerDraft
	var items []domain.LedgerItem
	if err == nil {
		draft, items, err = a.ledgers.AddWorkloads(request.Context(), principal, request.PathValue("ledger_id"), requestIDFromContext(request.Context()), input.WorkloadIDs)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ledger": draft, "items": items})
}

func (a *API) freezeLedger(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var value domain.LedgerDraft
	if err == nil {
		value, err = a.ledgers.Freeze(request.Context(), principal, request.PathValue("ledger_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) reviewLedger(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		Approve bool   `json:"approve"`
		Reason  string `json:"reason"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.LedgerDraft
	if err == nil {
		value, err = a.ledgers.Review(request.Context(), principal, request.PathValue("ledger_id"), requestIDFromContext(request.Context()), input.Approve, input.Reason)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) publishLedger(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var value domain.LedgerRelease
	if err == nil {
		value, err = a.ledgers.Publish(request.Context(), principal, request.PathValue("ledger_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, value)
}

func (a *API) revokeRelease(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		Reason string `json:"reason"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.LedgerRelease
	if err == nil {
		value, err = a.ledgers.Revoke(request.Context(), principal, request.PathValue("release_id"), requestIDFromContext(request.Context()), input.Reason)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) enqueueScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var value domain.SchedulerJob
	if err == nil {
		value, err = a.scheduler.Enqueue(request.Context(), principal, request.PathValue("release_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, value)
}

func (a *API) claimScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var result scheduler.ClaimResult
	if err == nil {
		result, err = a.scheduler.Claim(request.Context(), principal)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) getScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var value domain.SchedulerJob
	if err == nil {
		value, err = a.scheduler.Get(request.Context(), principal, request.PathValue("job_id"))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) renewScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
		Version    int64  `json:"version"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.SchedulerJob
	if err == nil {
		value, err = a.scheduler.Renew(request.Context(), principal, request.PathValue("job_id"), input.LeaseToken, input.Version)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) checkpointScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
		Checkpoint string `json:"checkpoint"`
		Version    int64  `json:"version"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.SchedulerJob
	if err == nil {
		value, err = a.scheduler.Checkpoint(request.Context(), principal, request.PathValue("job_id"), input.LeaseToken, input.Checkpoint, input.Version)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) completeScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
		OutputURI  string `json:"output_uri"`
		Version    int64  `json:"version"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.SchedulerJob
	if err == nil {
		value, err = a.scheduler.Complete(request.Context(), principal, request.PathValue("job_id"), input.LeaseToken, input.OutputURI, requestIDFromContext(request.Context()), input.Version)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) failScheduler(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
		Message    string `json:"message"`
		Version    int64  `json:"version"`
		Permanent  bool   `json:"permanent"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.SchedulerJob
	if err == nil {
		value, err = a.scheduler.Fail(request.Context(), principal, request.PathValue("job_id"), input.LeaseToken, input.Message, requestIDFromContext(request.Context()), input.Version, input.Permanent)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}
