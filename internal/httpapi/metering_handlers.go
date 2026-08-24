package httpapi

import (
	"net/http"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/metering"
)

func (a *API) createMeteringBatch(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var batch domain.MeteringBatch
	var items []domain.MeteringItem
	if err == nil {
		batch, items, err = a.meterings.Create(request.Context(), principal, request.PathValue("workload_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"batch": batch, "items": items})
}

func (a *API) getMeteringBatch(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var batch domain.MeteringBatch
	var items []domain.MeteringItem
	if err == nil {
		batch, items, err = a.meterings.Get(request.Context(), principal, request.PathValue("batch_id"))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"batch": batch, "items": items})
}

func (a *API) claimMeteringBatch(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var result metering.ClaimResult
	if err == nil {
		result, err = a.meterings.Claim(request.Context(), principal, request.PathValue("batch_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) renewMeteringBatch(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
		Version    int64  `json:"version"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var batch domain.MeteringBatch
	if err == nil {
		batch, err = a.meterings.Renew(request.Context(), principal, request.PathValue("batch_id"), input.LeaseToken, requestIDFromContext(request.Context()), input.Version)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, batch)
}

func (a *API) recordMeteringItem(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
		Label      string `json:"label"`
		Payload    string `json:"payload"`
		Version    int64  `json:"version"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var item domain.MeteringItem
	if err == nil {
		item, err = a.meterings.Record(request.Context(), principal, request.PathValue("batch_id"), input.LeaseToken, request.PathValue("item_id"), input.Label, input.Payload, requestIDFromContext(request.Context()), input.Version)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (a *API) submitMeteringBatch(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		LeaseToken string `json:"lease_token"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var batch domain.MeteringBatch
	if err == nil {
		batch, err = a.meterings.Submit(request.Context(), principal, request.PathValue("batch_id"), input.LeaseToken, requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, batch)
}

func (a *API) reviewMeteringBatch(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		Accept bool   `json:"accept"`
		Reason string `json:"reason"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var batch domain.MeteringBatch
	if err == nil {
		batch, err = a.meterings.Review(request.Context(), principal, request.PathValue("batch_id"), requestIDFromContext(request.Context()), input.Accept, input.Reason)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, batch)
}
