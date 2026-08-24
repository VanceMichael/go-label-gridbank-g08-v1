package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func (a *API) planWorkload(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input workload.PlanInput
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	input.RequestID = requestIDFromContext(request.Context())
	var result workload.PlanResult
	if err == nil {
		result, err = a.workloads.Plan(request.Context(), principal, input)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	status := http.StatusCreated
	if result.Replay {
		status = http.StatusOK
	}
	writeJSON(writer, status, result)
}

func (a *API) getWorkload(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var value domain.WorkloadSession
	if err == nil {
		value, err = a.workloads.Get(request.Context(), principal, request.PathValue("workload_id"))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) readyWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.MarkReady)
}

func (a *API) startWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.Start)
}

func (a *API) submitWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.Submit)
}

func (a *API) settleWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.Settle)
}

func (a *API) rejectWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.Fail)
}

func (a *API) cancelWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.Cancel)
}

func (a *API) reopenWorkload(writer http.ResponseWriter, request *http.Request) {
	a.workloadTransition(writer, request, a.workloads.Reopen)
}

func (a *API) workloadTransition(writer http.ResponseWriter, request *http.Request, operation func(context.Context, auth.Principal, string, string) (domain.WorkloadSession, error)) {
	principal, err := principalFromContext(request.Context())
	var value domain.WorkloadSession
	if err == nil {
		value, err = operation(request.Context(), principal, request.PathValue("workload_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) openStream(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		Kind domain.CapacityKind `json:"kind"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var value domain.CapacityStream
	if err == nil {
		value, err = a.capacitys.OpenStream(request.Context(), principal, request.PathValue("workload_id"), input.Kind, requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, value)
}

func (a *API) listStreams(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var values []domain.CapacityStream
	if err == nil {
		values, err = a.capacitys.List(request.Context(), principal, request.PathValue("workload_id"))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": values})
}

func (a *API) appendSegments(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		Segments []capacity.SegmentInput `json:"segments"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var result capacity.AppendResult
	if err == nil {
		result, err = a.capacitys.Append(request.Context(), principal, request.PathValue("stream_id"), requestIDFromContext(request.Context()), input.Segments)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) sealStream(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var value domain.CapacityStream
	if err == nil {
		value, err = a.capacitys.Seal(request.Context(), principal, request.PathValue("stream_id"), requestIDFromContext(request.Context()))
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (a *API) alignWorkload(writer http.ResponseWriter, request *http.Request) {
	principal, err := principalFromContext(request.Context())
	var input struct {
		ToleranceMillis int64 `json:"tolerance_millis"`
	}
	if err == nil {
		err = decodeJSON(writer, request, &input)
	}
	var values []domain.CapacityStream
	if err == nil {
		values, err = a.capacitys.AlignWorkload(request.Context(), principal, request.PathValue("workload_id"), requestIDFromContext(request.Context()), time.Duration(input.ToleranceMillis)*time.Millisecond)
	}
	if err != nil {
		writeError(a.logger, writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": values})
}
