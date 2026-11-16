package httpapi

import (
	"net/http"
	"strconv"
	"strings"
)

// autoStartCondition is an authenticated observation of the original target.
// It never clears a hold, sends Start, changes a Revision, or retries work.
func (s Server) autoStartCondition(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	queries := r.URL.Query()
	revisionText := queries.Get("hold_revision")
	revision, err := strconv.ParseUint(revisionText, 10, 64)
	epoch := queries.Get("assignment_epoch")
	if err != nil || revision == 0 || revisionText != strconv.FormatUint(revision, 10) ||
		id == "" || epoch == "" || epoch != strings.TrimSpace(epoch) || len(epoch) > 1024 ||
		len(queries["hold_revision"]) != 1 || len(queries["assignment_epoch"]) != 1 || len(queries) != 2 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "explicit_auto_start_observation_required"})
		return
	}
	condition := s.manager.ObserveAutoStartCondition(id)
	if condition.HoldRevision != revision || condition.AssignmentEpoch != epoch {
		writeJSON(w, http.StatusConflict, map[string]string{"code": "auto_start_observation_stale"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, condition)
}

// autoStartPresence observes the current assignment's SDK state separately
// from CP-owned Hold/Resume persistence. It cannot release or start anything.
func (s Server) autoStartPresence(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	queries := r.URL.Query()
	epoch := queries.Get("assignment_epoch")
	if id == "" || epoch == "" || epoch != strings.TrimSpace(epoch) || len(epoch) > 1024 || len(queries["assignment_epoch"]) != 1 || len(queries) != 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "explicit_auto_start_observation_required"})
		return
	}
	condition := s.manager.ObserveAutoStartPresence(id)
	if condition.AssignmentEpoch != epoch {
		writeJSON(w, http.StatusConflict, map[string]string{"code": "auto_start_observation_stale"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, condition)
}
