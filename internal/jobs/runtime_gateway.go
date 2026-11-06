package jobs

import (
	"context"

	"github.com/example/autostream-discord-bot/internal/control"
)

// The HTTP start path calls this while holding RefreshRuntimeConfig across the
// fresh primary check, credential reconciliation, and JoinVoice.
func (m *Manager) SetRuntimeGatewayReconciler(reconcile func(context.Context, control.RuntimeConfig) error) {
	m.runtimeRefreshMu.Lock()
	defer m.runtimeRefreshMu.Unlock()
	m.runtimeGatewayReconciler = reconcile
}

func (m *Manager) ReconcileRuntimeGateway(ctx context.Context, cfg control.RuntimeConfig) error {
	if m.runtimeGatewayReconciler == nil {
		return nil
	}
	return m.runtimeGatewayReconciler(ctx, cfg)
}

// SetExecutionAuthorityStreams pins previously observed waiting epochs for
// actual primary assignments while the Panel's start transaction is in flight.
// These IDs never authorize Start; the HTTP primary check remains independent.
// Even an empty snapshot is recorded as observed, enabling bounded pruning;
// legacy callers that never supply one retain the last-nonempty behavior.
func (m *Manager) SetExecutionAuthorityStreams(streams map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executionAuthorityStreams = make(map[string]bool, len(streams))
	for id, allowed := range streams {
		if allowed {
			m.executionAuthorityStreams[id] = true
		}
	}
}
