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
