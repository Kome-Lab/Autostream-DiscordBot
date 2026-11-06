package jobs

import "errors"

func (m *Manager) noteAutoStartCapacityResult(streamID string, retry *autoStartRetry, err error) {
	var classified staleAutoStartError
	capacityBusy := errors.As(err, &classified) && classified.HTTPStatusCode() == 409 && classified.ControlPanelCode() == "node_execution_busy"
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.autoStartRetries[streamID] == retry {
		retry.capacityBusy = capacityBusy
	}
}
