//go:build linux && ui188_chain

package jobs

// UI188CredentialDiagnostic is test-only. It exposes no credential value.
func (m *Manager) UI188CredentialDiagnostic() (string, bool) {
	m.gatewayCredentialMu.Lock()
	defer m.gatewayCredentialMu.Unlock()
	return m.gatewayCredentialReference, m.gatewayCredentialToken != ""
}

func (m *Manager) UI188ProfileDiagnostic() map[string]GatewayProfileAuthority {
	m.gatewayCredentialMu.Lock()
	defer m.gatewayCredentialMu.Unlock()
	out := map[string]GatewayProfileAuthority{}
	for k, v := range m.gatewayProfileAuthority {
		out[k] = v
	}
	return out
}
