package jobs

// GatewayProfileAuthority binds a previously verified waiting profile to the
// current primary assignment epoch. Active streams omit waiting VC mappings;
// that omission cannot select an unrelated profile from list order.
type GatewayProfileAuthority struct {
	Epoch     string
	ProfileID string
}

func (m *Manager) GatewayProfileForAssignments(current map[string]string, mapped map[string]string, mappingsPresent bool) (string, bool) {
	m.gatewayCredentialMu.Lock()
	defer m.gatewayCredentialMu.Unlock()
	if m.gatewayProfileAuthority == nil {
		m.gatewayProfileAuthority = make(map[string]GatewayProfileAuthority)
	}
	for stream, previous := range m.gatewayProfileAuthority {
		if epoch, ok := current[stream]; !ok || epoch != previous.Epoch {
			delete(m.gatewayProfileAuthority, stream)
		}
	}
	selected := ""
	resolved := make(map[string]string, len(current))
	for stream := range current {
		profile, explicit := mapped[stream]
		if explicit && profile != "" {
			resolved[stream] = profile
		} else if !mappingsPresent {
			profile = m.gatewayProfileAuthority[stream].ProfileID
		} else {
			profile = ""
		}
		if profile == "" || (selected != "" && selected != profile) {
			for id := range current {
				delete(m.gatewayProfileAuthority, id)
			}
			return "", false
		}
		selected = profile
	}
	if mappingsPresent {
		for stream, profile := range resolved {
			m.gatewayProfileAuthority[stream] = GatewayProfileAuthority{Epoch: current[stream], ProfileID: profile}
		}
	}
	return selected, selected != ""
}
