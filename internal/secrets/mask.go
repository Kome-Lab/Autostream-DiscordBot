package secrets

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	httpURLPattern       = regexp.MustCompile(`https?://[^\s"'<>]+`)
	sensitiveWordPattern = regexp.MustCompile(`(?i)(authorization|bearer|token|secret|password|passwd|webhook|discord\.com/api/webhooks|hooks\.slack\.com/services)`)
)

func MaskURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	username := u.User.Username()
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(username, "****")
	}
	return u.String()
}

func SanitizeOperationalError(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.TrimSpace(fallback) == "" {
		fallback = "operation failed"
	}
	masked := httpURLPattern.ReplaceAllStringFunc(value, MaskURL)
	if sensitiveWordPattern.MatchString(masked) {
		return fallback
	}
	return masked
}
