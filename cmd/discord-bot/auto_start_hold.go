package main

import (
	"context"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
)

func (s controlStreamStarter) StartManagedStream(streamID string, request control.AutoStartRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// One original request. A transport failure is not permission to issue a
	// second Start, use an empty body, or change the Attempt identifier.
	return s.client.StartManagedStream(ctx, streamID, request)
}

func (s controlStreamStarter) InspectManagedStream(streamID, requestID string) (control.ManagedStartInspection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.client.InspectManagedStream(ctx, streamID, requestID)
}
