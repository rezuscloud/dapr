package options

import (
	"testing"
	"time"
)

func TestValidateLeaderElectionDurations(t *testing.T) {
	tests := []struct {
		name    string
		lease   time.Duration
		renew   time.Duration
		retry   time.Duration
		wantErr bool
	}{
		{
			name:    "valid durations",
			lease:   30 * time.Second,
			renew:   20 * time.Second,
			retry:   5 * time.Second,
			wantErr: false,
		},
		{
			name:    "lease must be positive",
			lease:   0,
			renew:   20 * time.Second,
			retry:   5 * time.Second,
			wantErr: true,
		},
		{
			name:    "renew must be positive",
			lease:   30 * time.Second,
			renew:   0,
			retry:   5 * time.Second,
			wantErr: true,
		},
		{
			name:    "retry must be positive",
			lease:   30 * time.Second,
			renew:   20 * time.Second,
			retry:   0,
			wantErr: true,
		},
		{
			name:    "lease must exceed renew",
			lease:   20 * time.Second,
			renew:   20 * time.Second,
			retry:   5 * time.Second,
			wantErr: true,
		},
		{
			name:    "renew must exceed retry",
			lease:   30 * time.Second,
			renew:   5 * time.Second,
			retry:   5 * time.Second,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLeaderElectionDurations(tt.lease, tt.renew, tt.retry)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateLeaderElectionDurations() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
