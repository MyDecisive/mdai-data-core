package audit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestParseHumanDuration_NativeAndDays(t *testing.T) {
	cases := map[string]time.Duration{
		"100ms": 100 * time.Millisecond,
		"3s":    3 * time.Second,
		"2m":    2 * time.Minute,
		"1.5h":  90 * time.Minute,
		"72h":   72 * time.Hour,
		"1d":    24 * time.Hour,
		"2.5d":  60 * time.Hour,
	}

	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			got, err := parseHumanDuration(in)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestParseHumanDuration_RejectsDigitsOnlyAndWeeks(t *testing.T) {
	invalid := []string{"", " ", "1500", "2w", "10x", "-5m", "-1d"}
	for _, in := range invalid {
		t.Run(in, func(t *testing.T) {
			_, err := parseHumanDuration(in)
			require.Error(t, err)
		})
	}
}

func TestStreamRetentionFromEnv_Default(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
	logger := zap.NewNop()

	got, err := StreamRetentionFromEnv(logger)
	require.NoError(t, err)
	require.Equal(t, 30*24*time.Hour, got)
}

func TestStreamRetentionFromEnv_NewEnv_Days(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "30d")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
	logger := zap.NewNop()

	got, err := StreamRetentionFromEnv(logger)
	require.NoError(t, err)
	require.Equal(t, 30*24*time.Hour, got)
}

func TestStreamRetentionFromEnv_NewEnv_Native(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "72h")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
	logger := zap.NewNop()

	got, err := StreamRetentionFromEnv(logger)
	require.NoError(t, err)
	require.Equal(t, 72*time.Hour, got)
}

func TestStreamRetentionFromEnv_DeprecatedMs(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "2592000000") // 30d in ms
	logger := zap.NewNop()

	got, err := StreamRetentionFromEnv(logger)
	require.NoError(t, err)
	require.Equal(t, 30*24*time.Hour, got)
}

func TestStreamRetentionFromEnv_Preference_NewOverOld(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "48h")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "2592000000")
	logger := zap.NewNop()

	got, err := StreamRetentionFromEnv(logger)
	require.NoError(t, err)
	require.Equal(t, 48*time.Hour, got)
}

func TestStreamRetentionFromEnv_Invalid(t *testing.T) {
	cases := []struct {
		name, retention, expiryMs string
	}{
		{name: "unparseable retention", retention: "30 days"},
		{name: "negative retention", retention: "-1d"},
		{name: "unparseable deprecated ms", expiryMs: "30d"},
		{name: "negative deprecated ms", expiryMs: "-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", tc.retention)
			t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", tc.expiryMs)

			got, err := StreamRetentionFromEnv(zap.NewNop())
			require.ErrorIs(t, err, ErrInvalidRetention)
			assert.Zero(t, got)
		})
	}
}

func TestStreamRetentionFromEnv_NilLogger(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "72h")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")

	got, err := StreamRetentionFromEnv(nil)
	require.NoError(t, err)
	assert.Equal(t, 72*time.Hour, got)
}

func TestNewAuditAdapterE(t *testing.T) {
	t.Run("valid retention", func(t *testing.T) {
		t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "72h")
		t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")

		adapter, err := NewAuditAdapterE(zap.NewNop(), nil)
		require.NoError(t, err)
		assert.Equal(t, 72*time.Hour, adapter.valkeyAuditStreamExpiry)
	})

	t.Run("invalid retention", func(t *testing.T) {
		t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "30 days")
		t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")

		adapter, err := NewAuditAdapterE(zap.NewNop(), nil)
		require.ErrorIs(t, err, ErrInvalidRetention)
		assert.Nil(t, adapter)
	})
}

func TestNewAuditAdapter_InvalidRetentionFallsBackToDefault(t *testing.T) {
	t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "30 days")
	t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")

	adapter := NewAuditAdapter(zap.NewNop(), nil)
	require.NotNil(t, adapter)
	assert.Equal(t, 30*24*time.Hour, adapter.valkeyAuditStreamExpiry)
}

func TestCreateRestartEvent_ReservedKeysWin(t *testing.T) {
	adapter := &AuditAdapter{logger: zap.NewNop()}

	got := adapter.CreateRestartEvent("my-hub", map[string]string{
		"SERVICE_LIST_CSV": "a,b",
		"hub_name":         "other-hub",
		"type":             "something_else",
		"timestamp":        "not-a-timestamp",
	})

	assert.Equal(t, "my-hub", got["hub_name"])
	assert.Equal(t, CollectorRestart, got["type"])
	_, err := time.Parse(time.RFC3339, got["timestamp"])
	require.NoError(t, err, "timestamp should be set by CreateRestartEvent")
	assert.Equal(t, "a,b", got["SERVICE_LIST_CSV"], "other env entries are kept")
}

func TestHandleEventsGetN(t *testing.T) {
	t.Run("limits the read with COUNT", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := vmock.NewClient(ctrl)
		adapter := &AuditAdapter{logger: zap.NewNop(), valkeyClient: client}

		client.EXPECT().
			Do(gomock.Any(), vmock.Match("XREVRANGE", MdaiHubEventHistoryStreamName, "+", "-", "COUNT", "5")).
			Return(vmock.Result(vmock.ValkeyArray()))

		entries, err := adapter.HandleEventsGetN(t.Context(), 5)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("rejects a non-positive count", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adapter := &AuditAdapter{logger: zap.NewNop(), valkeyClient: vmock.NewClient(ctrl)}
		// No Do expectation: the client must not be called.

		for _, count := range []int64{0, -1} {
			_, err := adapter.HandleEventsGetN(t.Context(), count)
			require.Error(t, err)
		}
	})
}

func TestHandleEventsGet_ReadsWholeStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := vmock.NewClient(ctrl)
	adapter := &AuditAdapter{logger: zap.NewNop(), valkeyClient: client}

	client.EXPECT().
		Do(gomock.Any(), vmock.Match("XREVRANGE", MdaiHubEventHistoryStreamName, "+", "-")).
		Return(vmock.Result(vmock.ValkeyArray()))

	entries, err := adapter.HandleEventsGet(t.Context())
	require.NoError(t, err)
	assert.Empty(t, entries)
}
