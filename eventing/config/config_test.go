package config

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

func TestSafeToken(t *testing.T) {
	cases := map[string]string{
		"":            "unknown",
		"   ":         "unknown",
		"hello.world": "hello_world",
		"a b c":       "a_b_c",
		". . .":       "_____", // FIXME: replace with a more sophisticated token sanitization
		"valid_token": "valid_token",
	}
	for input, want := range cases {
		got := SafeToken(input)
		assert.Equal(t, want, got, "safeToken(%q)", input)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "first", firstNonEmpty("", "first", "second"))
	assert.Equal(t, "second", firstNonEmpty("", "", "second"))
	assert.Empty(t, firstNonEmpty("", "", ""))
}

// Delay server startup to force initial connect failures.
func TestConnectRetriesUntilServerAvailable(t *testing.T) {
	lc := net.ListenConfig{}
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to pick a free port")
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("expected TCP address, got %T", l.Addr())
	}
	port := addr.Port
	_ = l.Close()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	cfg := Config{
		URL:        url,
		ClientName: "test-retry",
		Logger:     logger,
	}

	// Delay server startup to force initial connect failures
	go func() {
		time.Sleep(1 * time.Second)
		opts := &server.Options{JetStream: true, Port: port}
		srv, createServerErr := server.NewServer(opts)
		assert.NoError(t, createServerErr, "failed to create embedded NATS server")
		go srv.Start()
		assert.True(t, srv.ReadyForConnections(5*time.Second), "embedded server did not start in time")
	}()

	// Attempt to connect with retries
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn, js, err := Connect(ctx, cfg)
	require.NoError(t, err, "Connect should succeed after retries")
	assert.NotNil(t, conn, "nats.Conn should not be nil")
	assert.NotNil(t, js, "JetStream context should not be nil")

	// Cleanup
	_ = conn.Drain()
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected Config
		wantErr  bool
		desc     string
	}{
		{
			name:    "uses default values when no env vars set",
			envVars: map[string]string{},
			expected: Config{
				URL:               "nats://mdai-hub-nats.mdai.svc.cluster.local:4222",
				Subject:           "eventing",
				StreamName:        "EVENTS_STREAM",
				InactiveThreshold: 1 * time.Minute,
				NatsPassword:      "",
			},
			wantErr: false,
			desc:    "should use all default values",
		},
		{
			name: "uses environment variables when set",
			envVars: map[string]string{
				"NATS_URL":                "nats://custom-server:4222",
				"NATS_SUBJECT":            "custom-eventing",
				"NATS_STREAM_NAME":        "CUSTOM_STREAM",
				"NATS_QUEUE_NAME":         "custom-queue",
				"NATS_INACTIVE_THRESHOLD": "5m",
				"NATS_PASSWORD":           "secret123",
			},
			expected: Config{
				URL:               "nats://custom-server:4222",
				Subject:           "custom-eventing",
				StreamName:        "CUSTOM_STREAM",
				InactiveThreshold: 5 * time.Minute,
				NatsPassword:      "secret123",
			},
			wantErr: false,
			desc:    "should override defaults with env vars",
		},
		{
			name: "handles partial environment variables",
			envVars: map[string]string{
				"NATS_URL":      "nats://partial-server:4222",
				"NATS_PASSWORD": "partial-secret",
			},
			expected: Config{
				URL:               "nats://partial-server:4222",
				Subject:           "eventing",      // default
				StreamName:        "EVENTS_STREAM", // default
				InactiveThreshold: 1 * time.Minute, // default
				NatsPassword:      "partial-secret",
			},
			wantErr: false,
			desc:    "should mix env vars with defaults",
		},
		{
			name: "handles various duration formats",
			envVars: map[string]string{
				"NATS_INACTIVE_THRESHOLD": "30s",
			},
			expected: Config{
				URL:               "nats://mdai-hub-nats.mdai.svc.cluster.local:4222", // default
				Subject:           "eventing",                                         // default
				StreamName:        "EVENTS_STREAM",                                    // default
				InactiveThreshold: 30 * time.Second,
				NatsPassword:      "", // default
			},
			wantErr: false,
			desc:    "should parse duration correctly",
		},
		{
			name: "returns error for invalid duration",
			envVars: map[string]string{
				"NATS_INACTIVE_THRESHOLD": "invalid-duration",
			},
			expected: Config{},
			wantErr:  true,
			desc:     "should fail with invalid duration format",
		},
		{
			name: "handles empty string values",
			envVars: map[string]string{
				"NATS_URL":      "",
				"NATS_PASSWORD": "",
			},
			expected: Config{
				URL:               "",              // empty from env
				Subject:           "eventing",      // default
				StreamName:        "EVENTS_STREAM", // default
				InactiveThreshold: 1 * time.Minute, // default
				NatsPassword:      "",              // empty from env
			},
			wantErr: false,
			desc:    "should handle empty string env vars",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			cfg, err := LoadConfig()

			if tt.wantErr {
				assert.Error(t, err, tt.desc)
				return
			}

			require.NoError(t, err, tt.desc)

			assert.Equal(t, tt.expected.URL, cfg.URL, "URL mismatch")
			assert.Equal(t, tt.expected.Subject, cfg.Subject, "Subject mismatch")
			assert.Equal(t, tt.expected.StreamName, cfg.StreamName, "StreamName mismatch")
			assert.Equal(t, tt.expected.InactiveThreshold, cfg.InactiveThreshold, "InactiveThreshold mismatch")
			assert.Equal(t, tt.expected.NatsPassword, cfg.NatsPassword, "NatsPassword mismatch")

			assert.Empty(t, cfg.ClientName, "ClientName should be empty")
		})
	}
}

func TestGetMemberIDs(t *testing.T) {
	tests := []struct {
		name     string
		podName  string
		hostname string
		expected func(string) bool
		desc     string
	}{
		{
			name:     "uses POD_NAME when available",
			podName:  "my-pod-123",
			hostname: "ignored-hostname",
			expected: func(result string) bool { return result == "my-pod-123" },
			desc:     "should use POD_NAME over HOSTNAME",
		},
		{
			name:     "uses HOSTNAME when POD_NAME empty",
			podName:  "",
			hostname: "my-hostname",
			expected: func(result string) bool { return result == "my-hostname" },
			desc:     "should fallback to HOSTNAME",
		},
		{
			name:     "uses nuid when both empty",
			podName:  "",
			hostname: "",
			expected: func(result string) bool { return result != "" },
			desc:     "should fallback to nuid",
		},
		{
			name:     "sanitizes invalid characters",
			podName:  "pod@name#with$invalid%chars",
			hostname: "",
			expected: func(result string) bool { return result == "th_invalid_chars" && len(result) == 16 },
			desc:     "should replace invalid chars with underscores and truncate",
		},
		{
			name:     "truncates long names",
			podName:  "very-long-pod-name-that-exceeds-sixteen-characters",
			hostname: "",
			expected: func(result string) bool { return result == "xteen-characters" && len(result) == 16 },
			desc:     "should truncate to 16 chars from the end",
		},
		{
			name:     "preserves valid characters",
			podName:  "valid-pod_123/=",
			hostname: "",
			expected: func(result string) bool { return result == "valid-pod_123/=" },
			desc:     "should preserve A-Z, a-z, 0-9, -_/=",
		},
		{
			name:     "handles mixed case",
			podName:  "Pod-Type-ABC",
			hostname: "",
			expected: func(result string) bool { return result == "Pod-Type-ABC" },
			desc:     "should preserve case",
		},
		{
			name:     "handles exactly 16 chars",
			podName:  "exactly16chars12",
			hostname: "",
			expected: func(result string) bool { return result == "exactly16chars12" && len(result) == 16 },
			desc:     "should not truncate when exactly 16 chars",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.podName != "" {
				t.Setenv("POD_NAME", tt.podName)
			}
			if tt.hostname != "" {
				t.Setenv("HOSTNAME", tt.hostname)
			}

			result := GetMemberIDs()

			assert.True(t, tt.expected(result), "%s: got %q", tt.desc, result)
			assert.LessOrEqual(t, len(result), 16, "result should not exceed 16 characters: %q", result)
			assert.NotEmpty(t, result, "result should not be empty")
		})
	}
}

func TestWildcardString(t *testing.T) {
	tests := []struct {
		desc          string
		prefix        string
		subjectConfig mdaiSubjectConfig
		expected      string
		expectErr     bool
	}{
		{
			desc:   "two",
			prefix: "eventing",
			subjectConfig: mdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
			expected: "eventing.foobar.*.*",
		},
		{
			desc:   "five",
			prefix: "eventing",
			subjectConfig: mdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 5,
			},
			expected: "eventing.foobar.*.*.*.*.*",
		},
		{
			desc:      "no topic",
			expectErr: true,
			prefix:    "eventing",
			subjectConfig: mdaiSubjectConfig{
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
		},
		{
			desc:      "no cg",
			expectErr: true,
			prefix:    "eventing",
			subjectConfig: mdaiSubjectConfig{
				Topic:         "foobar",
				WildcardCount: 2,
			},
		},
		{
			desc:          "no nothing",
			expectErr:     true,
			prefix:        "eventing",
			subjectConfig: mdaiSubjectConfig{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual, err := tt.subjectConfig.getPrefixedWildcardString(tt.prefix)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual, "WildcardString mismatch")
			}
		})
	}
}

func TestGetWildcardAndSuffixedSubjects(t *testing.T) {
	tests := []struct {
		desc          string
		prefix        string
		suffixes      []string
		subjectConfig mdaiSubjectConfig
		expected      []string
	}{
		{
			desc:     "two",
			prefix:   "eventing",
			suffixes: []string{"dlq"},
			subjectConfig: mdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
			expected: []string{"eventing.foobar.*.*", "eventing.foobar.dlq"},
		},
		{
			desc:     "five",
			prefix:   "eventing",
			suffixes: []string{"dlq"},
			subjectConfig: mdaiSubjectConfig{
				Topic:         "asdf",
				ConsumerGroup: "asdf-consumer",
				WildcardCount: 5,
			},
			expected: []string{"eventing.asdf.*.*.*.*.*", "eventing.asdf.dlq"},
		},
		{
			desc:     "negative?",
			prefix:   "eventing",
			suffixes: []string{"dlq"},
			subjectConfig: mdaiSubjectConfig{
				Topic:         "asdf",
				ConsumerGroup: "asdf-consumer",
				WildcardCount: -1,
			},
			expected: []string{"eventing.asdf", "eventing.asdf.dlq"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual, err := tt.subjectConfig.getWildcardAndSuffixedSubjects(tt.prefix, tt.suffixes...)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual, "Suffix mismatch")
		})
	}
}

func TestGetWildcardIndicesSlice(t *testing.T) {
	tests := []struct {
		desc          string
		subjectConfig mdaiSubjectConfig
		expected      []int
	}{
		{
			desc: "two",
			subjectConfig: mdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
			expected: []int{1, 2},
		},
		{
			desc: "five",
			subjectConfig: mdaiSubjectConfig{
				Topic:         "asdf",
				ConsumerGroup: "asdf-consumer",
				WildcardCount: 5,
			},
			expected: []int{1, 2, 3, 4, 5},
		},
		{
			desc: "negative?",
			subjectConfig: mdaiSubjectConfig{
				Topic:         "asdf",
				ConsumerGroup: "asdf-consumer",
				WildcardCount: -1,
			},
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual := tt.subjectConfig.getWildcardIndices()
			assert.Equal(t, tt.expected, actual, "Bad wildcard indices")
		})
	}
}

func TestGetAllSubjectStringsWithAdditionalSuffixes(t *testing.T) {
	tests := []struct {
		desc     string
		prefix   string
		suffixes []string
		expected []string
	}{
		{
			desc:     "none",
			prefix:   "eventing",
			suffixes: []string{},
			expected: []string{
				"eventing.alert.*.*",
				"eventing.var.*.*",
				"eventing.trigger.vars.*.*.*",
			},
		},
		{
			desc:     "dlq",
			prefix:   "eventing",
			suffixes: []string{dlqSuffix},
			expected: []string{
				"eventing.alert.*.*",
				"eventing.alert.dlq",
				"eventing.var.*.*",
				"eventing.var.dlq",
				"eventing.trigger.vars.*.*.*",
				"eventing.trigger.vars.dlq",
			},
		},
		{
			desc:     "ni",
			prefix:   "ni",
			suffixes: []string{"ekke", "ekke", "ekke", "ekke", "ptang", "zoo", "boing"},
			expected: []string{
				"ni.alert.*.*",
				"ni.alert.ekke",
				"ni.alert.ekke",
				"ni.alert.ekke",
				"ni.alert.ekke",
				"ni.alert.ptang",
				"ni.alert.zoo",
				"ni.alert.boing",
				"ni.var.*.*",
				"ni.var.ekke",
				"ni.var.ekke",
				"ni.var.ekke",
				"ni.var.ekke",
				"ni.var.ptang",
				"ni.var.zoo",
				"ni.var.boing",
				"ni.trigger.vars.*.*.*",
				"ni.trigger.vars.ekke",
				"ni.trigger.vars.ekke",
				"ni.trigger.vars.ekke",
				"ni.trigger.vars.ekke",
				"ni.trigger.vars.ptang",
				"ni.trigger.vars.zoo",
				"ni.trigger.vars.boing",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual, err := everySubjectConfig.getAllSubjectStringsWithAdditionalSuffixes(tt.prefix, tt.suffixes...)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual, "Bad subject strings")
		})
	}
}

func runJetStream(t *testing.T) *server.Server {
	t.Helper()
	tempDir := t.TempDir()
	ns, err := server.NewServer(&server.Options{
		JetStream: true,
		StoreDir:  tempDir,
		Port:      -1, // pick a random free port
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	url := ns.ClientURL()
	t.Setenv("NATS_URL", url)

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns
}

func TestEnsureStream_CreatesNewStream(t *testing.T) {
	srv := runJetStream(t)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	logger, _ := zap.NewDevelopment()
	cfg := Config{
		StreamName: "TEST_STREAM",
		Subject:    "test.subject",
		Logger:     logger,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Pre-condition: ensure stream does not exist
	_, err = js.Stream(ctx, cfg.StreamName)
	require.Error(t, err)
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)

	// Act: call EnsureStream
	err = EnsureStream(ctx, js, cfg)
	require.NoError(t, err)

	// Assert: stream now exists
	stream, err := js.Stream(ctx, cfg.StreamName)
	require.NoError(t, err, "stream should exist after EnsureStream")
	require.NotNil(t, stream)

	// Assert: stream configuration is correct
	info, err := stream.Info(ctx)
	require.NoError(t, err)

	expectedSubjects, err := everySubjectConfig.getAllSubjectStringsWithAdditionalSuffixes(cfg.Subject, dlqSuffix)
	require.NoError(t, err)
	sort.Strings(expectedSubjects)
	sort.Strings(info.Config.Subjects)

	assert.Equal(t, cfg.StreamName, info.Config.Name)
	assert.Equal(t, expectedSubjects, info.Config.Subjects)
	assert.Equal(t, jetstream.FileStorage, info.Config.Storage)
	assert.Equal(t, jetstream.WorkQueuePolicy, info.Config.Retention)
	assert.Equal(t, defaultDuplicates, info.Config.Duplicates)
}

func TestEnsureStream_UpdateExistingStream(t *testing.T) {
	srv := runJetStream(t)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	logger, _ := zap.NewDevelopment()
	cfg := Config{
		StreamName: "TEST_UPDATE_STREAM",
		Subject:    "eventing",
		Logger:     logger,
	}

	// Pre-create a stream with a subset of subjects
	initialSubjects := []string{"eventing.alert.*.*"}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: initialSubjects,
	})
	require.NoError(t, err)

	// Act: run EnsureStream, which should detect the existing stream and add subjects
	err = EnsureStream(ctx, js, cfg)
	require.NoError(t, err, "EnsureStream should succeed when updating")

	// Assert: check that the stream was updated correctly
	stream, err := js.Stream(ctx, cfg.StreamName)
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)

	expectedSubjects, err := everySubjectConfig.getAllSubjectStringsWithAdditionalSuffixes(cfg.Subject, dlqSuffix)
	require.NoError(t, err)

	sort.Strings(info.Config.Subjects)
	sort.Strings(expectedSubjects)

	assert.Equal(t, expectedSubjects, info.Config.Subjects, "Stream subjects should be updated to the full desired set")
}

func TestEnsureStream_RefusesToDropSubjects(t *testing.T) {
	srv := runJetStream(t)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	cfg := Config{
		StreamName: "TEST_STREAM_DROP",
		Subject:    "eventing",
		Logger:     logger,
	}

	// 1. Create a stream with an extra subject that EnsureStream will not generate
	initialSubjects := []string{"eventing.some-old-subject"}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: initialSubjects,
	})
	require.NoError(t, err)

	// 2. Call EnsureStream, which will generate a different set of desired subjects
	err = EnsureStream(ctx, js, cfg)

	// 3. Assert that an error is returned because dropping a subject is not allowed
	require.Error(t, err, "EnsureStream should return an error when trying to remove subjects")
	assert.Contains(t, err.Error(), "desired subjects missing existing subject(s): eventing.some-old-subject")

	// 4. Verify the stream's subjects have not been changed
	stream, err := js.Stream(ctx, cfg.StreamName)
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, initialSubjects, info.Config.Subjects, "Stream subjects should not have been modified")
}

func TestEnsureStream_NoUpdateWhenSubjectsMatch(t *testing.T) {
	srv := runJetStream(t)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	cfg := Config{
		StreamName: "TEST_STREAM_NO_UPDATE",
		Subject:    "test.subject",
		Logger:     logger,
	}

	// Calculate the exact subjects that EnsureStream will expect
	desiredSubjects, err := everySubjectConfig.getAllSubjectStringsWithAdditionalSuffixes(cfg.Subject, dlqSuffix)
	require.NoError(t, err)

	// Pre-create the stream with the exact desired subjects
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: desiredSubjects,
	})
	require.NoError(t, err)

	// Act: Call EnsureStream on the already-correct stream
	err = EnsureStream(ctx, js, cfg)

	// Assert: No error should occur, and no update should have been attempted
	require.NoError(t, err, "EnsureStream should not return an error when subjects match")

	// Verify the stream's subjects are unchanged
	stream, err := js.Stream(ctx, cfg.StreamName)
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)

	sort.Strings(desiredSubjects)
	sort.Strings(info.Config.Subjects)
	assert.Equal(t, desiredSubjects, info.Config.Subjects, "Stream subjects should remain unchanged")
}

func TestEnsureElasticGroup_CreatesNewGroup(t *testing.T) {
	srv := runJetStream(t)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10000000*time.Second)
	defer cancel()

	// Create a stream for the consumer group to attach to
	streamName := "test-stream"
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"test.subject.*.*"},
	})
	require.NoError(t, err)

	logger, _ := zap.NewDevelopment()
	cfg := Config{Logger: logger}
	groupName := "test-group"
	pattern := "test.subject.*.*"
	hashWildcards := []int{1, 2}

	// Ensure the elastic group, which doesn't exist yet
	err = ensureElasticGroup(ctx, js, streamName, groupName, pattern, hashWildcards, cfg)
	require.NoError(t, err)

	// Verify the group was created by checking for its config
	ec, err := pcgroups.GetElasticConsumerGroupConfig(ctx, js, streamName, groupName)

	require.NoError(t, err, "should not fail to get config for a created group")
	require.NotNil(t, ec, "elastic consumer group config should not be nil")
	assert.Equal(t, pattern, ec.Filter, "filter pattern mismatch")
	assert.Equal(t, hashWildcards, ec.PartitioningWildcards, "hash wildcards mismatch")
	assert.Equal(t, int(ec.MaxMembers), maxPCGroupMembers, "partitions should match maxPCGroupMembers")
}
