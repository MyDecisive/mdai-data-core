package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	otelSdkDisabledEnvVar          = "OTEL_SDK_DISABLED"
	otelExporterOtlpEndpointEnvVar = "OTEL_EXPORTER_OTLP_ENDPOINT"
	logLevelEnvVar                 = "LOG_LEVEL"
)

// InitLogger initializes a logger with OTel and returns the internal and external logger instances.
// It also returns a cleanup function to close the logger resources.
// serviceName is the mdai service name of the service in the format "github.com/mydecisive/service-name"
// internalLogger is for logging to stdout only, while appLogger is for logging through OTEL SDK.
// Use LOG_LEVEL environment variable to change the log level.
// If OpenTelemetry setup fails, it logs a warning and appLogger logs to stdout only;
// use InitLoggerE to get the error instead.
func InitLogger(ctx context.Context, serviceName string) (internalLogger *zap.Logger, appLogger *zap.Logger, cleanup func()) {
	internalLogger, stdCore, logLevel := newInternalLogger()

	appLogger, cleanup, err := newAppLogger(ctx, serviceName, internalLogger, stdCore, logLevel)
	if err != nil {
		internalLogger.Warn("failed to set up OpenTelemetry; application logs will go to stdout only", zap.Error(err))
		appLogger = zap.New(stdCore, zap.AddCaller(), zap.IncreaseLevel(logLevel))
		cleanup = func() {
			_ = internalLogger.Sync()
			_ = appLogger.Sync()
		}
	}

	return internalLogger, appLogger, cleanup
}

// InitLoggerE is like InitLogger, but returns an error instead of falling back to stdout-only
// logging when OpenTelemetry setup fails. The loggers and cleanup function are nil on error.
func InitLoggerE(ctx context.Context, serviceName string) (internalLogger *zap.Logger, appLogger *zap.Logger, cleanup func(), err error) {
	internalLogger, stdCore, logLevel := newInternalLogger()

	appLogger, cleanup, err = newAppLogger(ctx, serviceName, internalLogger, stdCore, logLevel)
	if err != nil {
		_ = internalLogger.Sync()
		return nil, nil, nil, err
	}

	return internalLogger, appLogger, cleanup, nil
}

// newInternalLogger builds the stdout logger at the level set by LOG_LEVEL (default info).
func newInternalLogger() (*zap.Logger, zapcore.Core, zap.AtomicLevel) {
	// Define custom encoder configuration
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"                   // Rename the time field
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // Use human-readable timestamps
	encoderConfig.CallerKey = "caller"                    // Show caller file and line number

	// Parse LOG_LEVEL; default info.
	invalidLogLevel := false
	logLevel := zap.NewAtomicLevelAt(zap.InfoLevel)
	logLevelValue := strings.TrimSpace(os.Getenv(logLevelEnvVar))
	if logLevelValue != "" {
		var parsed zapcore.Level
		if err := parsed.UnmarshalText([]byte(strings.ToLower(logLevelValue))); err == nil {
			logLevel.SetLevel(parsed)
		} else {
			invalidLogLevel = true
		}
	}

	stdCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // JSON logging with readable timestamps
		zapcore.Lock(os.Stdout),               // Output to stdout
		zapcore.DebugLevel,                    // Log level will be updated later
	)
	internalLogger := zap.New(stdCore, zap.AddCaller(), zap.IncreaseLevel(logLevel))
	if invalidLogLevel {
		internalLogger.Warn("invalid log level provided, defaulting to info", zap.String("level", logLevelValue))
	}

	return internalLogger, stdCore, logLevel
}

// newAppLogger sets up OTel and returns a logger that writes to both stdout and the OTel SDK.
func newAppLogger(
	ctx context.Context,
	serviceName string,
	internalLogger *zap.Logger,
	stdCore zapcore.Core,
	logLevel zap.AtomicLevel,
) (*zap.Logger, func(), error) {
	otelShutdown, err := setupOTel(ctx, internalLogger)
	if err != nil {
		return nil, nil, fmt.Errorf("set up OpenTelemetry: %w", err)
	}

	otelCore := otelzap.NewCore(serviceName)
	multiCore := zapcore.NewTee(stdCore, otelCore)

	appLogger := zap.New(multiCore, zap.AddCaller(), zap.IncreaseLevel(logLevel))

	cleanup := func() {
		if otelShutdown != nil {
			_ = otelShutdown(context.Background())
		}
		_ = internalLogger.Sync()
		_ = appLogger.Sync()
	}

	return appLogger, cleanup, nil
}

type ZapErrorHandler struct {
	logger *zap.Logger
}

func (errorHandler ZapErrorHandler) Handle(err error) {
	if errorHandler.logger != nil {
		errorHandler.logger.Error(err.Error())
	}
}

type ShutdownFunc func(context.Context) error

// setupOTel configures OTEL SDK and validates the OTLP endpoint.
func setupOTel(ctx context.Context, internalLogger *zap.Logger) (ShutdownFunc, error) {
	if internalLogger == nil {
		return nil, errors.New("internal logger is required")
	}

	if v, _ := strconv.ParseBool(os.Getenv(otelSdkDisabledEnvVar)); v {
		internalLogger.Info("OTEL SDK is disabled")
		return nil, nil
	}

	otel.SetErrorHandler(&ZapErrorHandler{logger: internalLogger})

	shutdownFuncs := make([]ShutdownFunc, 0, 1)
	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown := func(ctx context.Context) error {
		var errs []error

		for _, fn := range shutdownFuncs {
			fnErr := fn(ctx)
			if fnErr != nil {
				errs = append(errs, fnErr)
			}
		}
		shutdownFuncs = nil
		return errors.Join(errs...)
	}

	resourceWAttributes, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("mdai-logstream", "hub"),
	))
	if err != nil {
		return nil, fmt.Errorf("create OTel resource: %w", err)
	}

	eventHandlerResource, err := resource.Merge(
		resource.Default(),
		resourceWAttributes,
	)
	if err != nil {
		return nil, fmt.Errorf("merge OTel resources: %w", err)
	}

	// Set up logger provider.
	loggerProvider, err := newLoggerProvider(ctx, eventHandlerResource)
	if err != nil {
		return nil, fmt.Errorf("create OTel logger provider: %w", err)
	}

	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	internalLogger.Info("OTEL SDK is enabled")
	if os.Getenv(otelExporterOtlpEndpointEnvVar) == "" {
		internalLogger.Warn("No OTLP endpoint is defined, but OTEL SDK is enabled.")
	}
	return shutdown, nil
}

func newLoggerProvider(ctx context.Context, res *resource.Resource) (*log.LoggerProvider, error) {
	logExporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(res),
	)

	return loggerProvider, nil
}
