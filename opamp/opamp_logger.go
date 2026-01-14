package opamp

import (
	"context"
	"github.com/open-telemetry/opamp-go/client/types"
	"go.uber.org/zap"
)

var _ types.Logger = &Logger{}

type Logger struct {
	l *zap.SugaredLogger
}

// Debugf implements types.Logger.
func (o *Logger) Debugf(_ context.Context, format string, v ...any) {
	o.l.Debugf(format, v...)
}

// Errorf implements types.Logger.
func (o *Logger) Errorf(_ context.Context, format string, v ...any) {
	o.l.Errorf(format, v...)
}

func NewLoggerFromZap(l *zap.Logger, name string) types.Logger {
	return &Logger{
		l: l.Sugar().Named(name).WithOptions(zap.AddCallerSkip(1)),
	}
}
