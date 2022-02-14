package core

import (
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/maybgit/glog"
	"github.com/ppkg/kit"
)

// offsetIntLogger is the stack frame offset in the call stack for the caller to
// one of the Warn,Info,Log,etc methods.
const offsetIntLogger = 3

type logger struct {
	name         string
	level        int32
	callerOffset int
	implied      []interface{}
	timeFormat   string
	disableTime  bool
}

func NewLogger(opts *hclog.LoggerOptions) *logger {

	if opts == nil {
		opts = &hclog.LoggerOptions{}
	}

	level := opts.Level
	if level == hclog.NoLevel {
		level = hclog.DefaultLevel
	}

	l := &logger{
		name:        opts.Name,
		timeFormat:  hclog.TimeFormat,
		disableTime: opts.DisableTime,
		level:       int32(opts.Level),
	}
	if opts.IncludeLocation {
		l.callerOffset = offsetIntLogger + opts.AdditionalLocationOffset
	}

	if opts.TimeFormat != "" {
		l.timeFormat = opts.TimeFormat
	}

	atomic.StoreInt32(&l.level, int32(level))

	return l
}

// JSON logging function
func (s *logger) logJSON(msg string, args ...interface{}) string {
	vals := s.jsonMapEntry(time.Now(), s.name, hclog.Level(s.level), msg)
	args = append(s.implied, args...)

	if len(args) > 0 {
		if len(args)%2 != 0 {
			cs, ok := args[len(args)-1].(hclog.CapturedStacktrace)
			if ok {
				args = args[:len(args)-1]
				vals["stacktrace"] = cs
			} else {
				extra := args[len(args)-1]
				args = append(args[:len(args)-1], hclog.MissingKey, extra)
			}
		}

		for i := 0; i < len(args); i = i + 2 {
			val := args[i+1]
			switch sv := val.(type) {
			case error:
				// Check if val is of type error. If error type doesn't
				// implement json.Marshaler or encoding.TextMarshaler
				// then set val to err.Error() so that it gets marshaled
				switch sv.(type) {
				case json.Marshaler, encoding.TextMarshaler:
				default:
					val = sv.Error()
				}
			case hclog.Format:
				val = fmt.Sprintf(sv[0].(string), sv[1:]...)
			}

			var key string

			switch st := args[i].(type) {
			case string:
				key = st
			default:
				key = fmt.Sprintf("%s", st)
			}
			vals[key] = val
		}
	}

	return kit.JsonEncode(vals)
}

func (s *logger) jsonMapEntry(t time.Time, name string, level hclog.Level, msg string) map[string]interface{} {
	vals := map[string]interface{}{
		"@message": msg,
	}
	if !s.disableTime {
		vals["@timestamp"] = t.Format(s.timeFormat)
	}

	var levelStr string
	switch level {
	case hclog.Error:
		levelStr = "error"
	case hclog.Warn:
		levelStr = "warn"
	case hclog.Info:
		levelStr = "info"
	case hclog.Debug:
		levelStr = "debug"
	case hclog.Trace:
		levelStr = "trace"
	default:
		levelStr = "all"
	}

	vals["@level"] = levelStr

	if name != "" {
		vals["@module"] = name
	}

	if s.callerOffset > 0 {
		if _, file, line, ok := runtime.Caller(s.callerOffset + 1); ok {
			vals["@caller"] = fmt.Sprintf("%s:%d", file, line)
		}
	}
	return vals
}

// Args are alternating key, val pairs
// keys must be strings
// vals can be any type, but display is implementation specific
// Emit a message and key/value pairs at a provided log level
func (s *logger) Log(level hclog.Level, msg string, args ...interface{}) {
	var fn func(format string, args ...interface{})
	switch level {
	case hclog.Error:
		fn = glog.Errorf
	case hclog.Warn:
		fn = glog.Warningf
	case hclog.Info:
		fn = glog.Infof
	case hclog.Debug:
		fn = glog.Infof
	case hclog.Trace:
		fn = glog.Infof
	default:
		fn = glog.Errorf
	}

	fn(s.logJSON(msg, args...))
}

// Emit a message and key/value pairs at the TRACE level
func (s *logger) Trace(msg string, args ...interface{}) {
	s.Log(hclog.Trace, msg, args...)
}

// Emit a message and key/value pairs at the DEBUG level
func (s *logger) Debug(msg string, args ...interface{}) {
	s.Log(hclog.Debug, msg, args...)
}

// Emit a message and key/value pairs at the INFO level
func (s *logger) Info(msg string, args ...interface{}) {
	s.Log(hclog.Info, msg, args...)
}

// Emit a message and key/value pairs at the WARN level
func (s *logger) Warn(msg string, args ...interface{}) {
	s.Log(hclog.Warn, msg, args...)
}

// Emit a message and key/value pairs at the ERROR level
func (s *logger) Error(msg string, args ...interface{}) {
	s.Log(hclog.Error, msg, args...)
}

// Indicate if TRACE logs would be emitted. This and the other Is* guards
// are used to elide expensive logging code based on the current level.
func (s *logger) IsTrace() bool {
	return s.level == int32(hclog.Trace)
}

// Indicate if DEBUG logs would be emitted. This and the other Is* guards
func (s *logger) IsDebug() bool {
	return s.level == int32(hclog.Debug)
}

// Indicate if INFO logs would be emitted. This and the other Is* guards
func (s *logger) IsInfo() bool {
	return s.level == int32(hclog.Info)
}

// Indicate if WARN logs would be emitted. This and the other Is* guards
func (s *logger) IsWarn() bool {
	return s.level == int32(hclog.Warn)
}

// Indicate if ERROR logs would be emitted. This and the other Is* guards
func (s *logger) IsError() bool {
	return s.level == int32(hclog.Error)
}

// ImpliedArgs returns With key/value pairs
func (s *logger) ImpliedArgs() []interface{} {
	return s.implied
}

// Creates a sublogger that will always have the given key/value pairs
func (s *logger) With(args ...interface{}) hclog.Logger {
	var extra interface{}

	if len(args)%2 != 0 {
		extra = args[len(args)-1]
		args = args[:len(args)-1]
	}

	sl := s.copy()

	result := make(map[string]interface{}, len(s.implied)+len(args))
	keys := make([]string, 0, len(s.implied)+len(args))

	// Read existing args, store map and key for consistent sorting
	for i := 0; i < len(s.implied); i += 2 {
		key := s.implied[i].(string)
		keys = append(keys, key)
		result[key] = s.implied[i+1]
	}
	// Read new args, store map and key for consistent sorting
	for i := 0; i < len(args); i += 2 {
		key := args[i].(string)
		_, exists := result[key]
		if !exists {
			keys = append(keys, key)
		}
		result[key] = args[i+1]
	}

	// Sort keys to be consistent
	sort.Strings(keys)

	sl.implied = make([]interface{}, 0, len(s.implied)+len(args))
	for _, k := range keys {
		sl.implied = append(sl.implied, k)
		sl.implied = append(sl.implied, result[k])
	}

	if extra != nil {
		sl.implied = append(sl.implied, hclog.MissingKey, extra)
	}

	return sl
}

// Returns the Name of the logger
func (s *logger) Name() string {
	return s.name
}

// Create a logger that will prepend the name string on the front of all messages.
// If the logger already has a name, the new value will be appended to the current
// name. That way, a major subsystem can use this to decorate all it's own logs
// without losing context.
func (s *logger) Named(name string) hclog.Logger {
	sl := s.copy()

	if sl.name != "" {
		sl.name = sl.name + "." + name
	} else {
		sl.name = name
	}

	return sl
}

// Create a logger that will prepend the name string on the front of all messages.
// This sets the name of the logger to the value directly, unlike Named which honor
// the current name as well.
func (s *logger) ResetNamed(name string) hclog.Logger {
	sl := s.copy()

	sl.name = name

	return sl
}

// Updates the level. This should affect all related loggers as well,
// unless they were created with IndependentLevels. If an
// implementation cannot update the level on the fly, it should no-op.
func (s *logger) SetLevel(level hclog.Level) {
	s.level = int32(level)
}

// Return a value that conforms to the stdlib log.Logger interface
func (s *logger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return nil
}

// Return a value that conforms to io.Writer, which can be passed into log.SetOutput()
func (s *logger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return nil
}

func (l *logger) copy() *logger {
	sl := *l
	sl.level = l.level
	return &sl
}
