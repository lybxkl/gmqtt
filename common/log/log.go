package log

import (
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Level zapcore.Level

const (
	DebugLevel Level = iota - 1
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel
)

func ToLevel(level string) Level {
	return map[string]Level{
		"debug": DebugLevel,
		"info":  InfoLevel,
		"warn":  WarnLevel,
		"error": ErrorLevel,
	}[level]
}

var (
	once sync.Once
	Log  Logger
)

type Logger interface {
	io.Closer

	Info(args ...interface{})
	Error(args ...interface{})
	Warn(args ...interface{})
	Debug(args ...interface{})

	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Debugf(template string, args ...interface{})
}

// NewGLog 获取日志
func NewGLog(level Level) Logger {
	once.Do(func() {
		encoderConfig := zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,                          // 小写编码器
			EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"), // ISO8601 UTC 时间格式
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder, // 短路径编码器
		}

		config := zap.Config{
			Level:         zap.NewAtomicLevelAt(zapcore.Level(level)), // 日志级别
			Development:   false,                                      // 开发模式，堆栈跟踪
			Encoding:      "console",                                  // 输出格式 console 或 json
			EncoderConfig: encoderConfig,                              // 编码器配置
			//InitialFields:    map[string]interface{}{"SaltIceMQTT": "SaltIce"}, // 初始化字段，如：添加一个服务器名称
			OutputPaths:      []string{"stdout"}, // 输出到指定文件 stdout（标准输出，正常颜色） stderr（错误输出，红色）
			ErrorOutputPaths: []string{"stderr"},
		}

		// 构建日志
		log, err := config.Build()
		if err != nil {
			panic(fmt.Errorf("log 初始化失败: %v", err))
		}
		Log = &_log{
			log.Sugar(),
		}
		Log.Info("log 初始化成功", zap.Time("runTime", time.Now()))
	})
	return Log
}

type _log struct {
	*zap.SugaredLogger
}

func (l *_log) Close() error {
	return nil
}
