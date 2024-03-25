package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm/schema"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/go-sql-driver/mysql"
	"github.com/pires/go-proxyproto"
	gmysql "gorm.io/driver/mysql"

	gormlogger "gorm.io/gorm/logger"
)

type Config struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database" yaml:"database"`
	// PPV2Enabled indicates whether to add
	// ppv2 header when connecting to DB
	PPV2Enabled bool `json:"ppv2Enabled" yaml:"ppv2Enabled"`
	// ClientIP is the client source IP
	ClientIP string `json:"clientIP" yaml:"clientIP"`
}

func connDBForUser(ctx context.Context, cfg Config, log logger.Interface) (*gorm.DB, error) {
	// conn for user
	var (
		user *gorm.DB
	)
	var eg errgroup.Group
	// conn for user
	eg.Go(func() error {
		conn, err := OpenDB(cfg, log)
		if err != nil {
			return err
		}
		cp, err := conn.DB()
		if err != nil {
			return err
		}
		cp.SetMaxOpenConns(1)
		cp.SetConnMaxLifetime(time.Hour * 2)

		user = conn
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return user, nil
}

// OpenDB initializes db connection
func OpenDB(cfg Config, logger logger.Interface) (*gorm.DB, error) {
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = cfg.Username
	mysqlCfg.Passwd = cfg.Password
	mysqlCfg.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	mysqlCfg.DBName = cfg.Database
	mysqlCfg.AllowNativePasswords = true
	mysqlCfg.ParseTime = true
	mysqlCfg.Timeout = time.Second * 30

	connector, err := mysql.NewConnector(mysqlCfg)
	if err != nil {
		return nil, err
	}

	if cfg.PPV2Enabled {
		mysql.RegisterDialContext("tcp", cfg.Dial)
	}

	return gorm.Open(gmysql.New(gmysql.Config{
		Conn: sql.OpenDB(connector),
	}), &gorm.Config{
		SkipDefaultTransaction: false,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		Logger:                                   logger,
		DisableAutomaticPing:                     false,
		DisableForeignKeyConstraintWhenMigrating: true,
	})
}

func (c *Config) Dial(ctx context.Context, addr string) (net.Conn, error) {
	nd := net.Dialer{Timeout: 10 * time.Second}
	conn, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, err
	}
	if c.ClientIP == "" {
		return nil, fmt.Errorf("invalid client IP")
	}
	// Create a proxyprotocol header
	header := &proxyproto.Header{
		Version:           2,
		Command:           proxyproto.PROXY,
		TransportProtocol: proxyproto.TCPv4,
		SourceAddr: &net.TCPAddr{
			IP: net.ParseIP(c.ClientIP),
		},
		// dummy dest addr
		DestinationAddr: &net.TCPAddr{
			IP: net.ParseIP("127.0.0.1"),
		},
	}
	// After the connection was created write the proxy headers first
	_, err = header.WriteTo(conn)
	if err != nil {
		return nil, err
	}
	return conn, err
}

type Logger struct {
	ZapLogger                 *zap.Logger
	LogLevel                  gormlogger.LogLevel
	SlowThreshold             time.Duration
	SkipCallerLookup          bool
	IgnoreRecordNotFoundError bool
}

func NewLogger(zapLogger *zap.Logger) Logger {
	return Logger{
		ZapLogger:                 zapLogger,
		LogLevel:                  logLevelAdapter(zapLogger),
		SlowThreshold:             100 * time.Millisecond,
		SkipCallerLookup:          false,
		IgnoreRecordNotFoundError: false,
	}
}

func logLevelAdapter(logger *zap.Logger) gormlogger.LogLevel {
	if logger.Core().Enabled(zapcore.DebugLevel) {
		return gormlogger.Info
	}
	if logger.Core().Enabled(zapcore.InfoLevel) {
		return gormlogger.Warn
	}
	if logger.Core().Enabled(zapcore.ErrorLevel) {
		return gormlogger.Error
	}
	return gormlogger.Silent

}

func (l Logger) SetAsDefault() {
	gormlogger.Default = l
}

func (l Logger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	return Logger{
		ZapLogger:                 l.ZapLogger,
		SlowThreshold:             l.SlowThreshold,
		LogLevel:                  level,
		SkipCallerLookup:          l.SkipCallerLookup,
		IgnoreRecordNotFoundError: l.IgnoreRecordNotFoundError,
	}
}

func (l Logger) Info(ctx context.Context, str string, args ...interface{}) {
	if l.LogLevel < gormlogger.Info {
		return
	}
	l.logger().Sugar().Debugf(str, args...)
}

func (l Logger) Warn(ctx context.Context, str string, args ...interface{}) {
	if l.LogLevel < gormlogger.Warn {
		return
	}
	l.logger().Sugar().Warnf(str, args...)
}

func (l Logger) Error(ctx context.Context, str string, args ...interface{}) {
	if l.LogLevel < gormlogger.Error {
		return
	}
	l.logger().Sugar().Errorf(str, args...)
}

func (l Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= 0 {
		return
	}
	elapsed := time.Since(begin)
	switch {
	case err != nil && l.LogLevel >= gormlogger.Error && (!l.IgnoreRecordNotFoundError || !errors.Is(err, gorm.ErrRecordNotFound)):
		sql, rows := fc()
		l.logger().Error("trace", zap.Error(err), zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	case l.SlowThreshold != 0 && elapsed > l.SlowThreshold && l.LogLevel >= gormlogger.Warn:
		sql, rows := fc()
		l.logger().Warn("trace", zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	case l.LogLevel >= gormlogger.Info:
		sql, rows := fc()
		l.logger().Debug("trace", zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	}
}

var (
	gormPackage    = filepath.Join("gorm.io", "gorm")
	zapgormPackage = filepath.Join("moul.io", "zapgorm")
)

func (l Logger) logger() *zap.Logger {
	for i := 2; i < 15; i++ {
		_, file, _, ok := runtime.Caller(i)
		switch {
		case !ok:
		case strings.HasSuffix(file, "_test.go"):
		case strings.Contains(file, gormPackage):
		case strings.Contains(file, zapgormPackage):
		default:
			return l.ZapLogger.WithOptions(zap.AddCallerSkip(i))
		}
	}
	return l.ZapLogger
}
