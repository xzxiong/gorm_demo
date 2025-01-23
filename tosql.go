package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"gorm.io/gorm/callbacks"

	"gorm.io/gorm"
	"gorm.io/hints"
)

const (
	runningStatus        = "Running"
	statementInfoDBTable = "system.statement_info"
	execPlanCol          = "exec_plan"
	statsCol             = "stats"
	durationCol          = "duration"
	statementIDCol       = "statement_id"
	sqlSourceType        = "sql_source_type"
	responseAt           = "response_at"
	account              = "account"
	sysAccount           = "sys"
	obAccount            = "mocloud_mo_ob"
	anyStatsCol          = "any_value(`stats`) as `stats`"
	anyDurationCol       = "any_value(`duration`) as `duration`"
	anySqlSourceType     = "any_value(`sql_source_type`) as `sql_source_type`"
	anyResponseAt        = "any_value(`response_at`) as `response_at`"
	anyAccount           = "any_value(`account`) as `account`"
)

type StatementInfo struct {
	StatementId          string     `gorm:"primary_key;not null;type:varchar(36)" json:"statement_id,omitempty"`
	TransactionId        string     `gorm:"not null;type:varchar(36)" json:"transaction_id,omitempty"`
	SessionId            string     `gorm:"not null;type:varchar(36)" json:"session_id,omitempty"`
	Account              string     `gorm:"not null;type:varchar(1024)" json:"account,omitempty"`
	User                 string     `gorm:"not null;type:varchar(1024)" json:"user,omitempty"`
	Host                 string     `gorm:"not null;type:varchar(1024)" json:"host,omitempty"`
	Database             string     `gorm:"not null;type:varchar(1024)" json:"database,omitempty"`
	Statement            string     `gorm:"not null;type:text" json:"statement,omitempty"`
	StatementTag         string     `gorm:"type:text" json:"statement_tag,omitempty"`
	StatementFingerprint string     `gorm:"type:text" json:"statement_fingerprint,omitempty"`
	NodeUuid             string     `gorm:"not null;type:varchar(36)" json:"node_uuid,omitempty"`
	NodeType             string     `gorm:"not null;type:varchar(64)" json:"node_type,omitempty"`
	RequestAt            *time.Time `gorm:"not null;type:datetime" json:"request_at,omitempty"`
	ResponseAt           *time.Time `gorm:"not null;type:datetime" json:"response_at,omitempty"`
	Duration             uint64     `gorm:"not null;type:bigint unsigned" json:"duration,omitempty"`
	Status               string     `gorm:"type:varchar(32)" json:"status,omitempty"`
	ErrCode              string     `gorm:"type:varchar(1024)" json:"error_code,omitempty"`
	Error                string     `gorm:"not null;type:text" json:"error,omitempty"`
	ExecPlan             string     `gorm:"not null;type:json" json:"exec_plan,omitempty"`
	RowsRead             *uint64    `gorm:"not null;type:bigint unsigned" json:"rows_read,omitempty"`
	BytesScan            *uint64    `gorm:"not null;type:bigint unsigned" json:"bytes_scan,omitempty"`
	Stats                *Stats     `gorm:"not null;type:json" json:"stats,omitempty"`
	StatementType        string     `gorm:"not null;type:varchar(128)" json:"statement_type,omitempty"`
	QueryType            string     `gorm:"not null;type:varchar(128)" json:"query_type,omitempty"`
	RoleId               *uint64    `gorm:"not null;type:bigint unsigned" json:"role_id,omitempty"`
	SqlSourceType        string     `gorm:"type:text" json:"sql_source_type,omitempty"`
	ResultCount          int64      `gorm:"not null;type:bigint" json:"result_count,omitempty"`
	CU                   *float64   `json:"cu,omitempty"`

	Plan string `gorm:"not null;type:text" json:"plan"`
}

type ConnType float64

const (
	ConnTypeUnknown  ConnType = 0
	ConnTypeInternal ConnType = 1
	ConnTypeExternal ConnType = 2
)

type Stats struct {
	Version      float64
	TimeConsumed float64
	MemorySize   float64
	S3IOInput    float64
	S3IOOutput   float64
	NetworkIO    float64
	ConnType     ConnType
}

func (p StatementInfo) SelectByStatementId(db *gorm.DB,
	proj, start, end *string, sqlComment string, cu bool,
	responseEnd *string, enableStatementCU bool) (*StatementInfo, error) {
	if p.StatementId == "" || proj == nil {
		return nil, errors.New("Invalid Params")
	}

	cuCond := "statement_id = ? and account = ?"
	cond := "system.statement_info.statement_id = ? and system.statement_info.account = ?"
	joinArgs := []interface{}{p.StatementId, p.Account}
	args := []interface{}{p.StatementId, p.Account}
	if start != nil {
		cond = cond + " and request_at >= ?"
		cuCond = cuCond + " and response_at >= ?"
		args = append(args, *start)
		joinArgs = append(joinArgs, *start)
	}
	if end != nil {
		cond = cond + " and request_at <= ?"
		args = append(args, *end)
	}
	if responseEnd != nil {
		cuCond = cuCond + " and response_at <= ?"
		joinArgs = append(joinArgs, *responseEnd)
	}
	joinCond := fmt.Sprintf("left join (select * from mo_catalog.statement_cu where %s)tmpcu ON system.statement_info.statement_id = tmpcu.statement_id", cuCond)

	var record StatementInfo
	if cu && enableStatementCU {
		if err := db.Clauses(hints.CommentBefore("SELECT", sqlComment)).Table(statementInfoDBTable).Select(*proj).Joins(joinCond, joinArgs...).Where(cond, args...).Order("system.statement_info.response_at DESC").Take(&record).Error; err != nil {
			return nil, err
		}
	} else {
		if err := db.Clauses(hints.CommentBefore("SELECT", sqlComment)).Table(statementInfoDBTable).Select(*proj).Where(cond, args...).Order("system.statement_info.response_at DESC").Take(&record).Error; err != nil {
			return nil, err
		}
	}
	// add semicolon prefix
	if sql := strings.TrimSpace(record.Statement); len(sql) > 0 && !strings.HasSuffix(sql, ";") {
		record.Statement += ";"
	}
	return &record, nil
}

func (p StatementInfo) SelectStatements(db *gorm.DB,
	proj, cond string, args []any, order string,
	limit, offset uint, sqlComment string, cu bool,
	minCU *uint, joinCond string, joinArgs []any,
	enableStatementCU bool) ([]StatementInfo, int, error) {
	record := make([]StatementInfo, 0)
	var querySelect, queryCount string
	var count int64
	var eg errgroup.Group
	eg.Go(func() error {
		if cu {
			var tmpTableSQL string
			var allArgs []any
			if enableStatementCU {
				tmpTableSQL = fmt.Sprintf("(select %s from system.statement_info left join (select * from mo_catalog.statement_cu where %s)tmpcu ON system.statement_info.statement_id = tmpcu.statement_id where %s)t", proj, joinCond, cond)
				allArgs = append(joinArgs, args...)
			} else {
				tmpTableSQL = fmt.Sprintf("(select %s from system.statement_info where %s)t", proj, cond)
				allArgs = append(allArgs, args...)
			}
			tmpTable := db.Table(tmpTableSQL, allArgs...)
			if minCU != nil && *minCU > 0 {
				querySelect = tmpTable.ToSQL(func(tx *gorm.DB) *gorm.DB {
					return tx.Clauses(hints.CommentBefore("SELECT", sqlComment)).Where("cu > ?", *minCU).Order(order).Offset(int(offset)).Limit(int(limit)).Scan(&record)
				})
			} else {
				querySelect = tmpTable.ToSQL(func(tx *gorm.DB) *gorm.DB {
					//query := tx.Clauses(hints.CommentBefore("SELECT", sqlComment)).Order(order).Offset(int(offset)).Limit(int(limit))
					//if stmt := query.Statement; len(stmt.BuildClauses) == 0 {
					//	//stmt.BuildClauses = query
					//	//resetBuildClauses := true
					//}
					//callbacks.BuildQuerySQL(query)
					//logger.Info(context.TODO(), "query: %s", query.Statement.SQL.String())
					//return query
					return tx.Clauses(hints.CommentBefore("SELECT", sqlComment)).Order(order).Offset(int(offset)).Limit(int(limit)).Scan(&record)
				})
			}
		} else {
			querySelect = db.ToSQL(func(tx *gorm.DB) *gorm.DB {
				return tx.Clauses(hints.CommentBefore("SELECT", sqlComment)).Table(statementInfoDBTable).Select(proj).Where(cond, args...).Order(order).Offset(int(offset)).Limit(int(limit)).Scan(&record)
			})
		}
		return db.Raw(querySelect).Scan(&record).Error
	})

	eg.Go(func() error {
		var tmpTableSQL string
		var allArgs []any
		tmpTableSQL = fmt.Sprintf("(select %s from system.statement_info where %s)t", proj, cond)
		allArgs = append(allArgs, args...)
		//tmpTable := db.Table(tmpTableSQL, allArgs...)
		querySelect = db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			query := tx.Table(tmpTableSQL, allArgs...).
				Clauses(hints.CommentBefore("SELECT", sqlComment)).Order(order).Offset(int(offset)).Limit(int(limit))
			if stmt := query.Statement; len(stmt.BuildClauses) == 0 {
				//stmt.BuildClauses = query
				//resetBuildClauses := true
			}
			callbacks.BuildQuerySQL(query)
			logger.Info(context.TODO(), "query: %s", query.Statement.SQL.String())
			return query
		})
		return nil
	})

	// logger.Info(context.TODO(), "count: %d, queryCount: %s", count, queryCount)
	eg.Go(func() error {
		if cu {
			var tmpTableSQL string
			var allArgs []any
			if enableStatementCU {
				tmpTableSQL = fmt.Sprintf("(select %s from system.statement_info left join (select * from mo_catalog.statement_cu where %s)tmpcu ON system.statement_info.statement_id = tmpcu.statement_id where %s)t", proj, joinCond, cond)
				allArgs = append(joinArgs, args...)
			} else {
				tmpTableSQL = fmt.Sprintf("(select %s from system.statement_info where %s)t", proj, cond)
				allArgs = append(allArgs, args...)
			}
			tmpTable := db.Table(tmpTableSQL, allArgs...)
			if minCU != nil && *minCU > 0 {
				queryCount = tmpTable.ToSQL(func(tx *gorm.DB) *gorm.DB {
					return tx.Clauses(hints.CommentBefore("SELECT", sqlComment)).Where("cu > ?", *minCU).Count(&count)
				})
			}
		}
		if queryCount == "" {
			queryCount = db.ToSQL(func(tx *gorm.DB) *gorm.DB {
				return tx.Clauses(hints.CommentBefore("SELECT", sqlComment)).Table(statementInfoDBTable).Where(cond, args...).Count(&count)
			})
		}
		return db.Raw(queryCount).Count(&count).Error
	})

	if err := eg.Wait(); err != nil {
		return nil, -1, err
	}
	//return record, int(count), nil
	return record, 0, nil
}
