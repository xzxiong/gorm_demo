package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/hints"
)

var logger Logger

func main() {
	testContextTimeout()
	//testNullText()
}

func testNullText() {

	ctx := context.Background()

	dbCfg := Config{
		Host:        "127.0.0.1",
		Port:        6001,
		Username:    "dump",
		Password:    "111",
		Database:    "mysql",
		PPV2Enabled: false,
		ClientIP:    "",
	}

	logger = NewLogger(zap.NewExample())

	db, err := connDBForUser(ctx, dbCfg, logger)
	if err != nil {
		logger.Error(ctx, "Create db connection failed for %s: %v", dbCfg.Username, err)
		return
	}

	type StmtInfo struct {
		Plan   string `gorm:"not null;type:text" json:"plan"`
		IsNull bool   `gorm:"type:bool" json:"is_null"`
	}
	var record StmtInfo
	cond := "id = 1"
	if err := db.Clauses(hints.CommentBefore("SELECT", "cloud_nonuser")).
		Table("test.stmt_info").
		Select("plan", "plan is NULL as is_null").
		Where(cond).
		Take(&record).Error; err != nil {
		logger.Error(ctx, "query failed: %v", err)
	}

	logger.Info(ctx, "plan: '%s', IsNULL: %v", record.Plan, record.IsNull)
}

func testContextTimeout() {

	ctx := context.Background()

	dbCfg := Config{
		Host:        "127.0.0.1",
		Port:        6001,
		Username:    "dump",
		Password:    "111",
		Database:    "mysql",
		PPV2Enabled: false,
		ClientIP:    "",
	}

	logger = NewLogger(NewExampleZapLogger())

	db, err := connDBForUser(ctx, dbCfg, logger)
	if err != nil {
		logger.Error(ctx, "Create db connection failed for %s: %v", dbCfg.Username, err)
		return
	}

	timeoCtx, timeoCancel := context.WithTimeoutCause(ctx, 3*time.Second, fmt.Errorf("client-timeout"))
	defer timeoCancel()
	logger.Info(ctx, "startup")
	db.WithContext(timeoCtx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("select sleep(10)").Error; err != nil {
			logger.Error(ctx, "exec sql: %v", err)
			return err
		}

		return nil
	})

	logger.Info(ctx, "Done.")
}

func testAccount() {

	ctx := context.Background()

	dbCfg := Config{
		Host: "127.0.0.1",
		Port: 6001,
		//Username:    "dump",
		//Password:    "111",
		Username:    "query_tae_table:admin:accountadmin",
		Password:    "123456",
		Database:    "mysql",
		PPV2Enabled: false,
		ClientIP:    "",
	}

	logger = NewLogger(zap.NewExample())

	db, err := connDBForUser(ctx, dbCfg, logger)
	if err != nil {
		logger.Error(ctx, "Create db connection failed for %s: %v", dbCfg.Username, err)
		return
	}

	type StatUnit struct {
		StatTS string  `gorm:"not null;type:varchar(32)" json:"stat_ts"`
		Type   string  `gorm:"not null;type:varchar(32)" json:"type,omitempty"`
		Value  float64 `gorm:"type:double" json:"value"`
	}
	record := make([]StatUnit, 0)

	sql := "/* cloud_nonuser */ USE system_metrics; /* cloud_nonuser */ /* QPS */ SELECT `stat_ts`, SUM(`value`)/300 as value FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 5 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(5 * floor(minute(date_add(`collecttime`,Interval 5 MINUTE)) / 5) as int),2,0),':00') AS stat_ts, sum(`value`) AS value, `node` FROM sql_statement_total WHERE `collecttime` >= '2024-03-25 18:40:16' AND `collecttime` <= '2024-03-25 19:20:16' GROUP BY `node`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 5 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(5 * floor(minute(date_add(`collecttime`,Interval 5 MINUTE)) / 5) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 5 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(5 * floor(minute(date_add(`collecttime`,Interval 5 MINUTE)) / 5) as int),2,0),':00') LIMIT 100000)t GROUP BY `stat_ts`"
	if err = db.Raw(sql).Scan(&record).Error; err != nil {
		logger.Error(ctx, "error", err)
		return
	}

	testToSqlUsage(ctx, db)

	logger.Warn(ctx, "====== Done =======")
	if conn, err := db.DB(); err != nil {
		logger.Error(ctx, "error", err)
	} else {
		logger.Info(ctx, "conn close")
		conn.Close()
	}
	//time.Sleep(time.Hour)
}

func testToSqlUsage(ctx context.Context, userDB *gorm.DB) {

	type request struct {
		CU *uint
	}
	const cuSQL = "IF(status = 'Running', NULL, CAST(IF(JSON_UNQUOTE(JSON_EXTRACT(stats, '$[0]')) >= 4, JSON_UNQUOTE(JSON_EXTRACT(stats, '$[8]')), mo_cu_v1(stats, duration)) AS DECIMAL(32,4))) AS `cu`"
	si := &StatementInfo{
		StatementId: "018eb819-4048-7e69-aaa6-feb99965eb97", // for step 2.
		Account:     "query_tae_table",
	}
	req := &request{}
	cuMin := uint(0)
	req.CU = &cuMin

	// step 1/2

	// cond, args, order, limit, offset := generateFilters(&req, accountID)
	logger.Warn(ctx, "==== testToSqlUsage: Step 1/2 =====")
	cond, args, order, limit, offset := "1=1 AND request_at between ? and ? ",
		[]any{"2024-03-25 18:40:16", "2024-03-25 19:20:16"}, "request_at desc",
		uint(20), uint(0)
	// joinCond, joinArgs := generateJoinFilters(&req, accountID, h.cfg.ResponseAtExtension)
	joinCond, joinArgs := "account = ? ", []any{"query_tae_table"}
	//proj, cu := h.generateProjection(&req, h.cfg.EnableStatementCU, needCU, h.cfg.EnableStatsCU)
	proj, cu :=
		"`statement`, system.statement_info.statement_id, IF(`status`='Running', TIMESTAMPDIFF(MICROSECOND,`request_at`,now())*1000, `duration`) AS `duration`, `status`, `request_at`, system.statement_info.response_at, `user`, system.statement_info.account, `database`, `transaction_id`, `session_id`, `rows_read`, `bytes_scan`, `error`, `err_code`, `result_count` ",
		false

	{
		proj += ", " + cuSQL
		cu = true
	}

	queryList, total, err := si.SelectStatements(userDB, proj, cond, args, order, limit, offset, NonUserRawComment, cu, req.CU, joinCond, joinArgs, false /*h.cfg.EnableStatementCU*/)
	logger.Info(ctx, "queryList: %s", queryList)
	logger.Info(ctx, "total: %d", total)
	logger.Info(ctx, "err: %v", err)

	// Step 2/2
	logger.Warn(ctx, "==== testToSqlUsage: Step 2/2 =====")
	//proj, cu := h.generateProjection(&DescribeQueryHistoryRequest{}, h.cfg.EnableStatementCU, needCU, h.cfg.EnableStatsCU)
	start, end := "2024-03-25 18:40:16", "2024-03-25 19:20:16"
	responseEnd := ""
	detail, err := si.SelectByStatementId(userDB, &proj, &start, &end, NonUserRawComment, cu, &responseEnd, false /*h.cfg.EnableStatementCU*/)
	logger.Info(ctx, "detail: %s", detail)
	logger.Info(ctx, "err: %v", err)
}
