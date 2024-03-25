package main

import (
	"context"
	"go.uber.org/zap"
)

func main() {

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

	logger := NewLogger(zap.NewExample())

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
}
