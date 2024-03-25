

# Make

```
go build . -o cmd
```

# Run 
```
./cmd
```

# Expect

`rows` 为零

- log
```
{"level":"debug","msg":"trace","elapsed":"40.094708ms","rows":9,"sql":"/* cloud_nonuser */ USE system_metrics; /* cloud_nonuser */ /* QPS */ SELECT `stat_ts`, SUM(`value`)/300 as value FROM(SELECT concat(DATE_FORMAT(date_add(`collecttime`,Interval 5 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(5 * floor(minute(date_add(`collecttime`,Interval 5 MINUTE)) / 5) as int),2,0),':00') AS stat_ts, sum(`value`) AS value, `node` FROM sql_statement_total WHERE `collecttime` >= '2024-03-25 18:40:16' AND `collecttime` <= '2024-03-25 19:20:16' GROUP BY `node`, concat(DATE_FORMAT(date_add(`collecttime`,Interval 5 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(5 * floor(minute(date_add(`collecttime`,Interval 5 MINUTE)) / 5) as int),2,0),':00')  ORDER BY concat(DATE_FORMAT(date_add(`collecttime`,Interval 5 MINUTE),'%Y-%m-%d %H'),':',LPAD(CAST(5 * floor(minute(date_add(`collecttime`,Interval 5 MINUTE)) / 5) as int),2,0),':00') LIMIT 100000)t GROUP BY `stat_ts`"}
```
