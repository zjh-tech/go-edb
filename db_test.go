package edb

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"testing"
)

// func Test_Select(t *testing.T) {
// 	loopCount := 10000
// 	for i := 0; i < loopCount; i++ {
func Benchmark_Select(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		userName := fmt.Sprintf("Test%v", i)
		uid := Hash64(userName)
		tableName := GDBModule.GetTableNameByUID("account", uid)
		selectSql := BuildSelectSQL(tableName, []string{
			"accountid",
			"username",
			"password",
		}, map[string]interface{}{
			"username": userName,
		})

		recordSet, err := GDBModule.SyncQuerySqlOpt(selectSql, Hash64(userName))
		if err != nil {
			ELog.Infof("UserName=%v Select Error", userName)
			return
		}

		rc := recordSet.GetRecordSet()
		if len(rc) > 1 || len(rc) == 0 {
			ELog.Infof("UserName=%v Select RecordSet Len != 1", userName)
			return
		}

		accountid, _ := strconv.ParseUint(rc[0]["accountid"], 10, 64)
		username := rc[0]["username"]
		password := rc[0]["password"]
		ELog.Infof("AccountID=%v UserName=%v Password=%v", accountid, username, password)
	}
}

func Test_AsyncSelect(t *testing.T) {
	loopCount := 100000
	for i := 0; i < loopCount; i++ {
		GDBModule.AsyncDoSqlOpt(func(conn IMysqlConn, attach []interface{}) (IDBResult, error) {
			index := attach[0].(int)
			userName := fmt.Sprintf("Test%v", index)
			uid := Hash64(userName)
			tableName := GDBModule.GetTableNameByUID("account", uid)
			selectSql := BuildSelectSQL(tableName, []string{
				"accountid",
				"username",
				"password",
			}, map[string]interface{}{
				"username": userName,
			})

			recordSet, err := GDBModule.SyncQuerySqlOpt(selectSql, Hash64(userName))
			if err != nil {
				message := fmt.Sprintf("UserName=%v Select Error", userName)
				ELog.Infof(message)
				return nil, errors.New(message)
			}

			return recordSet, nil

		}, func(recordSet IDBResult, attach []interface{}, err error) {
			index := attach[0].(int)
			userName := fmt.Sprintf("Test%v", index)
			rc := recordSet.GetRecordSet()
			if len(rc) > 1 || len(rc) == 0 {
				ELog.Infof("UserName=%v Select RecordSet Len != 1", userName)
				return
			}

			accountid, _ := strconv.ParseUint(rc[0]["accountid"], 10, 64)
			username := rc[0]["username"]
			password := rc[0]["password"]
			ELog.Infof("AccountID=%v UserName=%v Password=%v", accountid, username, password)
		}, []interface{}{i}, uint64(i))
	}
}

func Test_InsertOrUpdate(t *testing.T) {
	loopCount := 10000
	for i := 0; i < loopCount; i++ {

		// func Benchmark_InsertOrUpdate(b *testing.B) {
		// 	b.ReportAllocs()
		// 	for i := 0; i < b.N; i++ {
		userName := fmt.Sprintf("Test%v", i)
		rdmValue := rand.Intn(1000000000)
		password := strconv.Itoa(rdmValue)

		uid := Hash64(userName)
		tableName := GDBModule.GetTableNameByUID("account", uid)
		sql := BuildInsertOrUpdateSQL(tableName, map[string]interface{}{
			"accountid": i,
			"username":  userName,
			"password":  password,
		}, []string{
			"accountid",
			"username",
		})
		_, err := GDBModule.SyncNonQuerySqlOpt(sql, Hash64(userName))
		if err != nil {
			ELog.Infof("UserName=%v InsertOrUpdate Error", userName)
			return
		}
	}
}

//----------------------------------------
func Hash(str string) uint32 {
	hash := fnv.New32()
	hash.Write([]byte(str))
	return hash.Sum32()
}

func Hash64(str string) uint64 {
	hash := fnv.New64()
	hash.Write([]byte(str))
	return hash.Sum64()
}

type Log struct {
}

func (l *Log) Debug(v ...interface{}) {
	//fmt.Println(v...)
}
func (l *Log) Debugf(format string, v ...interface{}) {
	//fmt.Printf(format, v...)
}
func (l *Log) Info(v ...interface{}) {
	//fmt.Println(v...)
}
func (l *Log) Infof(format string, v ...interface{}) {
	//fmt.Printf(format, v...)
}

func (l *Log) Warn(v ...interface{}) {
	//fmt.Println(v...)
}
func (l *Log) Warnf(format string, v ...interface{}) {
	//fmt.Printf(format, v...)
}

func (l *Log) Error(v ...interface{}) {
	//fmt.Println(v...)
}
func (l *Log) Errorf(format string, v ...interface{}) {
	//fmt.Printf(format, v...)
}

func init() {
	ELog = &Log{}

	connMaxCount := uint64(1)
	tableMaxCount := uint64(1)
	dbInfoList := make([]*DBConnSpec, 0)
	sepc := &DBConnSpec{
		Name:     "testdb_0",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "123456",
		Charset:  "utf8",
	}
	dbInfoList = append(dbInfoList, sepc)
	if err := GDBModule.Init(connMaxCount, tableMaxCount, dbInfoList); err != nil {
		ELog.Info(err)
	}

	// go func() {
	// 	busy := false
	// 	for {
	// 		busy = false
	// 		if GDBModule.Run(100) {
	// 			busy = true
	// 		}

	// 		if !busy {
	// 			time.Sleep(1 * time.Millisecond)
	// 		}
	// 	}
	// }()

	//create database testdb_0;
	//use testdb_0
	// CREATE TABLE `account_00` (
	// `accountid` bigint(20) unsigned COMMENT '账号ID',
	// `username` varchar(128) NOT NULL DEFAULT '' COMMENT '账号',
	// `password` varchar(128) NOT NULL DEFAULT '' COMMENT '密码',
	// PRIMARY KEY (`accountid`),
	// KEY (`username`)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
}
