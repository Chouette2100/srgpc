package ShowroomDBlib

import (
	//	"fmt"
	"io/ioutil"
	"os"
	"time"

	"log"

	"gopkg.in/yaml.v2"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

/*

	20A00	結果をDBで保存する。Excel保存の機能は残存。次に向けての作り込み少々。
	2.0B00		データ取得のタイミングをtimetableから得る。Excelへのデータの保存をやめる。
	2.0B01	timetableの更新で処理が終わっていないものを処理済みにしていた問題を修正する。

*/

const Version = "20B01"

type EventRank struct {
	Order       int
	Rank        int
	Listner     string
	Lastname    string
	LsnID       int
	T_LsnID     int
	Point       int
	Incremental int
	Status      int
}

// 構造体のスライス
type EventRanking []EventRank

//	sort.Sort()のための関数三つ
func (e EventRanking) Len() int {
	return len(e)
}

func (e EventRanking) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

//	降順に並べる
func (e EventRanking) Less(i, j int) bool {
	//	return e[i].point < e[j].point
	return e[i].Point > e[j].Point
}

type DBConfig struct {
	WebServer string `yaml:"WebServer"`
	HTTPport  string `yaml:"HTTPport"`
	SSLcrt    string `yaml:"SSLcrt"`
	SSLkey    string `yaml:"SSLkey"`
	Dbhost    string `yaml:"Dbhost"`
	Dbname    string `yaml:"Dbname"`
	Dbuser    string `yaml:"Dbuser"`
	Dbpw      string `yaml:"Dbpw"`
}

var Db *sql.DB
var Err error

// 設定ファイルを読み込む
//      以下の記事を参考にさせていただきました。
//              【Go初学】設定ファイル、環境変数から設定情報を取得する
//                      https://note.com/artefactnote/n/n8c22d1ac4b86
//
func LoadConfig(filePath string) (dbconfig *DBConfig, err error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	content = []byte(os.ExpandEnv(string(content)))

	result := &DBConfig{}
	if err := yaml.Unmarshal(content, result); err != nil {
		return nil, err
	}

	return result, nil
}

func OpenDb(dbconfig *DBConfig) (status int) {

	status = 0

	if dbconfig.Dbhost == "" {
		Db, Err = sql.Open("mysql", (*dbconfig).Dbuser+":"+(*dbconfig).Dbpw+"@/"+(*dbconfig).Dbname+"?parseTime=true&loc=Asia%2FTokyo")
	} else {
		Db, Err = sql.Open("mysql", (*dbconfig).Dbuser+":"+(*dbconfig).Dbpw+"@tcp("+(*dbconfig).Dbhost+":3306)/"+(*dbconfig).Dbname+"?parseTime=true&loc=Asia%2FTokyo")
	}

	if Err != nil {
		status = -1
	}
	return
}

func InsertIntoEventrank(
	eventid	string,
	userno	int,
	sampletm2	time.Time,
	eventranking EventRanking,
) (
	status int,
) {

	status = 0

	ts := time.Now().Truncate(time.Minute)

	var row *sql.Stmt
	sql := "INSERT INTO eventrank(eventid, userid, ts, listner, lastname, lsnid, t_lsnid, norder, nrank, point, increment, status)"
	sql += " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
	row, Err = Db.Prepare(sql)
	if Err != nil {
		log.Printf("InsertIntoPoints() prepare() err=[%s]\n", Err.Error())
		status = -1
	}
	defer row.Close()

	for _, evr := range eventranking {

		_, Err = row.Exec(eventid, userno, ts, evr.Listner, evr.Lastname, evr.LsnID, evr.T_LsnID, evr.Order, evr.Rank, evr.Point, evr.Incremental, 0)

		if Err != nil {
			log.Printf("InsertIntoEventrank() exec() err=[%s]\n", Err.Error())
			status = -1
		}
	}

	return
}

func SelectMaxTsFromEventrank(
	eventid	string,
	userid	int,
) (
	ndata	int,
	maxts	time.Time,
) {

//	獲得ポイントのデータが何セットあるか調べる。
	sql := "select count(ts) from (select distinct(ts) from eventrank where eventid = ? and userid = ? ) tmptable"
	Err = Db.QueryRow(sql, eventid, userid).Scan(&ndata)

	if Err != nil {
		log.Printf("select count(ts) from (select distinct(ts) from eventrank  where eventid = %s and userid = %d ) tmptable ==> %d\n", eventid, userid, ndata)
		log.Printf("err=[%s]\n", Err.Error())
		ndata = -1
		return
	}
	if ndata == 0 {
		return
	}

//	直近の獲得ポイントデータのタイムスタンプを取得する。
	sql = "select max(ts) from eventrank where eventid = ? and userid = ? "
	Err = Db.QueryRow(sql, eventid, userid).Scan(&maxts)

	if Err != nil {
		log.Printf("error [select max(ts) from eventrank where eventid = %s and userid = %d]\n", eventid, userid)
		log.Printf("err=[%s]\n", Err.Error())
		ndata -= 1000
		return
	}
	log.Printf("select max(ts) from eventrank where eventid = %s and userid = %d ==> %v\n", eventid, userid, maxts)
	return

}

func SelectEidUidFromTimetable() (
	ndata	int,
	eventid	string,
	userid	int,
	sampletm1	time.Time,
) {

//
	sql := "select count(*) from timetable where sampletm1 < ? and status = 0"
	tnow := time.Now()
	Err = Db.QueryRow(sql, tnow).Scan(&ndata)

	if Err != nil {
		log.Printf("error [select count(*) from timetable where sampletm1 < %v and status = 0 ]\n", tnow)
		log.Printf("err=[%s]\n", Err.Error())
		ndata = -1
		return
	}
	if ndata == 0 {
		return
	}

//	獲得ポイントデータを取得すべきイベント、ユーザーIDを取得する。
	sql = "select eventid, userid, sampletm1 from timetable where status = 0 and sampletm1 = (select min(sampletm1) from timetable where status = 0)"
	Err = Db.QueryRow(sql).Scan(&eventid, &userid, &sampletm1)

	if Err != nil {
		log.Printf("error [select eventid, userid, sampletm1 from timetable where status = 0 and sampletm1 = (select min(sampletm1) from timetable where status = 0)]\n")
		log.Printf("err=[%s]\n", Err.Error())
		ndata -= 1000
		return
	}
	log.Printf("select eventid, userid, sampletm1 from timetable where status = 0 and sampletm1 = (select min(sampletm1) from timetable where status = 0) ==> %s %d %v\n", eventid, userid, sampletm1 )
	return

}


func SelectMaxTlsnidFromEventranking(
	eventid	string,
	userid	int,
) (
	maxtlsnid	int,
) {

//
	sql := "select max(t_lsnid) from eventrank where eventid =  ? and userid = ? "
	Err = Db.QueryRow(sql, eventid, userid).Scan(&maxtlsnid)

	if Err != nil {
		log.Printf("error [select max(t_lsnid) from eventrank where eventid =  %s and userid = %d ]\n", eventid, userid)
		log.Printf("err=[%s]\n", Err.Error())
		maxtlsnid = -1000
	}
	return
}


func UpdateTimetable(
	eventid	string,
	userid	int,
	sampletm1	time.Time,
	sampletm2	time.Time,
	totalpoint int,
) (
	status int,
) {

	var row *sql.Stmt

	status = 0

	sql := "update timetable set sampletm2 = ?, totalpoint = ?, status = 1 where eventid = ? and userid = ? and sampletm1 = ? and status = 0"
	row, Err = Db.Prepare(sql)
	if Err != nil {
		log.Printf("update timetable set sampletm2 = %v, totalpoint = %d, status = 1 where eventid = %s and userid = %d and sampletm1 = %v and status = 0 error (Update/Prepare) err=%s\n", sampletm2, totalpoint, eventid, userid, sampletm1, Err.Error())
		status = -1
		return
	}

	_, Err = row.Exec(sampletm2, totalpoint, eventid, userid, sampletm1)

	if Err != nil {
		log.Printf("update timetable set sampletm2 = %v, totalpoint = %d, status = 1 where eventid = %s and userid = %d and sampletm1 = %v and status = 0 error (Update/Prepare) err=%s\n", sampletm2, totalpoint, eventid, userid,sampletm1,  Err.Error())
		status = -2
	}

	return
}



func SelectEventRankingFromEventrank(
	eventid	string,
	userid	int,
	ts		time.Time,
) (
	eventranking EventRanking,
	status int,
) {

	var stmt *sql.Stmt
	var rows *sql.Rows

	status = 0


//	直近の獲得ポイントデータを読み込む
	sql := "SELECT listner, lastname, lsnid, t_lsnid, norder, nrank, point, increment, status "
	sql += " FROM eventrank WHERE eventid = ? and userid = ? and ts = ? order by norder"

	stmt, Err = Db.Prepare(sql)
	if Err != nil {
		log.Printf("err=[%s]\n", Err.Error())
		status = -1
		return
	}
	defer stmt.Close()

	rows, Err = stmt.Query(eventid, userid, ts)
	if Err != nil {
		log.Printf("err=[%s]\n", Err.Error())
		status = -2
		return
	}
	defer rows.Close()

	var evr EventRank

	for rows.Next() {
		Err = rows.Scan(&evr.Listner, &evr.Lastname, &evr.LsnID, &evr.T_LsnID, &evr.Order, &evr.Rank, &evr.Point, &evr.Incremental, &evr.Status)
		if Err != nil {
			log.Printf("err=[%s]\n", Err.Error())
			status = -3
			return
		}
		eventranking = append(eventranking, evr)
	}
	if Err = rows.Err(); Err != nil {
		log.Printf("err=[%s]\n", Err.Error())
		status = -4
		return
	}

	return

}
