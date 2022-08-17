/*
	SHOWROOMのイベント貢献ランキングを取得し、枠別の貢献ポイントを算出します。
	算出の際にはリスナー名の変更の突き合わせを行います（100%完全に突き合わせが行えるわけではありません）
	算出したリスナー別枠別貢献ポイントはeventrankテーブルに保存されます。


	前提
		1. SHOWROOMの獲得ポイントを監視しているプロセスが起動し
		2. 配信の終了後、配信に関するデータをtimtableテーブルに保存している
		ことが必要です。

		このプロセスは常時起動しており、timtableに新規の配信に関するデータが保存されるとそれにしたがってイベント貢献ランキングを取得します。

	使い方

		% 実行モジュール名 FilenameOfConfigurationFile

	課題
		このプログラムは以前データをファイルから取得し結果をExcelファイルに書き出していたため、現在でもそのときの名残があります。

*/
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/PuerkitoBio/goquery"
	lsdp "github.com/deltam/go-lsd-parametrized"

	"github.com/Chouette2100/exsrapi"

	"ShowroomDBlib"
)

/*
1.0A0	一致度のチェックで
				case second_v < 1.1 && second_v-first_v > 0.2:
			を
				case second_v < 1.1 && second_v-first_v > 0.2:
			と訂正した（"3C"と判定すべきものが"3B"と判定されていた）
1.0A1		「一致度のチェック対象が一つしかない」の判定条件を見直し
1.0B0		Sheet2への差分の書き込み処理を追加
1.0B1		順位（rank）をExcelに出力するようにした。
1.0C0		毎回別のファイルに書き込むようにした（データ書き込み後Excelファイルが壊れているケースがあるため）
1.0D0		Go Ver.1.17.4対応（import関連の修正、まだ3つの"問題"あり）
1.0D1		importのShowroomLibを正しいShowroomlibに修正する。
2.0A00		データの保存をDBに行う。
2.0B00		データ取得のタイミングをtimetableから得る。Excelへのデータの保存をやめる。
2.0B01		ムリな突き合わせをしないようにする。一致度を厳しくし、いったんランク外になったリスナーは突き合わせの対象としない。
2.0B02		リスナー名が変化していないときはLastnameをクリアする。
2.0B03		timetableの更新で処理が終わっていないものを処理済みにしていた問題を修正する。
			（この問題は通常発生しない。デバッグのため一つの配信に対して複数回の貢献ポイント取得を行ったときに発生した）
2.0C00		実行を指定した時間で打ち切るようにする。さくらインターネットのレンタルサーバでデーモンとみなされないための設定。
020AD00	WaitNextMinute()を取り込みShowroomlibをimportしない。
2.4.0		Githubにリリースする。

*/

const version = "020AD00"

type Environment struct {
	IntervalHour int
}



/*
	GetPointsCont()
	イベントページのURLと配信者さんのIDから、イベント貢献ランキングのリストを取得します。

	引数
	EvnetName	string	イベント名、下記イベントページURLの"event_id"の部分
		https://www.showroom-live.com/event/event_id
	ID_Account	string	配信者さんのID
		SHOWROOMへの登録順を示すと思われる6桁(以下)の数字です。アカウントとは違います。

	戻り値
	TotaScore	int
	eventranking	struct
		Rank	int		リスナーの順位
		Point	int		リスナーの貢献ポイント
		Listner string	リスナーの名前
	status		int

	***
	リスナーさんの日々のあるいは配信ごとの貢献ポイントの推移がすぐにわかれば配信者さんもいろいろ手の打ちよう(?)が
	ありそうですが、「リスナーの名前」というのはリスナーさんが自由に設定・変更できるので貢献ポイントを追いかけて
	行くのはけっこうたいへんです。
	このプログラムではLevenshtein距離による類似度のチェックや貢献ランキングの特性を使ってニックネームの変更を追尾しています。
	リスナーさんのuseridがわかればいいのですが、いろいろと面倒なところがあります。

	「レーベンシュタイン距離 - Wikipedia」
	https://ja.wikipedia.org/wiki/%E3%83%AC%E3%83%BC%E3%83%99%E3%83%B3%E3%82%B7%E3%83%A5%E3%82%BF%E3%82%A4%E3%83%B3%E8%B7%9D%E9%9B%A2

	「カスタマイズしやすい重み付きレーベンシュタイン距離ライブラリをGoで書きました - サルノオボエガキ」
	https://deltam.blogspot.com/2018/10/go.html

	貢献ポイントをこまめに記録しておくと、減算ポイントが発生したときの原因アカウントの特定に使えないこともないです。
	（実際やってみるとわかるのですが、これはこれでなかなかたいへんです）
	なお、原因アカウントの特定、というのは犯人探しというような意味で言ってるわけじゃありませんので念のため。

*/
func GetPointsCont(EventName, ID_Account string) (
	TotalScore int,
	eventranking ShowroomDBlib.EventRanking,
	status int,
) {

	status = 0

	//	貢献ランキングのページを開き、データ取得の準備をします。
	_url := "https://www.showroom-live.com/event/contribution/" + EventName + "?room_id=" + ID_Account

	resp, error := http.Get(_url)
	if error != nil {
		log.Printf("GetEventInfAndRoomList() http.Get() err=%s\n", error.Error())
		status = 1
		return
	}
	defer resp.Body.Close()

	var doc *goquery.Document
	doc, error = goquery.NewDocumentFromReader(resp.Body)
	if error != nil {
		log.Printf("GetEventInfAndRoomList() goquery.NewDocumentFromReader() err=<%s>.\n", error.Error())
		status = 1
		return
	}

	/*
		u := url.URL{}
		u.Scheme = doc.Url.Scheme
		u.Host = doc.Url.Host
	*/

	//	各リスナーの情報を取得します。
	//	var selector_ranking, selector_listner, selector_point, ranking, listner, point string
	var ranking, listner, point string
	var iranking, ipoint int
	var eventrank ShowroomDBlib.EventRank

	TotalScore = 0

	//	eventranking = make([]EventRank)

	doc.Find(".table-type-01:nth-child(2) > tbody > tr").Each(func(i int, s *goquery.Selection) {
		if i != 0 {

			//	データを一つ取得するたびに(戻り値となる)リスナー数をカウントアップします。
			//	NoListner++

			//	以下セレクターはブラウザの開発ツールを使って確認したものです。

			//	順位を取得し、文字列から数値に変換します。
			//	selector_ranking = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 1)
			ranking = s.Find("td:nth-child(1)").Text()

			/*
				//	データがなくなったらbreakします。このときのNoListnerは通常100、場合によってはそれ以下です。
				if ranking == "" {
					break
				}
			*/

			iranking, _ = strconv.Atoi(ranking)

			//	リスナー名を取得します。
			//	selector_listner = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 2)
			listner = s.Find("td:nth-child(2)").Text()

			//	貢献ポイントを取得し、文字列から"pt"の部分を除いた上で数値に変換します。
			//	selector_point = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 3)
			point = s.Find("td:nth-child(3)").Text()
			point = strings.Replace(point, "pt", "", -1)
			ipoint, _ = strconv.Atoi(point)
			TotalScore += ipoint

			//	戻り値となるスライスに取得したデータを追加します。
			eventrank.Rank = iranking
			eventrank.Point = ipoint
			eventrank.Listner = listner
			eventrank.Order = i
			eventranking = append(eventranking, eventrank)
		}
	})

	return
}

func MakeListInSheet(
	oldfilename,
	newfilename string,
	eventranking ShowroomDBlib.EventRanking,
	ncolw int,
	totalscore,
	totalincremental int,
) (
	status int,
) {

	status = 0

	no := len(eventranking)

	// Excelファイルをオープンする。
	//	fxlsx, err := excelize.OpenFile(EventID + ".xlsx")
	//	filename := event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	//	filename = "_tmp.xlsx"
	log.Printf(" inputfilename=<%s>\n", oldfilename)
	log.Printf(" outputfilename=<%s>\n", newfilename)
	fxlsx, err := excelize.OpenFile(oldfilename)
	if err != nil {
		log.Printf("<%v>\n", err)
		status = -1
		return
	}

	sheet1 := "Sheet1"
	sheet2 := "Sheet2"

	scolnew := CtoA(ncolw)
	//	scollast := CtoA(ncolw - 1)

	t19000101 := time.Date(1899, 12, 30, 0, 0, 0, 0, time.Local)
	tnow := time.Now()

	fxlsx.SetCellValue(sheet1, scolnew+"1", totalscore)
	fxlsx.SetCellValue(sheet2, scolnew+"1", totalincremental)

	//	fxlsx.SetCellValue(sheet, scolnew+"2", tnow)

	tserial := tnow.Sub(t19000101).Minutes() / 60.0 / 24.0
	fxlsx.SetCellValue(sheet1, scolnew+"3", tserial)
	fxlsx.SetCellValue(sheet2, scolnew+"3", tserial)

	fxlsx.SetCellValue(sheet1, scolnew+"4", tnow.Format("01/02 15:04"))
	fxlsx.SetCellValue(sheet2, scolnew+"4", tnow.Format("01/02 15:04"))

	for i := 0; i < no; i++ {
		loci := eventranking[i].Order
		srow := fmt.Sprintf("%d", loci+5)

		fxlsx.SetCellValue(sheet1, "A"+srow, eventranking[i].Rank)
		fxlsx.SetCellValue(sheet2, "A"+srow, eventranking[i].Rank)

		fxlsx.SetCellValue(sheet1, "C"+srow, eventranking[i].Listner)
		fxlsx.SetCellValue(sheet2, "C"+srow, eventranking[i].Listner)

		fxlsx.SetCellValue(sheet1, scolnew+srow, eventranking[i].Point)
		if eventranking[i].Incremental != -1 {
			fxlsx.SetCellValue(sheet2, scolnew+srow, eventranking[i].Incremental)
		} else {
			fxlsx.SetCellValue(sheet2, scolnew+srow, "n/a")
		}

		if eventranking[i].Lastname != "" {
			fxlsx.SetCellValue(sheet1, "B"+srow, eventranking[i].Lastname)
			/*
				//	Excelファイルの肥大化はこれが原因かも。あくまで"かも"。
				fxlsx.AddComment(sheet1, scollast+srow, `{"author":"Chouette: ","text":"`+eventranking[i].Lastname+`"}`)
			*/
		} else {
			fxlsx.SetCellValue(sheet1, "B"+srow, nil)
		}
	}

	//	serial++
	//	filename = event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	//	Printf(" filename(out) = <%s>\n", filename)
	err = fxlsx.SaveAs(newfilename)

	if err != nil {
		log.Printf(" error in SaveAs() <%s>\n", err)
		status = -1
	}

	return
}

func CtoA(col int) (acol string) {
	acol = string(rune('A') + int32((col-1)%26))
	if int((col-1)/26) > 0 {
		acol = string(rune('A')+(int32((col-1)/26))-1) + acol
	}
	return
}
func CRtoA1(col, row int) (a1 string) {
	a1 = CtoA(col) + fmt.Sprintf("%d", row)
	return
}

func CopyFile(inputfile, outputfile string) (status int) {

	status = 0

	// read the whole file at once
	b, err := ioutil.ReadFile(inputfile)
	if err != nil {
		//	panic(err)
		log.Printf("error <%v>\n", err)
		status = -1
		return
	}

	// write the whole body at once
	err = ioutil.WriteFile(outputfile, b, 0644)
	if err != nil {
		//	panic(err)
		log.Printf("error <%v>\n", err)
		status = -2
	}
	return

}

func ReadListInSheet(
	oldfilename string,
) (
	eventranking ShowroomDBlib.EventRanking,
	ncolw int,
	status int,
) {

	status = 0

	// Excelファイルをオープンする。
	//	fxlsx, err := excelize.OpenFile(EventID + ".xlsx")
	//	filename := event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	log.Printf(" inputfilename=<%s>\n", oldfilename)
	//	filename = "_tmp.xlsx"
	fxlsx, err := excelize.OpenFile(oldfilename)
	if err != nil {
		log.Printf("<%v>\n", err)
		status = -1
		return
	}

	sheet := "Sheet1"

	for i := 4; ; i++ {
		//	value, _ := fxlsx.GetCellValue(sheet, CRtoA1(i, 4))
		value := fxlsx.GetCellValue(sheet, CRtoA1(i, 4))
		if value == "" {
			ncolw = i
			if ncolw == 4 {
				return
			}
			break
		}
	}

	var eventrank ShowroomDBlib.EventRank
	//	eventranking = make([]EventRank)

	scol := CtoA(ncolw - 1)
	for i := 0; i < 200; i++ {
		srow := fmt.Sprintf("%d", i+5)
		//	listner, _ := fxlsx.GetCellValue(sheet, "C"+srow)
		//	spoint, _ := fxlsx.GetCellValue(sheet, scol+srow)
		listner := fxlsx.GetCellValue(sheet, "C"+srow)
		spoint := fxlsx.GetCellValue(sheet, scol+srow)
		if listner == "" && spoint == "" {
			log.Println("*** break *** i=", i)
			break
		}

		eventrank.Order = i

		eventrank.Listner = listner

		eventrank.Point, _ = strconv.Atoi(spoint)

		eventranking = append(eventranking, eventrank)
	}

	sort.Sort(eventranking)

	return
}

func CompareEventRanking(
	last_eventranking ShowroomDBlib.EventRanking,
	new_eventranking ShowroomDBlib.EventRanking,
	idx int,
) (ShowroomDBlib.EventRanking, int) {

	totalincremental := 0

	log.Printf("          Phase 1\n")
	//	既存のデータとリスナー名が一致するデータがあったときは既存のデータを更新する。
	ncol := 1
	msg := ""
	for j := 0; j < len(last_eventranking); j++ {
		for i := 0; i < len(new_eventranking); i++ {
			if new_eventranking[i].Status == 1 {
				continue
			}
			if new_eventranking[i].Listner == last_eventranking[j].Listner {
				if new_eventranking[i].Point >= last_eventranking[j].Point {
					if last_eventranking[j].Point != -1 {
						incremental := new_eventranking[i].Point - last_eventranking[j].Point
						totalincremental += incremental
						last_eventranking[j].Incremental = incremental
					} else {
						last_eventranking[j].Incremental = -1
					}
					last_eventranking[j].Rank = new_eventranking[i].Rank
					last_eventranking[j].Point = new_eventranking[i].Point
					last_eventranking[j].Order = new_eventranking[i].Order
					last_eventranking[j].Lastname = ""
					new_eventranking[i].Status = 1
					last_eventranking[j].Status = 1
					msg = msg + fmt.Sprintf("%3d/%3d  ", j, i)
					if ncol == 10 {
						log.Printf("%s\n", msg)
						ncol = 1
						msg = ""
					} else {
						ncol++
					}
					break
				}
			}
		}
	}
	if msg != "" {
		log.Printf("%s\n", msg)
	}

	log.Printf("          Phase 2\n")

	phase2 := func() {
		log.Printf("     vvvvv     Phase 2\n")

		//	現在のポイント以上のリスナーが一人しかいないなら同一人物のはず
	Outerloop:
		for j := 0; j < len(last_eventranking); j++ {
			if last_eventranking[j].Status == 1 {
				continue
			}
			noasgn := -1
			for i := 0; i < len(new_eventranking); i++ {
				if new_eventranking[i].Status == 1 {
					//	すでに突き合わせが終わったものは対象にしない。
					continue
				}
				if new_eventranking[i].Point < 0 {
					//	いったんランクキング表外に出たものは突き合わせの対象としない。
					continue
				}
				if new_eventranking[i].Point < last_eventranking[j].Point {
					break
				}

				if noasgn != -1 {
					//	現在のポイント以上のリスナーが複数人いるとき
					//	ここで処理を完全やめてしまうのは last_eventranking がソートしてあることが前提
					//	ソートされていないのであれば単なるbreakにすべき
					break Outerloop
				} else {
					//	現在のポイント以上のはじめてのリスナー
					noasgn = i
				}
			}
			if noasgn != -1 {
				//	現在のポイント以上のリスナーが一人しかいなかった
				if last_eventranking[j].Point != -1 {
					incremental := new_eventranking[noasgn].Point - last_eventranking[j].Point
					totalincremental += incremental
					last_eventranking[j].Incremental = incremental
				} else {
					last_eventranking[j].Incremental = -1
				}
				last_eventranking[j].Rank = new_eventranking[noasgn].Rank
				last_eventranking[j].Point = new_eventranking[noasgn].Point
				last_eventranking[j].Order = new_eventranking[noasgn].Order
				new_eventranking[noasgn].Status = 1
				last_eventranking[j].Status = 1
				last_eventranking[j].Lastname = last_eventranking[j].Listner + " [2]"
				last_eventranking[j].Listner = new_eventranking[noasgn].Listner
				log.Printf("*****         【%s】 equals to 【%s】\n", new_eventranking[noasgn].Listner, last_eventranking[j].Lastname)
			}

		}
		log.Printf("     ^^^^^     Phase 2\n")
	}
	//	コメントにした理由を思い出す！
	//	phase2()

	log.Printf("          Phase 3\n")
	//	完全に一致するものがない場合は一致度が高いものを探す。
	// weighted
	wd := lsdp.Weights{Insert: 0.8, Delete: 0.8, Replace: 1.0}
	// weighted and normalized
	nd := lsdp.Normalized(wd)
	for j := 0; j < len(last_eventranking); j++ {
		if last_eventranking[j].Status == 1 {
			continue
		}
		log.Println("---------------")
		first_n := 0
		first_v := 2.0
		second_v := 2.0
		for i := 0; i < len(new_eventranking); i++ {
			if new_eventranking[i].Status == 1 {
				continue
			}
			if new_eventranking[i].Point < last_eventranking[j].Point {
				break
			}

			newlistner := new_eventranking[i].Listner
			lastlistner := last_eventranking[j].Listner
			value := nd.Distance(newlistner, lastlistner)
			log.Printf("%6.3f [%3d] 【%s】 [%3d] 【%s】\n", value, j, lastlistner, i, newlistner)
			if value < first_v {
				second_v = first_v
				first_v = value
				first_n = i
			} else if value < second_v {
				second_v = value
			}
		}

		phase3 := func(cond string, dist float64) {
			if last_eventranking[j].Point != -1 {
				incremental := new_eventranking[first_n].Point - last_eventranking[j].Point
				totalincremental += incremental
				last_eventranking[j].Incremental = incremental
			} else {
				last_eventranking[j].Incremental = -1
			}
			last_eventranking[j].Rank = new_eventranking[first_n].Rank
			last_eventranking[j].Point = new_eventranking[first_n].Point
			last_eventranking[j].Order = new_eventranking[first_n].Order
			new_eventranking[first_n].Status = 1
			last_eventranking[j].Status = 1
			last_eventranking[j].Lastname = last_eventranking[j].Listner + " [" + cond + fmt.Sprintf("%6.3f", dist) + "]"
			last_eventranking[j].Listner = new_eventranking[first_n].Listner
			log.Printf("*****         【%s】 equals to 【%s】\n", last_eventranking[j].Lastname, new_eventranking[first_n].Listner)
		}

		switch {
		//	case first_v < 0.72:	//	この数値は大きすぎると思われる。0.6を超えて一致と判断されるものはあやしいものが多かった（2022-03-23)
		case first_v < 0.62:
			//	一致度が高い
			phase3("3A", first_v)
		case second_v < 1.1 && second_v-first_v > 0.2:
			//	一致度が他に比較して高い
			phase3("3B", first_v)
		case first_v < 1.1 && second_v > 1.1 &&
			last_eventranking[j].Point != -1 &&
			(j == len(last_eventranking)-1 || last_eventranking[j].Point != last_eventranking[j+1].Point):
			//	一致度のチェック対象が一つしかない
			//	ここで last_eventranking[j].Point != last_eventranking[j+1].Point の条件が成り立たないことはありえないはずだが...
			phase3("3C", first_v)
		default:
			//	同一と思われるデータがみつからなかった。
			last_eventranking[j].Point = -1
			last_eventranking[j].Incremental = -1
			last_eventranking[j].Status = -1
			last_eventranking[j].Order = 999
			last_eventranking[j].Lastname = ""
			log.Printf("*****         【%s】  not found.\n", last_eventranking[j].Listner)
		}

	}

	phase2()

	log.Printf("          Phase 4\n")

	//	既存のランキングになかったリスナーを既存のランキングに追加する。
	//	ソートはしない。ソートするとExcelにあるデータと整合性がとれなくなる。
	//	つまり、ソートはExcelで行う。
	var eventrank ShowroomDBlib.EventRank
	no := len(last_eventranking)
	for i := 0; i < len(new_eventranking); i++ {
		if new_eventranking[i].Status != 1 {
			eventrank.Order = no
			no++
			eventrank.Listner = new_eventranking[i].Listner
			eventrank.Rank = new_eventranking[i].Rank
			eventrank.Point = new_eventranking[i].Point
			eventrank.Order = new_eventranking[i].Order
			eventrank.T_LsnID = new_eventranking[i].Order + idx*1000
			eventrank.Incremental = -1

			incremental := new_eventranking[i].Point
			totalincremental += incremental
			eventrank.Incremental = incremental

			last_eventranking = append(last_eventranking, eventrank)
		}
	}

	return last_eventranking, totalincremental
}
func ExtractTask(
	environment *Environment,
	/*
		bmakesheet bool,
	*/
) (
	status int,
) {

	//	var hh, mm, hhn, mmn int
	var hhn, mmn int
	//	var event_id, room_id string
	//	var bmakesheet bool

	bmakesheet := true

	fmt.Printf("%s ***************** ExtractTaskGroup() ****************\n", time.Now().Format("2006/1/2 15:04:05"))
	defer fmt.Printf("%s ************* end of ExtractTaskGroup() *************\n", time.Now().Format("2006/1/2 15:04:05"))

	hhn = 99
	mmn = 99

	st := time.Now()
	log.Printf(" Start of ExtractTaskGroup() at %s\n", st.Format("2006/1/2 15:04:05"))

Outerloop:
	for {

		for {

			ndata, event_id, userno, sampletm1 := ShowroomDBlib.SelectEidUidFromTimetable()
			if ndata <= 0 {
				break
			}

			room_id := fmt.Sprintf("%d", userno)

			log.Printf(" ndata = %d event_id [%s]  userno =%d.\n", ndata, event_id, userno)

			log.Printf("------------------- new_eventranking --------------------\n")
			//	totalscore, new_eventranking, _ := GetPointsCont(event_id, room_id)
			_, new_eventranking, _ := GetPointsCont(event_id, room_id)

			/*
				for i := 0; i < len(new_eventranking); i++ {
					log.Printf("%3d\t%7d\t【%s】\r\n", new_eventranking[i].rank, new_eventranking[i].point, new_eventranking[i].listner)
				}
				log.Printf("Total Score=%d\n", totalscore)
			*/

			log.Printf("------------------- last_eventranking --------------------\n")

			last_eventranking := make(ShowroomDBlib.EventRanking, 0)

			ndata, maxts := ShowroomDBlib.SelectMaxTsFromEventrank(event_id, userno)
			if ndata < 0 {
				log.Printf(" %d returned by SelectMaxTsFromEventrank()\n", ndata)
				break Outerloop
			} else if ndata > 0 {
				last_eventranking, status = ShowroomDBlib.SelectEventRankingFromEventrank(event_id, userno, maxts)
			}
			/*	*/
			for i := 0; i < len(last_eventranking); i++ {
				log.Printf("%3d\t%7d\t【%s】\r\n", last_eventranking[i].Order, last_eventranking[i].Point, last_eventranking[i].Listner)
			}
			/*	*/

			log.Printf("------------------- compare --------------------\n")
			idx := ShowroomDBlib.SelectMaxTlsnidFromEventranking(event_id, userno) / 1000
			if idx >= 1000 {
				idx /= 1000
			}
			idx += 1
			final_eventranking, totalincremental := CompareEventRanking(last_eventranking, new_eventranking, idx)
			log.Printf("------------------- final_eventranking --------------------\n")
			for i := 0; i < len(final_eventranking); i++ {
				if final_eventranking[i].Lastname != "" {
					log.Printf("%3d\t%7d\t【%s】\t【%s】\r\n",
						final_eventranking[i].Order,
						final_eventranking[i].Point,
						final_eventranking[i].Listner,
						final_eventranking[i].Lastname)
				} else {
					log.Printf("%3d\t%7d\t【%s】\r\n",
						final_eventranking[i].Order,
						final_eventranking[i].Point,
						final_eventranking[i].Listner)
				}
			}

			if bmakesheet {

				sampletm2 := time.Now().Truncate(time.Minute)
				ier_status := ShowroomDBlib.InsertIntoEventrank(event_id, userno, sampletm2, final_eventranking)
				if ier_status != 0 {
					log.Printf(" Can`t insert into eventrank.\n")
				}
				ShowroomDBlib.UpdateTimetable(event_id, userno, sampletm1, sampletm2, totalincremental)

			} else {
				for i := 1; i < 100; i++ {
					fmt.Printf(" (%d) %s", i, CtoA(i))
				}
				fmt.Printf(".\n")
			}

		}
		hhn, mmn, _ = WaitNextMinute()
		fmt.Printf("** %02d %02d\n", hhn, mmn)

		if (hhn+1)%environment.IntervalHour == 0 && mmn == 0 {
			log.Printf(" End of ExtractTaskGroup() t=%s\n", time.Now().Format("2006/1/2 15:04:05"))
			break
		}

	}

	status = 0

	return
}

/*
	WaitNextMinute()
	現在時の時分の次の時分までウェイトします。
	現在時が11時12分10秒であれば、11時13分00秒までウェイトします。

	引数
	なし

	戻り値
	hhn		int		ウェイト終了後の時刻の時
	mmn		int		ウェイト終了後の時刻の分
	ssn		int		ウェイト終了後の時刻の秒
*/
func WaitNextMinute() (hhn, mmn, ssn int) {

	//	現在時（時分秒.....）
	t0 := time.Now()

	//	現在時（時分）
	t0tm := t0.Truncate(1 * time.Minute)

	//	次の時分（現在時が11時12分10秒であれば、11時13分00秒）
	t0tm = t0tm.Add(1 * time.Minute)

	//	次の時分までウェイトします。
	dt := t0tm.Sub(t0)
	time.Sleep(dt + 100*time.Millisecond)

	//	現在時を戻り値にセットします。
	hhn, mmn, ssn = time.Now().Clock()

	return
}

func main() {

	if len(os.Args) > 1 {
		fmt.Println("Usage: ", os.Args[0])
		return
	}

	logfilename := "GetPointsCont01" + "_" + version + "_" + ShowroomDBlib.Version + "_" + time.Now().Format("20060102") + ".txt"
	logfile, err := os.OpenFile(logfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic("cannnot open logfile: " + logfilename + err.Error())
	}
	defer logfile.Close()
	//	log.SetOutput(logfile)
	log.SetOutput(io.MultiWriter(logfile, os.Stdout))

	log.Printf("\n")
	log.Printf("\n")
	log.Printf("************************ GetPointsCont01 Ver.%s *********************\n", version+"_"+ShowroomDBlib.Version)

	dbconfig, err := ShowroomDBlib.LoadConfig("ServerConfig.yml")
	if err != nil {
		log.Printf("ShowroomDBlib.LoadConfig() Error: %s\n", err.Error())
		return
	}
	//	fmt.Printf("dbconfig: %v\n", dbconfig)

	var environment Environment

	err = exsrapi.LoadConfig("Environment.yml", &environment)
	if err != nil {
		log.Printf("LoadConfig returned err = %+v\n", err)
		log.Printf("Set IntervalMin to 99999.\n")
		environment.IntervalHour = 99999
	}
	log.Printf(" environment=%+v\n", environment)

	status := ShowroomDBlib.OpenDb(dbconfig)
	if status != 0 {
		log.Printf("OpenDB returned status = %d\n", status)
		return
	}
	defer ShowroomDBlib.Db.Close()

	ExtractTask(&environment)

}
