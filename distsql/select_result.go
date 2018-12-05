// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"bufio"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	//"github.com/pingcap/tidb/planner/core"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var (
	_ SelectResult = (*selectResult)(nil)
	_ SelectResult = (*streamResult)(nil)
	_ SelectResult = (*csvSelectResult)(nil)
	_ SelectResult = (*pgSelectResult)(nil)
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Fetch fetches partial results from client.     //result set的接口
	Fetch(context.Context)
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// Next reads the data into chunk.
	Next(context.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
}

type resultWithErr struct {
	result kv.ResultSubset
	err    error
}

type row struct{
	cols []interface{}
}

type pgSelectResult struct{   //result就是返回的channel
	fieldTypes []*types.FieldType
	isOver chan bool
	over bool
	dataChunck chan chunk.Chunk  //this is for data from csv
	dataRow chan row
	data chunk.Chunk //this is for data storage
	dataR row
	info []*model.ColumnInfo
	pushDownConditions string
	//plans []core.PhysicalPlan
}



func (r *pgSelectResult) fetch(){
	for{
		break
	}
}

func (r *pgSelectResult) Fetch(ctx context.Context) {
	go r.fetch()
}

func (r *pgSelectResult) NextRaw(context.Context) ([]byte, error) {
	log.Print("IN NEXTRAW")
	return nil,nil
}

func (r *pgSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error{
	chk.Reset()
	for {
		if r.over==true && len(r.dataRow)==0 {
			break
		}
		select{
		case r.over = <-r.isOver:{
			break
		}
		case r.dataR = <- r.dataRow :{
			for i:=0;i< len(r.info);i++ {
				switch r.info[i].Tp {
				case mysql.TypeLong:
					x,_ := r.dataR.cols[i].(int)
					chk.AppendInt64(i,int64(x))
				case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
					mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
					x,_ := r.dataR.cols[i].(string)
					chk.AppendString(i,x)
				}
				case mysql.TypeFloat:{
					x,_:=r.dataR.cols[i].(float64)
					chk.AppendFloat32(i,float32(x))
				}

				}
			}
			break
		}

		}

	}

	return nil
}

func (r *pgSelectResult) Close() error{
	return nil
}


type pgReader struct{
	path string
	isOver chan bool
	dataChunck chan chunk.Chunk
	dataRow chan row
	info []*model.ColumnInfo
	//schema
	//plans []core.PhysicalPlan
	pushDownConditions string
}

type RequestPG struct{
	SQL string
	Count int
}

type ResultPG struct{
	Count int
	Result string
}

func (cR *pgReader) readPG(){
	pathinfos := strings.Split(cR.path,"#")
	address := pathinfos[0]+":"+pathinfos[1]
	tableName := pathinfos[2]
	client,err := rpc.DialHTTP("tcp",address)
	if err!=nil {
		log.Print(err)
	}

	//make the plan to sql
	/*
	TARGET LIST
	*/
	attr :=""
	for i,item:=range cR.info{
		if i>0 {
			attr+=","
		}
		attr+=item.Name.L
	}
	/*
	WHERE CONDITION
	*/

	conditionString := ""
	if cR.pushDownConditions!="" {
		conditionString += "where " + cR.pushDownConditions
	}
	/*
	conditionPlan := cR.plans[1]
	conditionPlan2,ok:=conditionPlan.(*core.PhysicalSelection)
	if ok {
		conditions:=conditionPlan2.Conditions
		for i,oneCondition:=range conditions{
			if i>0 {
				conditionString += "and "
			}
			//if it is a scalarfunction
			tryFunction,ok := oneCondition.(*expression.ScalarFunction)
			if ok {
				//tryFunction.FuncName.L
				//tryFunction.Function.
			}
		}
	}*/

	/*
	THE FINAL SQL
	*/
	sql := "select "+attr + " from " + tableName + " "+conditionString+";"
	args := &RequestPG{sql,len(cR.info)}
	var reply ResultPG
	err = client.Call("PGX.Require",&args,&reply)
	if err!=nil {
		log.Println(err)
	}
	// fixn bug, 当pg没有数据的时候，可以直接关闭管道，然后返回
	if reply.Result == ""{
		cR.isOver<-true
		return
	}
	recordss := strings.Split(reply.Result,",")
	for _,record:=range recordss  {
		records := strings.Split(record,"#")
		dataVal := make([]interface{},0)
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
			case mysql.TypeLong:{
				x1,err1 := strconv.Atoi(string(records[i]))
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[i])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[i],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			}}
		data := row{dataVal}
		cR.dataRow<-data
	}
	cR.isOver<-true
}


func GetPGSelectResult(path string,info []*model.ColumnInfo,pushDownConditions string)(SelectResult, error){
	dataRowChan := make(chan row,1)
	isOver := make(chan bool,1)
	cReader := pgReader{path:path,dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions}//plans:plans}
	go (&cReader).readPG()
	return &pgSelectResult{dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions},nil//plans:plans},nil
}


type RequestRedis struct{
	/*
	 * redis查询支持单点查询和范围查询
	 * 目前是现在是读上来，使用string的形式
	 * col offset 可以认为是有key， val， <key,val>,也就是两列
	*/
	QueryType int
	Offsets []int
	Value string
	Count int
}

type ResultRedis struct {
	Result string //同样用pg和csv的形式
}

type redisSelectResult struct {
	//解析成行我们需要类型信息
	fieldTypes []*types.FieldType
	isOver chan bool
	over bool
	dataChunk chan chunk.Chunk
	dataRow chan row
	data chunk.Chunk     //这个是我临时从管道接受用的
	dataR row
	info []*model.ColumnInfo
	pushDownConditions string
}

func (r *redisSelectResult)fetch(){
	for{
		break
	}
}

func (r *redisSelectResult)Fetch(ctx context.Context){
	go r.fetch()
}

func (r *redisSelectResult)NextRaw(ctx context.Context)([]byte, error){
	//这个函数我们暂时并不给予实现
	log.Println("in redis nextraw func")
	return nil, nil
}

func (r *redisSelectResult)Next(ctx context.Context, chk *chunk.Chunk)error{
	chk.Reset()
	for{
		if r.over==true && len(r.dataRow)==0{
			break
		}
		select{
		case r.over =<-r.isOver:{
			break
		}
		/*
		 * 这边是将我们从自定义的reader中扔过来的自定义row，封装成chunk之后，往上仍的过程
		 * 需要注意，chunk中的数据是列存的，填充的时候，需要对应col的下标进行填充
		 * row的长度，其实就是列存的深度，那么就是chunk.length
		*/



		case r.dataR =<- r.dataRow:{   //因为我们从rpc读完，之后就直接扔到select的管道中了
			for i:=0; i<len(r.info); i++ {

				switch r.info[i].Tp {
				case mysql.TypeLong:
					{
						x, _ := r.dataR.cols[i].(int)
						chk.AppendInt64(i, int64(x)) //这么看的话了，这个i其实是列的下标了
					}
				case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
					mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
					{
						x, _ := r.dataR.cols[i].(string)
						chk.AppendString(i, x)
					}
				case mysql.TypeFloat:
					{
						x, _ := r.dataR.cols[i].(float64)
						chk.AppendFloat32(i, float32(x))
					}
				}
			}
			break
		}
		}
	}
	return nil
}


func (r *redisSelectResult) Close()error {
	return nil
}

type redisReader struct{
	path string
	isOver chan bool
	dataChunk chan chunk.Chunk   //基本上用不到
	dataRow chan row
	info []*model.ColumnInfo
	pushDownConditions string
}


func (cR *redisReader)readRedis(){
	pathinfos := strings.Split(cR.path, "#")
	address := pathinfos[0]+":"+pathinfos[1]
	//no need like csv name file just need to connect to redis server
	client, err := rpc.DialHTTP("tcp", address)   //这边是dialHTTP！！！！！！
	if err!=nil{
		log.Println(err)
		panic(err)
	}
	//rq :=RequestRedis{}
	//re :=ResultRedis{}
	//client.Call("RedisXX.Require", &rq, &re)
	offsets := make([]int,0)
	for i:=0; i<len(cR.info); i++{      //列的信息，并不是schema中完整的，可能已经被投影过了
	                                    //所以这边col中，都记录着在原始的schema中的偏移
		offsets = append(offsets, cR.info[i].Offset)
	}
	//this part is commit from the pushDownConditions
	parax:=""
	queryType:=100
	if cR.pushDownConditions == ""{
		queryType =4
	}else{
		log.Println("sss", cR.pushDownConditions)
		//这个pushdown的条件还是上层传递下来的，自定义的
		infos:=strings.Split(cR.pushDownConditions, "#")
		queryType, _ =strconv.Atoi(infos[0])
		if queryType==1{     //应该是点查询
			parax = infos[1]
		}else{
			parax= infos[1]+"*"   //模糊查询
		}
	}
	log.Println("Redis")
	args := RequestRedis{QueryType:queryType, Offsets:offsets, Value:parax, Count:0}
	log.Println(args)
	var reply =ResultRedis{}

	// this is  a part for rpc call
	err = client.Call("RedisX.Require", &args, &reply)

	if err!=nil{
		log.Println(err)
	}
	if reply.Result==""{
		cR.isOver<-true
		return
	}
	recordss := strings.Split(reply.Result, ",")
	for _,record := range recordss{
		records := strings.Split(record, "#")
		dataVal := make([]interface{}, 0)
		for i:=0; i<len(records); i++{
			switch cR.info[i].Tp {
			case mysql.TypeLong:{
				x1, err1:=strconv.Atoi(string(records[i]))
				if err1!=nil{
					log.Println(err1)
				}
				dataVal = append(dataVal, x1)
			}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
					x1 := string(records[i])
					dataVal = append(dataVal, x1)
					}
			case mysql.TypeFloat:{
				x1,err1:=strconv.ParseFloat(records[i], 32)
				if err!=nil{
					log.Println(err1)
				}
				dataVal = append(dataVal, x1)
			}
			}
		}
		data := row{dataVal}   //我们读完之后，往上还是一行行扔的
		cR.dataRow<-data
	}
	cR.isOver<-true
}

//By Arenatlx: link the reader and redisSelectResult by channel!!!
func GetRedisSelectResult(path string, info []*model.ColumnInfo, pushDownConditions string)(SelectResult,error){
	dataRowChan := make(chan row, 1)
	isOver := make(chan bool, 1)
	redisReader := redisReader{path:path, dataRow:dataRowChan, isOver:isOver, info:info, pushDownConditions:pushDownConditions}
	go (&redisReader).readRedis()
	return &redisSelectResult{dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions},nil
}



type csvSelectResult struct{
	fieldTypes []*types.FieldType
	isOver chan bool
	over bool
	dataChunck chan chunk.Chunk  //this is for data from csv
	dataRow chan row
	data chunk.Chunk //this is for data storage
	dataR row
	info []*model.ColumnInfo
}

func (r *csvSelectResult) fetch(){
	for{


		break
	}
}

func (r *csvSelectResult) Fetch(ctx context.Context) {      //csv select result的fetch
	go r.fetch()
}

func (r *csvSelectResult) NextRaw(context.Context) ([]byte, error) {
	log.Print("IN NEXTRAW")
	return nil,nil
}

func (r *csvSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error{  //next的才是真正拿数据的
	chk.Reset()
	for {
		if r.over==true && len(r.dataRow)==0 {             //next再
			break
		}
		select{
			case r.over = <-r.isOver:{
				break
			}
			case r.dataR = <- r.dataRow :{
				for i:=0;i< len(r.info);i++ {
					switch r.info[i].Tp {
					case mysql.TypeLong:
						x,_ := r.dataR.cols[i].(int)
						chk.AppendInt64(i,int64(x))
					case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
						mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
						x,_ := r.dataR.cols[i].(string)
						chk.AppendString(i,x)
					}
						case mysql.TypeFloat:{
							x,_:=r.dataR.cols[i].(float64)
							chk.AppendFloat32(i,float32(x))
						}

					}
				}
				break
			}

		}

	}

	return nil
}

func (r *csvSelectResult) Close() error{
	return nil
}

type csvReader struct{
	path string
	isOver chan bool
	dataChunck chan chunk.Chunk
	dataRow chan row
	info []*model.ColumnInfo
	//schema
}

type Requestx2 struct{
	Start int
	TableName string
	Offsets []int
}

type Resultx2 struct{
	Count int
	Result string
}

func (cR *csvReader) readFile2(){
	pathinfos := strings.Split(cR.path,"#")
	address := pathinfos[0]+":"+pathinfos[1]
	csvName := pathinfos[2]
	client,err := rpc.DialHTTP("tcp",address)
	if err!=nil {
		log.Print(err)
	}
	//投影下推
	offsets := make([]int,0)
	for i:=0;i<len(cR.info);i++  {
		offsets = append(offsets,cR.info[i].Offset)    //正好可以利用它列信息在模式中的偏移
	}
	log.Println(offsets)

	args := &Requestx2{0,csvName,offsets}
	var reply Resultx2
	err = client.Call("CSVX.Require",args,&reply)
	if err!=nil {
		log.Println(err)
	}
	if reply.Result==""{
		cR.isOver<-true
		return
	}
	recordss := strings.Split(reply.Result,",")
	for _,record:=range recordss  {
		records := strings.Split(record,"#")
		dataVal := make([]interface{},0)
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
			case mysql.TypeLong:{
				x1,err1 := strconv.Atoi(string(records[i]))
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[i])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[i],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			}}
		data := row{dataVal}
		cR.dataRow<-data            //data 这边已经包装好返回channel了， 所以fetch的操作（就是包装)，可以省略
	}
	cR.isOver<-true      //数据读完之后，isover的channel设置为over
}

func (cR *csvReader) readFile(){    //本地读文件
	//suppose we know the schema,later we need to store the schema,when init the csvReader
	f,err := os.OpenFile(cR.path,os.O_RDONLY,0644)
	if err != nil{
		log.Println("OPEN ERROR")
		return // there maybe some send some infos to let the csvSR know nothing this time
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for{
		recordTemp,_,err:=reader.ReadLine()
		if err!=nil {
			//log.Println("Read Over")
			break
		}
		dataVal := make([]interface{},0)
		recordTemp = recordTemp[:len(recordTemp)]
		recordTempS := string(recordTemp)
		records:=strings.Split(recordTempS,",")
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
				case mysql.TypeLong:{
					x1,err1 := strconv.Atoi(string(records[cR.info[i].Offset]))
					if  err1 != nil {
						log.Print(err1)
					}
					dataVal = append(dataVal,x1)
				}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[cR.info[i].Offset])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[cR.info[i].Offset],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
		}}

		data := row{dataVal}
		cR.dataRow<-data
	}
	cR.isOver<-true
}


//BY LANHAI:this function will be called to create a csvSR
func GetCSVSelectResult(path string,info []*model.ColumnInfo)(SelectResult, error){
	dataRowChan := make(chan row,1)
	isOver := make(chan bool,1)
	cReader := csvReader{path:path,dataRow:dataRowChan,isOver:isOver,info:info}
	if strings.Count(path, "#")==0{
		//说明是本地文件路径
		go (&cReader).readFile()
	}else{
		//说明是远程的rpc调用
		//remote rpc call here
		go (&cReader).readFile2()
	}
	//返回能够返回数据的channel
	return &csvSelectResult{dataRow:dataRowChan,isOver:isOver,info:info},nil
}



type selectResult struct {
	label string
	resp  kv.Response

	results chan resultWithErr
	closed  chan struct{}

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context      //这里的有r.ctx.GetSessionVars().MaxChunkSize，规定了chunk size

	selectResp *tipb.SelectResponse
	respChkIdx int

	feedback     *statistics.QueryFeedback
	partialCount int64 // number of partial results.
	sqlType      string
}

func (r *selectResult) Fetch(ctx context.Context) {
	go r.fetch(ctx)
}

func (r *selectResult) fetch(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		close(r.results)
		duration := time.Since(startTime)
		metrics.DistSQLQueryHistgram.WithLabelValues(r.label, r.sqlType).Observe(duration.Seconds())
	}()
	for {
		resultSubset, err := r.resp.Next(ctx)               //select result自身的fetch，就是去getResp之后，
		                                                    // 目前只有tikv自己的实现
		if err != nil {                                     //拿next的数据，扔到result的管道中
			r.results <- resultWithErr{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			return
		}

		select {
		case r.results <- resultWithErr{result: resultSubset}:  //数据在fetch的时候，写入result管道
		case <-r.closed:
			// If selectResult called Close() already, make fetch goroutine exit.
			return
		case <-ctx.Done():
			return
		}
	}
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) ([]byte, error) {
	re := <-r.results
	r.partialCount++                            //batch结果计数
	r.feedback.Invalidate()
	if re.result == nil || re.err != nil {
		return nil, errors.Trace(re.err)
	}
	return re.result.GetData(), nil
}

// Next reads data to the chunk.
func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	/*
	 * 这海哥的代码，这里是bug，填充chunk的时候，需要判断chunk的大小，不然要溢出
	 */
	for chk.NumRows() < r.ctx.GetSessionVars().MaxChunkSize {
		if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.getSelectResp()
			if err != nil || r.selectResp == nil {
				return errors.Trace(err)
			}
		}
		err := r.readRowsData(chk)     //chk已经是空的
		if err != nil {
			return errors.Trace(err)
		}
		if len(r.selectResp.Chunks[r.respChkIdx].RowsData) == 0 {
			r.respChkIdx++
		}
	}
	return nil
}

func (r *selectResult) getSelectResp() error {
	r.respChkIdx = 0
	for {
		re := <-r.results       //还是从result中拿的数据
		if re.err != nil {
			return errors.Trace(re.err)
		}
		if re.result == nil {
			r.selectResp = nil
			return nil
		}
		r.selectResp = new(tipb.SelectResponse)
		err := r.selectResp.Unmarshal(re.result.GetData())
		if err != nil {
			return errors.Trace(err)
		}
		if err := r.selectResp.Error; err != nil {
			return terror.ClassTiKV.New(terror.ErrCode(err.Code), err.Msg)
		}
		sc := r.ctx.GetSessionVars().StmtCtx
		for _, warning := range r.selectResp.Warnings {
			sc.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
		}
		r.feedback.Update(re.result.GetStartKey(), r.selectResp.OutputCounts)
		r.partialCount++
		sc.MergeExecDetails(re.result.GetExecDetails())
		if len(r.selectResp.Chunks) == 0 {
			continue
		}
		return nil
	}
}

//这个selectResp中的chunk好像和chunk.Chunk不一样
func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData    //数据已经在r.selectResp.Chunks
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().Location())
	for chk.NumRows() < maxChunkSize && len(rowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			rowsData, err = decoder.DecodeOne(rowsData, i, r.fieldTypes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData  //解码之后，还是写回去
	return nil
}

// Close closes selectResult.
func (r *selectResult) Close() error {
	// Close this channel tell fetch goroutine to exit.
	if r.feedback.Actual() >= 0 {
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	close(r.closed)
	return r.resp.Close()
}
