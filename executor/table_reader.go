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

package executor

import (
	"log"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table           table.Table
	physicalTableID int64
	keepOrder       bool
	desc            bool
	ranges          []*ranger.Range
	dagPB           *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
	streaming     bool
	feedback      *statistics.QueryFeedback

	// corColInFilter tells whether there's correlated column in filter.
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	plans          []plannercore.PhysicalPlan

	sourceType string                  //同样的在plancore中，physicaltablescan计划构建成tablereaderexecutor执行器的时候
	pathInfo string                    //会将这种，source和path，以及pushdowncondition填充
	PushDownCondition string
}

// Open initialzes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInFilter {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)   //递归操作到子节点的时候，肯定是table reader exec了
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.corColInAccess {
		ts := e.plans[0].(*plannercore.PhysicalTableScan)
		access := ts.AccessCondition
		pkTP := ts.Table.GetPkColInfo().FieldType
		e.ranges, err = ranger.BuildTableRange(access, e.ctx.GetSessionVars().StmtCtx, &pkTP)
		if err != nil {
			return errors.Trace(err)
		}
	}

	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tableReader.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	if err := e.resultHandler.nextChunk(ctx, chk); err != nil {
		e.feedback.Invalidate()
		return err
	}
	return errors.Trace(nil)
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	e.ctx.StoreQueryFeedback(e.feedback)
	err := e.resultHandler.Close()
	return errors.Trace(err)
}

// buildResp first builds request and sends it to tikv using distsql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {

	//this is for debug the gRPC
	
	if e.sourceType == "csv"{
		//BY LANHAI:there,we create a SelectResult of csv.
		result,err := distsql.GetCSVSelectResult(e.pathInfo,e.columns)
		if err!=nil {
			return nil,errors.Trace(err)
		}
		result.Fetch(ctx)
		return result,nil ///this is needed to be recoveried
	}
	if e.sourceType == "postgresql"{
		result,err := distsql.GetPGSelectResult(e.pathInfo,e.columns,e.PushDownCondition)
		if err!=nil {
			return nil,errors.Trace(err)
		}
		result.Fetch(ctx)
		return result,nil
	}
	if e.sourceType=="redis"{
		log.Println("preparing for the redis reading")
		log.Println("redis table reader", e.PushDownCondition)
		//table scan 中还是包含col信息的
		result, err := distsql.GetRedisSelectResult(e.pathInfo, e.columns, e.PushDownCondition)
		if err!=nil{
			return nil, errors.Trace(err)
		}
		result.Fetch(ctx)    //ctx一般没有什么用，是使用tikv的resp的时候，会用到
		return result, nil
	}


	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(e.physicalTableID, ranges, e.feedback).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.retTypes(), e.feedback)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true.
	// optionalResult handles the request whose range is in signed int range.
	// result handles the request whose range is exceed signed int range.
	// Otherwise, we just set optionalFinished true and the result handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

	optionalFinished bool
}

func (tr *tableResultHandler) open(optionalResult, result distsql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return errors.Trace(err)
}
