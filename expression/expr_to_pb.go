// Copyright 2016 PingCAP, Inc.
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

package expression

import (
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
)

/*
 * 崔一丁，expression_to_pb,使用用来将executor直接发送到tikv的过程中使用的，其中的表达式
 * 需要将其转化成tipb传输所用的protobuf的形式，所以才有了这个文件
*/

func ExpressionToStringForRedis(conditions []Expression)string{
	conditionString := ""
	for i, oneCondition := range conditions{
		//just consider first condition (if have)
		if i>0{
			break
		}
		tryFunction,ok:=oneCondition.(*ScalarFunction)
		if ok{
			args:=tryFunction.Function.getArgs()
			arg2, _ := args[1].(*Constant)    //获取表示的第二个参数，如果是常量，那么我们就可以做点事情
			value2 := arg2.Value
			rightArgForStr := value2.GetString()
			operator := tryFunction.FuncName.L
			if operator == "like"{
				conditionString = "2#"+rightArgForStr    //我左边的列信息呢？？？？
			}else if operator=="eq"{
				conditionString = "1#"+rightArgForStr
			}else{

			}

		}
	}
	return conditionString

}

func ExpressionToString(conditions []Expression, tableName string )string{     //本身好像只支持<的小于
	conditionString := ""
	flag:=1   //this is for specical case constant 1
	for i,oneCondition:=range conditions{
		if i>0 && flag == 1 {
			conditionString += "and "
		}
		//if it is a scalarfunction
		tryFunction,ok := oneCondition.(*ScalarFunction)   //a=1这种，就不会用函数表达式了，常量传播我们不下推了，因为不好做成where的条件
		if ok {
			if i>0{
				flag=1
			}
			//tryFunction.FuncName.L
			args:=tryFunction.Function.getArgs()
			arg1 := args[0]
			arg1Fi,_ := arg1.(*Column)
			leftArg:=arg1Fi.ColName.L
			arg2 := args[1].(*Constant)
			//rightArg:=arg2.Value.GetInt64()
			////////////////////////////////这边说是要把，constant中的value值转换导出，因为本身是小写可能无法导出
			value2 := arg2.Value
			var rightArgForInt int64
			var rightArgForStr string
			var rightArgForFloat float64
			which := -1
			switch value2.Kind() {
			case types.KindInt64:
				rightArgForInt = value2.GetInt64()
				which=1
			case types.KindString:
				rightArgForStr = value2.GetString()
				which=2
			case types.KindFloat64:
				rightArgForFloat = value2.GetFloat64()
				which=3
			}
			log.Println(which)
			/////////////////////////////////end
			operateor := tryFunction.FuncName.L
			if operateor=="lt" { //less than
				//conditionString += string(leftArg)+"<"+string(rightArg)
				//conditionString += string(leftArg)+"<"  谓词下推，表名也必须要加入
				conditionString += tableName+"."+string(leftArg)+"<"
				/////////////////////////////////start
				switch which {
				case 1:
					conditionString += strconv.FormatInt(rightArgForInt, 10)
				case 2:
					conditionString += rightArgForStr             //这里不用封装成postgreSQL的语法嘛
				case 3:
					conditionString += strconv.FormatFloat(rightArgForFloat, 'E', -1, 64)
				}

			}else if operateor == "eq" {  //equal
				//conditionString += string(leftArg)+"="
				conditionString += tableName+"."+string(leftArg)+"="
				switch which {
				case 1:
					conditionString += strconv.FormatInt(rightArgForInt, 10)
				case 2:
					conditionString += "'"+rightArgForStr+"'"    //这是PostgreSQL的语法
				case 3:
					conditionString += strconv.FormatFloat(rightArgForFloat, 'E', -1, 64)
				}
			}else if operateor=="gt"{    //greater than
				//conditionString += string(leftArg)+">"
				conditionString += tableName+"."+string(leftArg)+">"
				switch which {
				case 1:
					conditionString += strconv.FormatInt(rightArgForInt, 10)
				case 2:
					conditionString += "'"+rightArgForStr+"'"    //这是PostgreSQL的语法
				case 3:
					conditionString += strconv.FormatFloat(rightArgForFloat, 'E', -1, 64)
				}
			}
			log.Println(conditionString)
		}else{
			if i==0{
				flag=0
			}
		}
	}
	return conditionString
}

/*
 * 这是我们hack的代码
 */
func JoinEqExpressionToString(conditions []*ScalarFunction, ltablename, rtablename string)string{
	//现在我们仅仅考虑，这里只有一个等值的条件
	condition := conditions[0]
	args := condition.GetArgs()    //等到函数的参数，join中的=，也是用一个表达式函数来表示的
	arg1 := args[0]
	arg1Col, _ := arg1.(*Column)   //这里终于能理解，将列对象化的好处，可以用来验证
	leftArg := ltablename+"."+arg1Col.ColName.L
	arg2Col, _ := args[1].(*Column)
	rightArg := rtablename+"."+arg2Col.ColName.L
	log.Println("join condition = ,", leftArg+"="+rightArg)
	return leftArg+"="+rightArg
}

// ExpressionsToPB converts expression to tipb.Expr.
func ExpressionsToPB(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbCNF *tipb.Expr, pushed []Expression, remained []Expression) {
	pc := PbConverter{client: client, sc: sc}
	retTypeOfAnd := &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    1,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}

	for _, expr := range exprs {
		pbExpr := pc.ExprToPB(expr)
		if pbExpr == nil {
			remained = append(remained, expr)
			continue
		}

		pushed = append(pushed, expr)
		if pbCNF == nil {
			pbCNF = pbExpr
			continue
		}

		// Merge multiple converted pb expression into a CNF.
		pbCNF = &tipb.Expr{
			Tp:        tipb.ExprType_ScalarFunc,
			Sig:       tipb.ScalarFuncSig_LogicalAnd,
			Children:  []*tipb.Expr{pbCNF, pbExpr},
			FieldType: ToPBFieldType(retTypeOfAnd),
		}
	}
	return
}

// ExpressionsToPBList converts expressions to tipb.Expr list for new plan.
func ExpressionsToPBList(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbExpr []*tipb.Expr) {
	pc := PbConverter{client: client, sc: sc}
	for _, expr := range exprs {
		v := pc.ExprToPB(expr)
		pbExpr = append(pbExpr, v)
	}
	return
}

// PbConverter supplys methods to convert TiDB expressions to TiPB.
type PbConverter struct {
	client kv.Client
	sc     *stmtctx.StatementContext
}

// NewPBConverter creates a PbConverter.
func NewPBConverter(client kv.Client, sc *stmtctx.StatementContext) PbConverter {
	return PbConverter{client: client, sc: sc}
}

// ExprToPB converts Expression to TiPB.
func (pc PbConverter) ExprToPB(expr Expression) *tipb.Expr {
	switch x := expr.(type) {
	case *Constant, *CorrelatedColumn:
		return pc.conOrCorColToPBExpr(expr)
	case *Column:
		return pc.columnToPBExpr(x)
	case *ScalarFunction:
		return pc.scalarFuncToPBExpr(x)
	}
	return nil
}

func (pc PbConverter) conOrCorColToPBExpr(expr Expression) *tipb.Expr {
	ft := expr.GetType()
	d, err := expr.Eval(chunk.Row{})
	if err != nil {
		log.Errorf("Fail to eval constant, err: %s", err.Error())
		return nil
	}
	tp, val, ok := pc.encodeDatum(d)
	if !ok {
		return nil
	}

	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp, Val: val, FieldType: ToPBFieldType(ft)}
}

func (pc *PbConverter) encodeDatum(d types.Datum) (tipb.ExprType, []byte, bool) {
	var (
		tp  tipb.ExprType
		val []byte
	)
	switch d.Kind() {
	case types.KindNull:
		tp = tipb.ExprType_Null
	case types.KindInt64:
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString, types.KindBinaryLiteral:
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindBytes:
		tp = tipb.ExprType_Bytes
		val = d.GetBytes()
	case types.KindFloat32:
		tp = tipb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		tp = tipb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		tp = tipb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		tp = tipb.ExprType_MysqlDecimal
		var err error
		val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil {
			log.Errorf("Fail to encode value, err: %s", err.Error())
			return tp, nil, false
		}
	case types.KindMysqlTime:
		if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, int64(tipb.ExprType_MysqlTime)) {
			tp = tipb.ExprType_MysqlTime
			loc := pc.sc.TimeZone
			t := d.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp && loc != time.UTC {
				err := t.ConvertTimeZone(loc, time.UTC)
				terror.Log(errors.Trace(err))
			}
			v, err := t.ToPackedUint()
			if err != nil {
				log.Errorf("Fail to encode value, err: %s", err.Error())
				return tp, nil, false
			}
			val = codec.EncodeUint(nil, v)
			return tp, val, true
		}
		return tp, nil, false
	default:
		return tp, nil, false
	}
	return tp, val, true
}

// ToPBFieldType converts *types.FieldType to *tipb.FieldType.
func ToPBFieldType(ft *types.FieldType) *tipb.FieldType {
	return &tipb.FieldType{
		Tp:      int32(ft.Tp),
		Flag:    uint32(ft.Flag),
		Flen:    int32(ft.Flen),
		Decimal: int32(ft.Decimal),
		Charset: ft.Charset,
		Collate: collationToProto(ft.Collate),
	}
}

func collationToProto(c string) int32 {
	v, ok := mysql.CollationNames[c]
	if ok {
		return int32(v)
	}
	return int32(mysql.DefaultCollationID)
}

func (pc PbConverter) columnToPBExpr(column *Column) *tipb.Expr {
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	switch column.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry, mysql.TypeUnspecified:
		return nil
	}

	if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, kv.ReqSubTypeBasic) {
		return &tipb.Expr{
			Tp:        tipb.ExprType_ColumnRef,
			Val:       codec.EncodeInt(nil, int64(column.Index)),
			FieldType: ToPBFieldType(column.RetType),
		}
	}
	id := column.ID
	// Zero Column ID is not a column from table, can not support for now.
	if id == 0 || id == -1 {
		return nil
	}

	return &tipb.Expr{
		Tp:  tipb.ExprType_ColumnRef,
		Val: codec.EncodeInt(nil, id)}
}

func (pc PbConverter) scalarFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	// check whether this function can be pushed.
	if !pc.canFuncBePushed(expr) {
		return nil
	}

	// check whether this function has ProtoBuf signature.
	pbCode := expr.Function.PbCode()
	if pbCode < 0 {
		return nil
	}

	// check whether all of its parameters can be pushed.
	children := make([]*tipb.Expr, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}

	// construct expression ProtoBuf.
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Sig:       pbCode,
		Children:  children,
		FieldType: ToPBFieldType(expr.RetType),
	}
}

// GroupByItemToPB converts group by items to pb.
func GroupByItemToPB(sc *stmtctx.StatementContext, client kv.Client, expr Expression) *tipb.ByItem {
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e}
}

// SortByItemToPB converts order by items to pb.
func SortByItemToPB(sc *stmtctx.StatementContext, client kv.Client, expr Expression, desc bool) *tipb.ByItem {
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e, Desc: desc}
}

func (pc PbConverter) canFuncBePushed(sf *ScalarFunction) bool {
	switch sf.FuncName.L {
	case
		// logical functions.
		ast.LogicAnd,
		ast.LogicOr,
		ast.UnaryNot,

		// compare functions.
		ast.LT,
		ast.LE,
		ast.EQ,
		ast.NE,
		ast.GE,
		ast.GT,
		ast.NullEQ,
		ast.In,
		ast.IsNull,
		ast.Like,
		ast.IsTruth,
		ast.IsFalsity,

		// arithmetical functions.
		ast.Plus,
		ast.Minus,
		ast.Mul,
		ast.Div,
		ast.Abs,
		ast.Ceil,
		ast.Ceiling,
		ast.Floor,

		// control flow functions.
		ast.Case,
		ast.If,
		ast.Ifnull,
		ast.Coalesce,

		// json functions.
		ast.JSONType,
		ast.JSONExtract,
		ast.JSONUnquote,
		ast.JSONObject,
		ast.JSONArray,
		ast.JSONMerge,
		ast.JSONSet,
		ast.JSONInsert,
		ast.JSONReplace,
		ast.JSONRemove,

		// date functions.
		ast.DateFormat:

		return true
	}
	return false
}
