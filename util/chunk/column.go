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

package chunk

import (
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (c *column) appendDuration(dur types.Duration) {
	c.appendInt64(int64(dur.Duration))
}

func (c *column) appendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

func (c *column) appendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

type column struct {      //apache arrow storage
	length     int        //这个length是字节长度，所以length>>3就是获取，type的id
	nullCount  int
	nullBitmap []byte
	offsets    []int32
	data       []byte
	elemBuf    []byte
}

func (c *column) isFixed() bool {
	return c.elemBuf != nil
}

func (c *column) reset() {
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

func (c *column) isNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *column) appendNullBitmap(notNull bool) {
	idx := c.length >> 3                //获取新type中的下标
	if idx >= len(c.nullBitmap) {
		c.nullBitmap = append(c.nullBitmap, 0)   //字节不够加个字节
	}
	if notNull {                             //字节够的话，就在后面直接该动位置
		pos := uint(c.length) & 7            //提取length的最后三位
		c.nullBitmap[idx] |= byte(1 << pos)  //当前的这个bit位置应该放在那里，因为不一定是新的
		                                     //如果是0，说明当前byte是新的，自己可以放到byte右边的第一位
		                                     //如果是7，说明byte还是高位第8位是空的，需要自己放过去，所以移位
		                                     //如果为空的话，根本可以就不用动，因为默认就是0
	} else {
		c.nullCount++
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *column) appendMultiSameNullBitmap(notNull bool, num int) {
	numNewBytes := ((c.length + num + 7) >> 3) - len(c.nullBitmap)
	b := byte(0)
	if notNull {
		b = 0xff
	}
	for i := 0; i < numNewBytes; i++ {
		c.nullBitmap = append(c.nullBitmap, b)
	}
	if !notNull {
		c.nullCount += num
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	numRemainingBits := uint(c.length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.nullBitmap[c.length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.nullBitmap)*8 - c.length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.nullBitmap[len(c.nullBitmap)-1] &= bitMask
}

func (c *column) appendNull() {
	c.appendNullBitmap(false)
	if c.isFixed() {
		c.data = append(c.data, c.elemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.length])
	}
	c.length++
}

func (c *column) finishAppendFixed() {
	c.data = append(c.data, c.elemBuf...) //将这个值，append到data中去
	c.appendNullBitmap(true)      //append 具体的值的话，当然notnull
	c.length++                           //length++
}

func (c *column) appendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i    //问题为什么这个没有记录offset 看下面推断！！
	c.finishAppendFixed()
}

func (c *column) appendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

func (c *column) appendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *column) appendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *column) finishAppendVar() {    //一旦有string，bytes，time这种不定长的，需要记录偏移
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int32(len(c.data)))   //记录一下它的后续offset，取到为止
	c.length++
}

func (c *column) appendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

func (c *column) appendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

func (c *column) appendTime(t types.Time) {
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}


//可能的结果就是，使用的时候，根据col模式，将数据从data中取出，get int64就拿8个字节，get int32就拿
//4个字节，get string的时候，需要先看bitmap是否为空，然后需要到offset数组中，取到当前string/bytes
//的下标，找到string的下界。
