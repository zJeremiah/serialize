package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	faker "github.com/bxcodec/faker/v3"
	h "github.com/dustin/go-humanize"
	jsoniter "github.com/json-iterator/go"
	goavro "github.com/linkedin/goavro/v2"
)

type Stats struct {
	System   string
	Records  int64
	DataSize int64 // The full size of the data processed in bytes

	MarshalAvg time.Duration
	MarshalAll time.Duration
	MarshalOpS int64 // operations per second

	UnMarshalAvg time.Duration
	UnMarshalAll time.Duration
	UnMarshalOpS int64 // operations per second
}

//proteus:generate
type Data struct {
	ID       int64             `json:"id"`
	Date     time.Time         `json:"date"`
	NameA    string            `json:"namea"`
	NameB    string            `json:"nameb"`
	NameC    string            `json:"namec"`
	Count1   int64             `json:"count1"`
	Count2   int64             `json:"count2"`
	Count3   int64             `json:"count3"`
	Amt1     float64           `json:"amt1"`
	Amt2     float64           `json:"amt2"`
	Flag     bool              `json:"flag"`
	StrArray []string          `json:"str_array"`
	MapStr   map[string]string `json:"map_string"`
	MapInt   map[int64]int64   `json:"map_int"`

	XXX_unrecognized []byte `proteus:"-" json:"-"`
}

var jsont = jsoniter.ConfigCompatibleWithStandardLibrary
var jsonf = jsoniter.ConfigFastest
var timer = time.Minute * 2
var data = []*Data{}

func main() {
	//ProtoTest() // I cannot seem to get the protobuf unmarshaling to work
	JsoniterStandardTest()
	JsonitterConfigFastestTest()
	StandardTest()
	AvroTest()
}

func init() {
	s := time.Now()
	for {
		d := &Data{}
		faker.SetRandomMapAndSliceSize(20)
		faker.FakeData(d)

		data = append(data, d)

		// stop loading test data after 15 seconds of data is stored
		if time.Now().Sub(s) > time.Second*15 {
			break
		}
	}

}

func ProtoTest() {
	var err error
	var b []byte
	var start, s, e time.Time // Start and End times

	c := time.Nanosecond // a counter (in nanoseconds)
	stat := Stats{
		System: "protobuf",
	}

	// testing loop jsoniter
	start = time.Now()
	for i, d := range data {
		c = c + time.Nanosecond

		// test Marshal time
		s = time.Now()
		b, err = d.Marshal()
		if err != nil {
			log.Fatalln("error marshal", stat.System, err)
		}
		e = time.Now()

		stat.DataSize = stat.DataSize + int64(len(b))
		stat.MarshalAll = stat.MarshalAll + e.Sub(s)
		stat.MarshalAvg = stat.MarshalAll / c

		// Test Unmarshal time
		s = time.Now()

		err = d.Unmarshal(b)
		if err != nil {
			log.Fatalln("error unmarshal", stat.System, err)
		}
		e = time.Now()

		stat.UnMarshalAll = stat.UnMarshalAll + e.Sub(s)
		stat.UnMarshalAvg = stat.UnMarshalAll / c

		// stop the test after a minute
		// or no more data to process
		if i == len(data) || time.Now().Sub(start) > timer {
			break
		}
	}

	stat.Records = c.Nanoseconds()
	stat.MarshalOpS = stat.Records / int64(stat.MarshalAll.Seconds())
	stat.UnMarshalOpS = stat.Records / int64(stat.UnMarshalAll.Seconds())
	PrintStats(stat)
}

func AvroTest() {
	var start, s, e time.Time // Start and End times

	codec, err := goavro.NewCodec(` 
	{
		"type" : "record",
		"name" : "data",
		"fields" : [
			{ "name":"id" ,       "type" : "int" },	 
			{ "name":"date" ,     "type" : "string" },
			{ "name":"namea",     "type" : "string" },
			{ "name":"nameb",     "type" : "string" },
			{ "name":"namec",     "type" : "string" },
			{ "name":"count1",    "type" : "int" },
			{ "name":"count2",    "type" : "int" },
			{ "name":"count3",    "type" : "int" },
			{ "name":"amt1",      "type" : "double" },
			{ "name":"amt2",      "type" : "double" },
			{ "name":"flag",      "type" : "boolean" },
			{ "name":"str_array", "type" : "array", "items":"string" },
			{ "name":"map_string","type" : "map", "values": "string" },
			{ "name":"map_int",   "type" : "map", "values": "int" }
		] }`)
	if err != nil {
		log.Fatalln("goavro newcodec error:", err)
	}

	stat := Stats{
		System: "avro binary",
	}

	c := time.Nanosecond // a counter (in nanoseconds)
	bmap := map[string]interface{}{}

	var b []byte
	start = time.Now()
	for i, d := range data {
		c = c + time.Nanosecond

		// this library will only use a map[string]interface{} to parse avro data
		b, _ = jsont.Marshal(d)
		jsont.Unmarshal(b, &bmap)

		s = time.Now()
		b, err = codec.BinaryFromNative(nil, bmap)
		if err != nil {
			log.Fatalln("binary_from_native error:", err)
		}
		e = time.Now()

		stat.DataSize = stat.DataSize + int64(len(b))
		stat.MarshalAll = stat.MarshalAll + e.Sub(s)
		stat.MarshalAvg = stat.MarshalAll / c

		s = time.Now()
		_, _, err = codec.NativeFromBinary(b)
		if err != nil {
			log.Fatalln("native_from_binary error:", err)
		}
		e = time.Now()

		stat.UnMarshalAll = stat.UnMarshalAll + e.Sub(s)
		stat.UnMarshalAvg = stat.UnMarshalAll / c

		// stop the test after a minute
		// or no more data to process
		if i == len(data) || time.Now().Sub(start) > timer {
			break
		}
	}
	stat.Records = c.Nanoseconds()
	stat.MarshalOpS = stat.Records / int64(stat.MarshalAll.Seconds())
	stat.UnMarshalOpS = stat.Records / int64(stat.UnMarshalAll.Seconds())
	PrintStats(stat)
}

func JsoniterStandardTest() {
	var err error
	var b []byte
	var start, s, e time.Time // Start and End times

	c := time.Nanosecond // a counter (in nanoseconds)
	stat := Stats{
		System: "jsoniter config with standard lib",
	}

	// testing loop jsoniter
	start = time.Now()
	for i, d := range data {
		c = c + time.Nanosecond

		// test Marshal time
		s = time.Now()
		b, err = jsont.Marshal(d)
		if err != nil {
			log.Fatalln("error marshal", stat.System, err)
		}
		e = time.Now()

		stat.DataSize = stat.DataSize + int64(len(b))
		stat.MarshalAll = stat.MarshalAll + e.Sub(s)
		stat.MarshalAvg = stat.MarshalAll / c

		// Test Unmarshal time
		s = time.Now()
		err = jsont.Unmarshal(b, &d)
		if err != nil {
			log.Fatalln("error unmarshal", stat.System, err)
		}
		e = time.Now()

		stat.UnMarshalAll = stat.UnMarshalAll + e.Sub(s)
		stat.UnMarshalAvg = stat.UnMarshalAll / c

		// stop the test after a minute
		// or no more data to process
		if i == len(data) || time.Now().Sub(start) > timer {
			break
		}
	}

	stat.Records = c.Nanoseconds()
	stat.MarshalOpS = stat.Records / int64(stat.MarshalAll.Seconds())
	stat.UnMarshalOpS = stat.Records / int64(stat.UnMarshalAll.Seconds())
	PrintStats(stat)
}

func JsonitterConfigFastestTest() {
	var err error
	var b []byte
	var start, s, e time.Time // Start and End times

	c := time.Nanosecond // a counter (in nanoseconds)
	stat := Stats{
		System: "jsoniter config fastest",
	}

	// testing loop jsoniter
	start = time.Now()
	for i, d := range data {
		c = c + time.Nanosecond

		// test Marshal time
		s = time.Now()
		b, err = jsonf.Marshal(d)
		if err != nil {
			log.Fatalln("error marshal", stat.System, err)
		}
		e = time.Now()

		stat.DataSize = stat.DataSize + int64(len(b))
		stat.MarshalAll = stat.MarshalAll + e.Sub(s)
		stat.MarshalAvg = stat.MarshalAll / c

		// Test Unmarshal time
		s = time.Now()
		err = jsonf.Unmarshal(b, &d)
		if err != nil {
			log.Fatalln("error unmarshal", stat.System, err)
		}
		e = time.Now()

		stat.UnMarshalAll = stat.UnMarshalAll + e.Sub(s)
		stat.UnMarshalAvg = stat.UnMarshalAll / c

		// stop the test after a minute
		// or no more data to process
		if i == len(data) || time.Now().Sub(start) > timer {
			break
		}
	}

	stat.Records = c.Nanoseconds()
	stat.MarshalOpS = stat.Records / int64(stat.MarshalAll.Seconds())
	stat.UnMarshalOpS = stat.Records / int64(stat.UnMarshalAll.Seconds())
	PrintStats(stat)
}

func StandardTest() {
	var err error
	var b []byte
	var start, s, e time.Time // Start and End times

	c := time.Nanosecond // a counter (in nanoseconds)
	stat := Stats{
		System: "standard encoding/json",
	}

	// testing loop jsoniter
	start = time.Now()
	for i, d := range data {
		c = c + time.Nanosecond

		// test Marshal time
		s = time.Now()
		b, err = json.Marshal(d)
		if err != nil {
			log.Fatalln("error marshal", stat.System, err)
		}
		e = time.Now()

		stat.DataSize = stat.DataSize + int64(len(b))
		stat.MarshalAll = stat.MarshalAll + e.Sub(s)
		stat.MarshalAvg = stat.MarshalAll / c

		// Test Unmarshal time
		s = time.Now()
		err = json.Unmarshal(b, &d)
		if err != nil {
			log.Fatalln("error unmarshal", stat.System, err)
		}
		e = time.Now()

		stat.UnMarshalAll = stat.UnMarshalAll + e.Sub(s)
		stat.UnMarshalAvg = stat.UnMarshalAll / c

		// stop the test after a minute
		// or no more data to process
		if i == len(data) || time.Now().Sub(start) > timer {
			break
		}
	}

	stat.Records = c.Nanoseconds()
	stat.MarshalOpS = stat.Records / int64(stat.MarshalAll.Seconds())
	stat.UnMarshalOpS = stat.Records / int64(stat.UnMarshalAll.Seconds())
	PrintStats(stat)
}

func PrintStats(s Stats) {
	fmt.Printf("System: %s\n", s.System)
	fmt.Printf("\t %s Records Processed\n", h.Comma(s.Records))
	fmt.Printf("\t %s Total Bytes\n", h.Comma(s.DataSize))
	fmt.Println()
	fmt.Printf("\t %s UnMarshal Average\n", s.UnMarshalAvg.String())
	fmt.Printf("\t %s UnMarshal Total Time\n", s.UnMarshalAll.String())
	fmt.Printf("\t %s UnMarshal Op/sec\n", h.Comma(s.UnMarshalOpS))
	fmt.Println()
	fmt.Printf("\t %s Marshal Average\n", s.MarshalAvg.String())
	fmt.Printf("\t %s Marshal Total Time\n", s.MarshalAll.String())
	fmt.Printf("\t %s Marshal Op/sec\n", h.Comma(s.MarshalOpS))
	fmt.Println()
}
