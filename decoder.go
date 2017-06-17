package avro

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/karrick/goavro"
)

// Decoder reads and decodes Avro content from an input stream.
type Decoder struct {
	err   error
	r     io.Reader
	codec *goavro.Codec
}

// Decode reads the next Avro record from an input stream and marshals into the provided struct
func (d *Decoder) Decode(v interface{}) error {
	if err := d.err; err != nil {
		return err
	}

	data, err := ioutil.ReadAll(d.r)
	if err != nil {
		return err
	}

	native, _, err := d.codec.NativeFromBinary(data)
	if err != nil {
		return err
	}

	data, err = json.Marshal(native)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}

// NewDecoder constructs a new Decoder that decodes an Avro record from the specified input stream
func NewDecoder(schema string, r io.Reader) *Decoder {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return &Decoder{err: err}
	}

	return &Decoder{
		r:     r,
		codec: codec,
	}
}
