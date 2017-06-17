package avro

import (
	"encoding/json"
	"io"

	"github.com/karrick/goavro"
)

// Encoder write and encodes Avro content to an input stream.
type Encoder struct {
	err   error
	w     io.Writer
	codec *goavro.Codec
}

// Encode converts the provided struct to an Avro record and writes it to an input stream
func (e *Encoder) Encode(in interface{}) error {
	if err := e.err; err != nil {
		return err
	}

	data, err := json.Marshal(in)
	if err != nil {
		return err
	}

	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	data, err = e.codec.BinaryFromNative(nil, v)
	if err != nil {
		return err
	}

	if _, err := e.w.Write(data); err != nil {
		return err
	}

	return nil
}

// NewEncoder creates a new encoder that reads a struct, converts it to Avro, and writes it to the specified output
// stream
func NewEncoder(schema string, w io.Writer) *Encoder {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return &Encoder{err: err}
	}

	return &Encoder{
		w:     w,
		codec: codec,
	}
}
