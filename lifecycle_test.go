package avro_test

import (
	"bytes"
	"testing"

	"github.com/altairsix/avro"
	"github.com/stretchr/testify/assert"
)

const (
	schema = `{
  "type": "record",
  "name": "comments",
  "doc:": "A basic schema for storing blog comments",
  "namespace": "com.example",
  "fields": [
    {
      "name": "username",
      "type": "string",
      "doc": "Name of user"
    },
    {
      "name": "comment",
      "type": "string",
      "doc": "The content of the user's message"
    },
    {
      "name": "timestamp",
      "type": "long",
      "doc": "Unix epoch time in milliseconds"
    }
  ]
}`
)

type Sample struct {
	Username  string `json:"username,omitempty"`
	Comment   string `json:"comment,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

func TestEncodeVerifiesSchema(t *testing.T) {
	sample := &Sample{Username: "username"}
	err := avro.NewEncoder(schema, bytes.NewBuffer(nil)).Encode(sample)
	assert.NotNil(t, err)
}

func TestEncodeDecode(t *testing.T) {
	expected := &Sample{
		Username:  "username",
		Comment:   "comment",
		Timestamp: 123,
	}

	buf := bytes.NewBuffer(nil)
	err := avro.NewEncoder(schema, buf).Encode(expected)
	assert.Nil(t, err)
	assert.Equal(t, len(buf.Bytes()), 19)

	actual := &Sample{}
	err = avro.NewDecoder(schema, buf).Decode(actual)
	assert.Nil(t, err)

	assert.Equal(t, expected, actual)
}

func BenchmarkEncoder_Encode(b *testing.B) {
	sample := &Sample{
		Username:  "username",
		Comment:   "comment",
		Timestamp: 123,
	}

	buf := bytes.NewBuffer(nil)
	for i := 0; i < b.N; i++ {
		err := avro.NewEncoder(schema, buf).Encode(sample)
		assert.Nil(b, err)
		buf.Truncate(0)
	}
}
