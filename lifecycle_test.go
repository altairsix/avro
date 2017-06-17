package avro_test

import (
	"bytes"
	"testing"

	"github.com/altairsix/avro"
	"github.com/stretchr/testify/assert"
)

type Sample struct {
	Username  string `json:"username"`
	Comment   string `json:"comment"`
	Timestamp int64  `json:"timestamp"`
}

func TestEncodeDecode(t *testing.T) {
	schema := `{
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

	expected := &Sample{
		Username:  "username",
		Comment:   "comment",
		Timestamp: 123,
	}

	buf := bytes.NewBuffer(nil)
	err := avro.NewEncoder(schema, buf).Encode(expected)
	assert.Nil(t, err)

	actual := &Sample{}
	err = avro.NewDecoder(schema, buf).Decode(actual)
	assert.Nil(t, err)

	assert.Equal(t, expected, actual)
}
