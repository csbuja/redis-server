package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForgotPasswordGet(t *testing.T) {
	length, _ := generateEmptyRDB()
	i := calcRdbLength(length)

	assert.Greater(t, i, 0, "empty rdb files aren't of 0 length because of all of the metadata included")
}
