package myhashing

import (
	"hash/fnv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/recordkey"
)

func HashString(key recordkey.FruitKey, options uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key.FruitName))
	return (h.Sum32() + key.ClientID) % options
}
