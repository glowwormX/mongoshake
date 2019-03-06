package oplog

import (
	"github.com/vinllen/mgo/bson"

	LOG "github.com/vinllen/log4go"
)

type GenericOplog struct {
	Raw    []byte
	Parsed *PartialLog
}

type PartialLog struct {
	Timestamp     bson.MongoTimestamp `bson:"ts"`
	Operation     string              `bson:"op"`
	Gid           string              `bson:"g"`
	Namespace     string              `bson:"ns"`
	Object        bson.M              `bson:"o"`
	Query         bson.M              `bson:"o2"`
	UniqueIndexes bson.M              `bson:"uk"`

	/*
	 * Every field subsequent declared is NEVER persistent or
	 * transfer on network connection. They only be parsed from
	 * respective logic
	 */
	UniqueIndexesUpdates bson.M // generate by CollisionMatrix
	RawSize              int    // generate by Decorator
	SourceId             int    // generate by Validator
}

func LogEntryEncode(logs []*GenericOplog) [][]byte {
	var encodedLogs [][]byte
	// log entry encode
	for _, log := range logs {
		encodedLogs = append(encodedLogs, log.Raw)
	}
	return encodedLogs
}

func LogParsed(logs []*GenericOplog) []*PartialLog {
	parsedLogs := make([]*PartialLog, len(logs), len(logs))
	for i, log := range logs {
		parsedLogs[i] = log.Parsed
		LOG.Debug("Pased oplog,then send to writer %v", parsedLogs[i])
	}
	return parsedLogs
}
