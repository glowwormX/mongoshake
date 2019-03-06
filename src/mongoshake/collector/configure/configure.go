package conf

type Configuration struct {
	MongoUrls              []string `config:"mongo_urls"`
	CollectorId            string   `config:"collector.id"`
	CheckpointInterval     int64    `config:"checkpoint.interval"`
	HTTPListenPort         int      `config:"http_profile"`
	SystemProfile          int      `config:"system_profile"`
	LogLevel               string   `config:"log_level"`
	LogFileName            string   `config:"log_file"`
	LogBuffer              bool     `config:"log_buffer"`
	OplogGIDS              string   `config:"oplog.gids"`
	FilterGoTag            bool     `config:"oplog.filter_gotag"`
	ShardKey               string   `config:"shard_key"`
	SyncerReaderBufferTime uint     `config:"syncer.reader.buffer_time"`
	WorkerNum              int      `config:"worker"`
	WorkerOplogCompressor  string   `config:"worker.oplog_compressor"`
	WorkerBatchQueueSize   uint64   `config:"worker.batch_queue_size"`
	Tunnel                 string   `config:"tunnel"`
	TunnelAddress          []string `config:"tunnel.address"`
	MasterQuorum           bool     `config:"master_quorum"`
	ContextStorage         string   `config:"context.storage"`
	ContextStorageUrl      string   `config:"context.storage.url"`
	ContextAddress         string   `config:"context.address"`
	ContextStartPosition   int64    `config:"context.start_position" type:"date"`
	ContextStartUseLocal   bool     `config:"context.start_use_local"`
	FilterNamespaceBlack   []string `config:"filter.namespace.black"`
	FilterNamespaceWhite   []string `config:"filter.namespace.white"`

	ReplayerDMLOnly                   bool   `config:"replayer.dml_only"`
	ReplayerExecutor                  int    `config:"replayer.executor"`
	ReplayerExecutorUpsert            bool   `config:"replayer.executor.upsert"`
	ReplayerExecutorInsertOnDupUpdate bool   `config:"replayer.executor.insert_on_dup_update"`
	ReplayerCollisionEnable           bool   `config:"replayer.collision_detection"`
	ReplayerConflictWriteTo           string `config:"replayer.conflict_write_to"`
	ReplayerDurable                   bool   `config:"replayer.durable"`

	ReplayerNum         int      `config:"replayer"`
	TunnelWriter        string   `config:"tunnel.writer"`
	TunnelAddressWriter []string `config:"tunnel.writer.address"`

	CopyLogFileTime uint   `config:"tunnel.file.copy.time"`
	CopyLogFilePath string `config:"tunnel.file.copy.path"`
	ReadLogFileTime uint   `config:"tunnel.writer.file.read.time"`
	MongoDumpCmd    string `config:"mongo.dump.cmd"`
}

func (configuration *Configuration) IsShardCluster() bool {
	return len(configuration.MongoUrls) > 1
}

var Options Configuration
