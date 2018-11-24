package conf

type Configuration struct {
	Tunnel        string `config:"tunnel"`
	TunnelAddress string `config:"tunnel.address"`
	SystemProfile int    `config:"system_profile"`
	LogLevel      string `config:"log_level"`
	LogFileName   string `config:"log_file"`
	LogBuffer     bool   `config:"log_buffer"`
	ReplayerNum   int    `config:"replayer"`

	ReplayerDMLOnly                   bool   `config:"replayer.dml_only"`
	ReplayerExecutor                  int    `config:"replayer.executor"`
	ReplayerExecutorUpsert            bool   `config:"replayer.executor.upsert"`
	ReplayerExecutorInsertOnDupUpdate bool   `config:"replayer.executor.insert_on_dup_update"`
	ReplayerCollisionEnable           bool   `config:"replayer.collision_detection"`
	ReplayerConflictWriteTo           string `config:"replayer.conflict_write_to"`
	ReplayerDurable                   bool   `config:"replayer.durable"`
}

var Options Configuration
