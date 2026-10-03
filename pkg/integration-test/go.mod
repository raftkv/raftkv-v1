module raftkv/integration-test

go 1.21

require (
	raftkv/pipeline-module v0.0.0
	raftkv/wal-sm4-module v0.0.0
)

replace raftkv/pipeline-module => ../pipeline-module

replace raftkv/wal-sm4-module => ../wal-sm4-module