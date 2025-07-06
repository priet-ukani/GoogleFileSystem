
# GFS Configuration

# Master Server Configuration
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 50052
METADATA_STORE = "gfs_metadata.db"
OPERATION_LOG = "gfs_op.log"
LEASE_TIME_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 10
REPLICATION_FACTOR = 1

# Chunk Server Configuration
CHUNK_SIZE_BYTES = 64 * 1024  # 64 KB

# Client Configuration
CLIENT_CHUNK_CACHE_TTL_SECONDS = 60
