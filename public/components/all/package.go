// Package all imports all component implementations that ship with the open
// source Benthos repo. This is a convenient way of importing every single
// connector at the cost of a larger dependency tree for your application.
package all

import (
	// Import all public sub-categories.
	_ "github.com/benthosdev/benthos/v4/public/components/amqp09"
	_ "github.com/benthosdev/benthos/v4/public/components/amqp1"
	_ "github.com/benthosdev/benthos/v4/public/components/avro"
	_ "github.com/benthosdev/benthos/v4/public/components/aws"
	_ "github.com/benthosdev/benthos/v4/public/components/azure"
	_ "github.com/benthosdev/benthos/v4/public/components/beanstalkd"
	_ "github.com/benthosdev/benthos/v4/public/components/cassandra"
	_ "github.com/benthosdev/benthos/v4/public/components/confluent"
	_ "github.com/benthosdev/benthos/v4/public/components/cos"
	_ "github.com/benthosdev/benthos/v4/public/components/dgraph"
	_ "github.com/benthosdev/benthos/v4/public/components/elasticsearch"
	_ "github.com/benthosdev/benthos/v4/public/components/gcp"
	_ "github.com/benthosdev/benthos/v4/public/components/hdfs"
	_ "github.com/benthosdev/benthos/v4/public/components/influxdb"
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/jaeger"
	_ "github.com/benthosdev/benthos/v4/public/components/kafka"
	_ "github.com/benthosdev/benthos/v4/public/components/maxmind"
	_ "github.com/benthosdev/benthos/v4/public/components/memcached"
	_ "github.com/benthosdev/benthos/v4/public/components/mongodb"
	_ "github.com/benthosdev/benthos/v4/public/components/mqtt"
	_ "github.com/benthosdev/benthos/v4/public/components/nanomsg"
	_ "github.com/benthosdev/benthos/v4/public/components/nats"
	_ "github.com/benthosdev/benthos/v4/public/components/nsq"
	_ "github.com/benthosdev/benthos/v4/public/components/otlp"
	_ "github.com/benthosdev/benthos/v4/public/components/prometheus"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/components/pure/extended"
	_ "github.com/benthosdev/benthos/v4/public/components/pusher"
	_ "github.com/benthosdev/benthos/v4/public/components/redis"
	_ "github.com/benthosdev/benthos/v4/public/components/sftp"
	_ "github.com/benthosdev/benthos/v4/public/components/snowflake"
	_ "github.com/benthosdev/benthos/v4/public/components/sql"
	_ "github.com/benthosdev/benthos/v4/public/components/statsd"
	_ "github.com/benthosdev/benthos/v4/public/components/wasm"
)
