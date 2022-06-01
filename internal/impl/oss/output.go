package oss

import (
	"bytes"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

import (
	"context"
)

func cosOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Sends message parts as files to a Oss.").
		Description(``).
		Field(service.NewStringField("endpoint").Description("Endpoint corresponding to bucket.")).
		Field(service.NewStringField("bucket").Description("Bucket name")).
		Field(service.NewStringField("secret_id").Description("User's Secret ID.")).
		Field(service.NewStringField("secret_key").Description("User's Secret key.")).
		Field(service.NewInterpolatedStringField("directory").Description("A directory to store message files within. If the directory does not exist it will be created.")).
		Field(service.NewInterpolatedStringField("path").Description("The path of each message to upload.")).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of inserts to run in parallel.").
			Default(64))
	spec = spec.Field(service.NewBatchPolicyField("batching")).
		Version("3.65.0").
		Example("file to cos",
			`Here we send data to COS in batches`,
			`
output:
  cos:
    url: https://xxxxxxx.cos.ap-beijing.myqcloud.com
    secret_id: xxxxxxxxxxxxxx
    secret_key: xxxxxxxxxxxxxx
    directory: /usr/hive/warehouse/test.db/test_topic_02/ds=${!now().format_timestamp("2006-01-02")}/hr=${!now().format_timestamp("15")}/
    path: benthos-${!count("files")}-${!timestamp_unix_nano()}.txt
    max_in_flight: 64
    batching:
      count: 100
      byte_size: 0
      period: ""
      check: ""
`,
		)
	return spec
}

func init() {
	service.RegisterBatchOutput("cos", cosOutputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
		if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
			return
		}
		if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
			return
		}
		out, err = newCosOutputFromConfig(conf, mgr.Logger())
		return
	})
}

func newCosOutputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (o *oosOutput, err error) {
	o = &oosOutput{}
	o.logger = logger
	if o.endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if o.bucketName, err = conf.FieldString("bucket_name"); err != nil {
		return nil, err
	}
	if o.secretId, err = conf.FieldString("secret_id"); err != nil {
		return nil, err
	}
	if o.secretKey, err = conf.FieldString("secret_key"); err != nil {
		return nil, err
	}
	if o.directory, err = conf.FieldInterpolatedString("directory"); err != nil {
		return nil, err
	}
	if o.path, err = conf.FieldInterpolatedString("path"); err != nil {
		return nil, err
	}
	return
}

type oosOutput struct {
	endpoint   string
	bucketName string
	secretId   string
	secretKey  string

	directory *service.InterpolatedString
	path      *service.InterpolatedString

	bucket *oss.Bucket

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func (o *oosOutput) Connect(ctx context.Context) error {
	client, err := oss.New(o.endpoint, o.secretId, o.secretKey)
	if err != nil {
		return err
	}
	o.bucket, err = client.Bucket(o.bucketName)
	if err != nil {
		return err
	}
	return nil
}

func (o *oosOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for _, msg := range batch {
		data, err := msg.AsBytes()
		if err != nil {
			return err
		}
		key := o.directory.String(msg) + o.path.String(msg)
		err = o.bucket.PutObject(key, bytes.NewReader(data))
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *oosOutput) Close(ctx context.Context) error {
	return nil
}
