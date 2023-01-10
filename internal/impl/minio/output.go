package minio

import (
	"bytes"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
		Field(service.NewStringField("bucket_name").Description("Bucket name")).
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
  minio:
    endpoint: xxxxx
    bucket: xxxx
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
	service.RegisterBatchOutput("minio", cosOutputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
		if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
			return
		}
		if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
			return
		}
		out, err = newMinioOutputFromConfig(conf, mgr.Logger())
		return
	})
}

func newMinioOutputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (m *minioOutput, err error) {
	m = &minioOutput{}
	m.logger = logger
	if m.endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if m.bucketName, err = conf.FieldString("bucket_name"); err != nil {
		return nil, err
	}
	if m.secretId, err = conf.FieldString("secret_id"); err != nil {
		return nil, err
	}
	if m.secretKey, err = conf.FieldString("secret_key"); err != nil {
		return nil, err
	}
	if m.directory, err = conf.FieldInterpolatedString("directory"); err != nil {
		return nil, err
	}
	if m.path, err = conf.FieldInterpolatedString("path"); err != nil {
		return nil, err
	}
	return
}

type minioOutput struct {
	endpoint   string
	bucketName string
	secretId   string
	secretKey  string

	directory *service.InterpolatedString
	path      *service.InterpolatedString

	client  *minio.Client
	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func (m *minioOutput) Connect(ctx context.Context) error {
	var err error
	m.client, err = minio.New(m.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(m.secretId, m.secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *minioOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for _, msg := range batch {
		data, err := msg.AsBytes()
		if err != nil {
			return err
		}
		key := m.directory.String(msg) + m.path.String(msg)
		_, err = m.client.PutObject(ctx, m.bucketName, key, bytes.NewReader(data), -1, minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *minioOutput) Close(ctx context.Context) error {
	return nil
}
