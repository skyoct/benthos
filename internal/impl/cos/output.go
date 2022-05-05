package cos

import (
	"bytes"
	"context"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/url"
)

func cosOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Sends message parts as files to a cos.").
		Description(``).
		Field(service.NewStringField("url")).
		Field(service.NewStringField("secret_id")).
		Field(service.NewStringField("secret_key")).
		Field(service.NewInterpolatedStringField("directory")).
		Field(service.NewInterpolatedStringField("path")).
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

func newCosOutputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (c *cosOutput, err error) {
	c = &cosOutput{}
	c.logger = logger
	if c.url, err = conf.FieldString("url"); err != nil {
		return nil, err
	}
	if c.secretId, err = conf.FieldString("secret_id"); err != nil {
		return nil, err
	}
	if c.secretKey, err = conf.FieldString("secret_key"); err != nil {
		return nil, err
	}
	if c.directory, err = conf.FieldInterpolatedString("directory"); err != nil {
		return nil, err
	}
	if c.path, err = conf.FieldInterpolatedString("path"); err != nil {
		return nil, err
	}
	return
}

type cosOutput struct {
	url       string
	secretId  string
	secretKey string

	directory *service.InterpolatedString
	path      *service.InterpolatedString

	client *cos.Client

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func (c *cosOutput) Connect(ctx context.Context) error {
	u, _ := url.Parse(c.url)
	b := &cos.BaseURL{BucketURL: u}
	c.client = cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.secretId,
			SecretKey: c.secretKey,
		},
	})
	return nil
}

func (c *cosOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	key := ""
	var data bytes.Buffer
	for _, msg := range batch {
		if key == "" {
			key = c.directory.String(msg) + c.path.String(msg)
		}
		content, err := msg.AsBytes()
		if err != nil {
			return err
		}
		data.Write(content)
		data.WriteString("\n")
	}
	dataBytes := data.Bytes()
	_, err := c.client.Object.Put(ctx, key, bytes.NewReader(dataBytes), nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *cosOutput) Close(ctx context.Context) error {
	return nil
}
