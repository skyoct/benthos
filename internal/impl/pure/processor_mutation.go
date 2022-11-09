package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor(
		"mutation",
		service.NewConfigSpec().
			Stable().
			Version("4.5.0").
			Categories("Mapping", "Parsing").
			Field(service.NewBloblangField("")).
			Summary("Executes a [Bloblang](/docs/guides/bloblang/about) mapping and directly transforms the contents of messages, mutating (or deleting) them.").
			Description(`
Bloblang is a powerful language that enables a wide range of mapping, transformation and filtering tasks. For more information [check out the docs](/docs/guides/bloblang/about).

If your mapping is large and you'd prefer for it to live in a separate file then you can execute a mapping directly from a file with the expression `+"`from \"<path>\"`"+`, where the path must be absolute, or relative from the location that Benthos is executed from.

## Input Document Mutability

A mutation is a mapping that transforms input documents directly, this has the advantage of reducing the need to copy the data fed into the mapping. However, this also means that the referenced document is mutable and therefore changes throughout the mapping. For example, with the following Bloblang:

`+"```coffee"+`
root.rejected = this.invitees.filter(i -> i.mood < 0.5)
root.invitees = this.invitees.filter(i -> i.mood >= 0.5)
`+"```"+`

Notice that we create a field `+"`rejected`"+` by copying the array field `+"`invitees`"+` and filtering out objects with a high mood. We then overwrite the field `+"`invitees`"+` by filtering out objects with a low mood, resulting in two array fields that are each a subset of the original. If we were to reverse the ordering of these assignments like so:

`+"```coffee"+`
root.invitees = this.invitees.filter(i -> i.mood >= 0.5)
root.rejected = this.invitees.filter(i -> i.mood < 0.5)
`+"```"+`

Then the new field `+"`rejected`"+` would be empty as we have already mutated `+"`invitees`"+` to exclude the objects that it would be populated by. We can solve this problem either by carefully ordering our assignments or by capturing the original array using a variable (`+"`let invitees = this.invitees`"+`).

Mutations are advantageous over a standard mapping in situations where the result is a document with mostly the same shape as the input document, since we can avoid unnecessarily copying data from the referenced input document. However, in situations where we are creating an entirely new document shape it can be more convenient to use the traditional `+"[`mapping` processor](/docs/components/processors/mapping)"+` instead.

## Error Handling

Bloblang mappings can fail, in which case the error is logged and the message is flagged as having failed, allowing you to use [standard processor error handling patterns](/docs/configuration/error_handling).

However, Bloblang itself also provides powerful ways of ensuring your mappings do not fail by specifying desired fallback behaviour, which you can read about [in this section](/docs/guides/bloblang/about#error-handling).
			`).
			Example("Mapping", `
Given JSON documents containing an array of fans:

`+"```json"+`
{
  "id":"foo",
  "description":"a show about foo",
  "fans":[
    {"name":"bev","obsession":0.57},
    {"name":"grace","obsession":0.21},
    {"name":"ali","obsession":0.89},
    {"name":"vic","obsession":0.43}
  ]
}
`+"```"+`

We can reduce the documents down to just the ID and only those fans with an obsession score above 0.5, giving us:

`+"```json"+`
{
  "id":"foo",
  "fans":[
    {"name":"bev","obsession":0.57},
    {"name":"ali","obsession":0.89}
  ]
}
`+"```"+`

With the following config:`,
				`
pipeline:
  processors:
    - mutation: |
        root.description = deleted()
        root.fans = this.fans.filter(fan -> fan.obsession > 0.5)
`).
			Example("More Mapping", `
When receiving JSON documents of the form:

`+"```json"+`
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
`+"```"+`

We could collapse the location names from the state of Washington into a field `+"`Cities`"+`:

`+"```json"+`
{"Cities": "Bellevue, Olympia, Seattle"}
`+"```"+`

With the following config:`,
				`
pipeline:
  processors:
    - mutation: |
        root.Cities = this.locations.
                        filter(loc -> loc.state == "WA").
                        map_each(loc -> loc.name).
                        sort().join(", ")
`),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			mapping, err := conf.FieldBloblang()
			if err != nil {
				return nil, err
			}
			return newMutation(mapping, mgr.Logger()), nil
		})
	if err != nil {
		panic(err)
	}
}

type mutationProc struct {
	exec *bloblang.Executor
	log  *service.Logger
}

func newMutation(exec *bloblang.Executor, log *service.Logger) service.BatchProcessor {
	return &mutationProc{
		exec: exec,
		log:  log,
	}
}

func (m *mutationProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	newBatch := make(service.MessageBatch, 0, len(batch))
	for i, msg := range batch {
		newPart, err := batch.BloblangMutate(i, m.exec)
		if err != nil {
			m.log.Error(err.Error())
			msg.SetError(err)
			newBatch = append(newBatch, msg)
			continue
		}
		if newPart != nil {
			newBatch = append(newBatch, newPart)
		}
	}
	if len(newBatch) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{newBatch}, nil
}

func (m *mutationProc) Close(context.Context) error {
	return nil
}
