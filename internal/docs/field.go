package docs

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// FieldType represents a field type.
type FieldType string

// ValueType variants.
var (
	FieldTypeString  FieldType = "string"
	FieldTypeInt     FieldType = "int"
	FieldTypeFloat   FieldType = "float"
	FieldTypeBool    FieldType = "bool"
	FieldTypeObject  FieldType = "object"
	FieldTypeUnknown FieldType = "unknown"

	// Core component types, only components that can be a child of another
	// component config are listed here.
	FieldTypeInput     FieldType = "input"
	FieldTypeBuffer    FieldType = "buffer"
	FieldTypeCache     FieldType = "cache"
	FieldTypeProcessor FieldType = "processor"
	FieldTypeRateLimit FieldType = "rate_limit"
	FieldTypeOutput    FieldType = "output"
	FieldTypeMetrics   FieldType = "metrics"
	FieldTypeTracer    FieldType = "tracer"
)

// IsCoreComponent returns the core component type of a field if applicable.
func (t FieldType) IsCoreComponent() (Type, bool) {
	switch t {
	case FieldTypeInput:
		return TypeInput, true
	case FieldTypeBuffer:
		return TypeBuffer, true
	case FieldTypeCache:
		return TypeCache, true
	case FieldTypeProcessor:
		return TypeProcessor, true
	case FieldTypeRateLimit:
		return TypeRateLimit, true
	case FieldTypeOutput:
		return TypeOutput, true
	case FieldTypeTracer:
		return TypeTracer, true
	case FieldTypeMetrics:
		return TypeMetrics, true
	}
	return "", false
}

// FieldKind represents a field kind.
type FieldKind string

// ValueType variants.
var (
	KindScalar  FieldKind = "scalar"
	KindArray   FieldKind = "array"
	Kind2DArray FieldKind = "2darray"
	KindMap     FieldKind = "map"
)

//------------------------------------------------------------------------------

// FieldSpec describes a component config field.
type FieldSpec struct {
	// Name of the field (as it appears in config).
	Name string `json:"name"`

	// Type of the field.
	Type FieldType `json:"type"`

	// Kind of the field.
	Kind FieldKind `json:"kind"`

	// Description of the field purpose (in markdown).
	Description string `json:"description,omitempty"`

	// IsAdvanced is true for optional fields that will not be present in most
	// configs.
	IsAdvanced bool `json:"is_advanced,omitempty"`

	// IsDeprecated is true for fields that are deprecated and only exist
	// for backwards compatibility reasons.
	IsDeprecated bool `json:"is_deprecated,omitempty"`

	// IsOptional is a boolean flag indicating that a field is optional, even
	// if there is no default. This prevents linting errors when the field
	// is missing.
	IsOptional bool `json:"is_optional,omitempty"`

	// IsSecret indicates whether the field represents information that is
	// generally considered sensitive such as passwords or access tokens.
	IsSecret bool `json:"is_secret,omitempty"`

	// Default value of the field.
	Default *any `json:"default,omitempty"`

	// Interpolation indicates that the field supports interpolation
	// functions.
	Interpolated bool `json:"interpolated,omitempty"`

	// Bloblang indicates that a string field is a Bloblang mapping.
	Bloblang bool `json:"bloblang,omitempty"`

	// Examples is a slice of optional example values for a field.
	Examples []any `json:"examples,omitempty"`

	// AnnotatedOptions for this field. Each option should have a summary.
	AnnotatedOptions [][2]string `json:"annotated_options,omitempty"`

	// Options for this field.
	Options []string `json:"options,omitempty"`

	// Children fields of this field (it must be an object).
	Children FieldSpecs `json:"children,omitempty"`

	// Version is an explicit version when this field was introduced.
	Version string `json:"version,omitempty"`

	// Linter is a bloblang mapping that should be used in order to lint
	// a field.
	Linter string `json:"linter,omitempty"`

	omitWhenFn   func(field, parent any) (why string, shouldOmit bool)
	customLintFn LintFunc
}

// IsInterpolated indicates that the field supports interpolation functions.
func (f FieldSpec) IsInterpolated() FieldSpec {
	f.Interpolated = true
	return f
}

// IsBloblang indicates that the field is a Bloblang mapping.
func (f FieldSpec) IsBloblang() FieldSpec {
	f.Bloblang = true
	return f
}

// HasType returns a new FieldSpec that specifies a specific type.
func (f FieldSpec) HasType(t FieldType) FieldSpec {
	f.Type = t
	return f
}

// Optional marks this field as being optional, and therefore its absence in a
// config is not considered an error even when a default value is not provided.
func (f FieldSpec) Optional() FieldSpec {
	f.IsOptional = true
	return f
}

// Secret marks this field as being a secret, which means it represents
// information that is generally considered sensitive such as passwords or
// access tokens.
func (f FieldSpec) Secret() FieldSpec {
	f.IsSecret = true
	return f
}

// Advanced marks this field as being advanced, and therefore not commonly used.
func (f FieldSpec) Advanced() FieldSpec {
	f.IsAdvanced = true
	for i, v := range f.Children {
		f.Children[i] = v.Advanced()
	}
	return f
}

// Deprecated marks this field as being deprecated.
func (f FieldSpec) Deprecated() FieldSpec {
	f.IsDeprecated = true
	for i, v := range f.Children {
		f.Children[i] = v.Deprecated()
	}
	return f
}

// Array determines that this field is an array of the field type.
func (f FieldSpec) Array() FieldSpec {
	f.Kind = KindArray
	return f
}

// ArrayOfArrays determines that this is an array of arrays of the field type.
func (f FieldSpec) ArrayOfArrays() FieldSpec {
	f.Kind = Kind2DArray
	return f
}

// Map determines that this field is a map of arbitrary keys to a field type.
func (f FieldSpec) Map() FieldSpec {
	f.Kind = KindMap
	return f
}

// Scalar determines that this field is a scalar type (the default).
func (f FieldSpec) Scalar() FieldSpec {
	f.Kind = KindScalar
	return f
}

// HasDefault returns a new FieldSpec that specifies a default value.
func (f FieldSpec) HasDefault(v any) FieldSpec {
	f.Default = &v
	return f
}

// AtVersion specifies the version at which this fields behaviour was last
// modified.
func (f FieldSpec) AtVersion(v string) FieldSpec {
	f.Version = v
	return f
}

// HasAnnotatedOptions returns a new FieldSpec that specifies a specific list of
// annotated options. Either.
func (f FieldSpec) HasAnnotatedOptions(options ...string) FieldSpec {
	if len(f.Options) > 0 {
		panic("cannot combine annotated and non-annotated options for a field")
	}
	if len(options)%2 != 0 {
		panic("annotated field options must each have a summary")
	}
	for i := 0; i < len(options); i += 2 {
		f.AnnotatedOptions = append(f.AnnotatedOptions, [2]string{
			options[i], options[i+1],
		})
	}
	return f.lintOptions()
}

// HasOptions returns a new FieldSpec that specifies a specific list of options.
func (f FieldSpec) HasOptions(options ...string) FieldSpec {
	if len(f.AnnotatedOptions) > 0 {
		panic("cannot combine annotated and non-annotated options for a field")
	}
	f.Options = options
	return f.lintOptions()
}

// WithChildren returns a new FieldSpec that has child fields.
func (f FieldSpec) WithChildren(children ...FieldSpec) FieldSpec {
	if len(f.Type) == 0 {
		f.Type = FieldTypeObject
	}
	if f.IsAdvanced {
		for i, v := range children {
			children[i] = v.Advanced()
		}
	}
	f.Children = append(f.Children, children...)
	return f
}

// OmitWhen specifies a custom func that, when provided a generic config struct,
// returns a boolean indicating when the field can be safely omitted from a
// config.
func (f FieldSpec) OmitWhen(fn func(field, parent any) (why string, shouldOmit bool)) FieldSpec {
	f.omitWhenFn = fn
	return f
}

// LinterFunc adds a linting function to a field. When linting is performed on a
// config the provided function will be called with a boxed variant of the field
// value, allowing it to perform linting on that value.
//
// It is important to note that for fields defined as a non-scalar (array,
// array of arrays, map, etc) the linting rule will be executed on the highest
// level (array) and also the individual scalar values. If your field is a high
// level type then make sure your linting rule checks the type of the value
// provided in order to limit when the linting is performed.
//
// Note that a linting rule defined this way will only be effective in the
// binary that defines it as the function cannot be serialized into a portable
// schema.
func (f FieldSpec) LinterFunc(fn LintFunc) FieldSpec {
	f.customLintFn = fn
	return f
}

// LinterBlobl adds a linting function to a field. When linting is performed on
// a config the provided bloblang mapping will be called with a boxed variant of
// the field value, allowing it to perform linting on that value, where an array
// of lints (strings) should be returned.
//
// It is important to note that for fields defined as a non-scalar (array,
// array of arrays, map, etc) the linting rule will be executed on the highest
// level (array) and also the individual scalar values. If your field is a high
// level type then make sure your linting rule checks the type of the value
// provided in order to limit when the linting is performed.
//
// Note that a linting rule defined this way will only be effective in the
// binary that defines it as the function cannot be serialized into a portable
// schema.
func (f FieldSpec) LinterBlobl(blobl string) FieldSpec {
	env := bloblang.NewEnvironment().OnlyPure()

	m, err := env.NewMapping(blobl)
	if err != nil {
		f.customLintFn = func(ctx LintContext, line, col int, value any) (lints []Lint) {
			return []Lint{NewLintError(line, LintCustom, fmt.Sprintf("Field lint mapping itself failed to parse: %v", err))}
		}
		return f
	}

	f.Linter = blobl
	f.customLintFn = func(ctx LintContext, line, col int, value any) (lints []Lint) {
		res, err := m.Exec(query.FunctionContext{
			Vars:     map[string]any{},
			Maps:     map[string]query.Function{},
			MsgBatch: message.QuickBatch(nil),
		}.WithValue(value))
		if err != nil {
			return []Lint{NewLintError(line, LintCustom, err.Error())}
		}
		switch t := res.(type) {
		case []any:
			for _, e := range t {
				if what, _ := e.(string); len(what) > 0 {
					lints = append(lints, NewLintError(line, LintCustom, what))
				}
			}
		case string:
			if len(t) > 0 {
				lints = append(lints, NewLintError(line, LintCustom, t))
			}
		}
		return
	}
	return f
}

// lintOptions enforces that a field value matches one of the provided options
// and returns a linting error if that is not the case. This is currently opt-in
// because some fields express options that are only a subset due to deprecated
// functionality.
func (f FieldSpec) lintOptions() FieldSpec {
	f.customLintFn = func(ctx LintContext, line, col int, value any) []Lint {
		str, ok := value.(string)
		if !ok {
			return nil
		}
		if len(f.Options) > 0 {
			for _, optStr := range f.Options {
				if str == optStr {
					return nil
				}
			}
		} else {
			for _, optStr := range f.AnnotatedOptions {
				if str == optStr[0] {
					return nil
				}
			}
		}
		return []Lint{NewLintError(line, LintInvalidOption, fmt.Sprintf("value %v is not a valid option for this field", str))}
	}
	return f
}

func (f FieldSpec) getLintFunc() LintFunc {
	fn := f.customLintFn
	if fn == nil && len(f.Linter) > 0 {
		fn = f.LinterBlobl(f.Linter).customLintFn
	}
	if f.Interpolated {
		if fn != nil {
			fn = func(ctx LintContext, line, col int, value any) []Lint {
				lints := f.customLintFn(ctx, line, col, value)
				moreLints := LintBloblangField(ctx, line, col, value)
				return append(lints, moreLints...)
			}
		} else {
			fn = LintBloblangField
		}
	}
	if f.Bloblang {
		if fn != nil {
			fn = func(ctx LintContext, line, col int, value any) []Lint {
				lints := f.customLintFn(ctx, line, col, value)
				moreLints := LintBloblangMapping(ctx, line, col, value)
				return append(lints, moreLints...)
			}
		} else {
			fn = LintBloblangMapping
		}
	}
	return fn
}

// FieldAnything returns a field spec for any typed field.
func FieldAnything(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeUnknown)
}

// FieldObject returns a field spec for an object typed field.
func FieldObject(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeObject)
}

// FieldString returns a field spec for a common string typed field.
func FieldString(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeString)
}

// FieldInterpolatedString returns a field spec for a string typed field
// supporting dynamic interpolated functions.
func FieldInterpolatedString(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeString).IsInterpolated()
}

// FieldBloblang returns a field spec for a string typed field containing a
// Bloblang mapping.
func FieldBloblang(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeString).IsBloblang()
}

// FieldInt returns a field spec for a common int typed field.
func FieldInt(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeInt)
}

// FieldFloat returns a field spec for a common float typed field.
func FieldFloat(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeFloat)
}

// FieldBool returns a field spec for a common bool typed field.
func FieldBool(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeBool)
}

// FieldInput returns a field spec for an input typed field.
func FieldInput(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeInput)
}

// FieldProcessor returns a field spec for a processor typed field.
func FieldProcessor(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeProcessor)
}

// FieldOutput returns a field spec for an output typed field.
func FieldOutput(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeOutput)
}

// FieldBuffer returns a field spec for a buffer typed field.
func FieldBuffer(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeBuffer)
}

// FieldCache returns a field spec for a cache typed field.
func FieldCache(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeCache)
}

// FieldRateLimit returns a field spec for a rate limit typed field.
func FieldRateLimit(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeRateLimit)
}

// FieldMetrics returns a field spec for a metrics typed field.
func FieldMetrics(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeMetrics)
}

// FieldTracer returns a field spec for a tracer typed field.
func FieldTracer(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeTracer)
}

func newField(name, description string, examples ...any) FieldSpec {
	return FieldSpec{
		Name:        name,
		Description: description,
		Kind:        KindScalar,
		Examples:    examples,
	}
}

// FieldComponent returns a field spec for a component.
func FieldComponent() FieldSpec {
	return FieldSpec{
		Kind: KindScalar,
	}
}

// CheckRequired returns true if this field, due to various factors, is a field
// that must be specified within a config. The factors at play are:
//
// - Whether the field has a default value
// - Whether the field was explicitly marked as optional
// - Whether the field is an object with children, none of which are required.
func (f FieldSpec) CheckRequired() bool {
	if f.IsOptional {
		return false
	}
	if f.Default != nil {
		return false
	}
	if len(f.Children) == 0 {
		return true
	}

	// If none of the children are required then this field is not required.
	for _, child := range f.Children {
		if child.CheckRequired() {
			return true
		}
	}
	return false
}

//------------------------------------------------------------------------------

// FieldSpecs is a slice of field specs for a component.
type FieldSpecs []FieldSpec

// Merge with another set of FieldSpecs.
func (f FieldSpecs) Merge(specs FieldSpecs) FieldSpecs {
	return append(f, specs...)
}

// Add more field specs.
func (f FieldSpecs) Add(specs ...FieldSpec) FieldSpecs {
	return append(f, specs...)
}

// FieldFilter defines a filter closure that returns a boolean for a component
// field indicating whether the field should be kept within a generated config.
type FieldFilter func(spec FieldSpec) bool

func (f FieldFilter) shouldDrop(spec FieldSpec) bool {
	if f == nil {
		return false
	}
	return !f(spec)
}

// ShouldDropDeprecated returns a field filter that removes all deprecated
// fields when the boolean argument is true.
func ShouldDropDeprecated(b bool) FieldFilter {
	if !b {
		return nil
	}
	return func(spec FieldSpec) bool {
		return !spec.IsDeprecated
	}
}

//------------------------------------------------------------------------------

// LintContext is provided to linting functions, and provides context about the
// wider configuration.
type LintContext struct {
	// A map of label names to the line they were defined at.
	LabelsToLine map[string]int

	// Provides documentation for component implementations.
	DocsProvider Provider

	// Provides an isolated context for Bloblang parsing.
	BloblangEnv *bloblang.Environment

	// Config fields

	// Reject any deprecated components or fields as linting errors.
	RejectDeprecated bool

	// Require labels for components.
	RequireLabels bool
}

// NewLintContext creates a new linting context.
func NewLintContext() LintContext {
	return LintContext{
		LabelsToLine:     map[string]int{},
		DocsProvider:     DeprecatedProvider,
		BloblangEnv:      bloblang.GlobalEnvironment().Deactivated(),
		RejectDeprecated: false,
		RequireLabels:    false,
	}
}

// LintFunc is a common linting function for field values.
type LintFunc func(ctx LintContext, line, col int, value any) []Lint

// LintLevel describes the severity level of a linting error.
type LintLevel int

// Lint levels.
const (
	LintError   LintLevel = iota
	LintWarning LintLevel = iota
)

// LintType is a discrete linting type.
type LintType int

const (
	// LintCustom means a custom linting rule failed.
	LintCustom LintType = iota

	// LintFailedRead means a configuration could not be read.
	LintFailedRead LintType = iota

	// LintInvalidOption means the field value was not one of the explicit list
	// of options.
	LintInvalidOption LintType = iota

	// LintBadLabel means the label contains invalid characters.
	LintBadLabel LintType = iota

	// LintMissingLabel means the label is missing when required.
	LintMissingLabel LintType = iota

	// LintDuplicateLabel means the label collides with another label.
	LintDuplicateLabel LintType = iota

	// LintBadBloblang means the field contains invalid Bloblang.
	LintBadBloblang LintType = iota

	// LintShouldOmit means the field should be omitted.
	LintShouldOmit LintType = iota

	// LintComponentMissing means a component value was expected but the type is
	// missing.
	LintComponentMissing LintType = iota

	// LintComponentNotFound means the specified component value is not
	// recognised.
	LintComponentNotFound LintType = iota

	// LintUnknown means the field is unknown.
	LintUnknown LintType = iota

	// LintMissing means a field was required but missing.
	LintMissing LintType = iota

	// LintExpectedArray means an array value was expected but something else
	// was provided.
	LintExpectedArray LintType = iota

	// LintExpectedObject means an object value was expected but something else
	// was provided.
	LintExpectedObject LintType = iota

	// LintExpectedScalar means a scalar value was expected but something else
	// was provided.
	LintExpectedScalar LintType = iota

	// LintDeprecated means a field is deprecated and should not be used.
	LintDeprecated LintType = iota
)

// Lint describes a single linting issue found with a Benthos config.
type Lint struct {
	Line   int
	Column int // Optional, set to 1 by default
	Level  LintLevel
	Type   LintType
	What   string
}

// NewLintError returns an error lint.
func NewLintError(line int, t LintType, msg string) Lint {
	return Lint{Line: line, Column: 1, Level: LintError, Type: t, What: msg}
}

// NewLintWarning returns a warning lint.
func NewLintWarning(line int, t LintType, msg string) Lint {
	return Lint{Line: line, Column: 1, Level: LintWarning, Type: t, What: msg}
}

// Error returns a formatted string explaining the lint error prefixed with its
// location within the file.
func (l Lint) Error() string {
	return fmt.Sprintf("(%v,%v) %v", l.Line, l.Column, l.What)
}

//------------------------------------------------------------------------------

func (f FieldSpec) needsDefault() bool {
	if f.IsOptional {
		return false
	}
	if f.IsDeprecated {
		return false
	}
	return true
}

func getDefault(pathName string, field FieldSpec) (any, error) {
	if field.Default != nil {
		// TODO: Should be deep copy here?
		return *field.Default, nil
	} else if field.Kind == KindArray {
		return []any{}, nil
	} else if field.Kind == Kind2DArray {
		return []any{}, nil
	} else if field.Kind == KindMap {
		return map[string]any{}, nil
	} else if len(field.Children) > 0 {
		m := map[string]any{}
		for _, v := range field.Children {
			defV, err := getDefault(pathName+"."+v.Name, v)
			if err == nil {
				m[v.Name] = defV
			} else if v.needsDefault() {
				return nil, err
			}
		}
		return m, nil
	}
	return nil, fmt.Errorf("field '%v' is required and was not present in the config", pathName)
}
