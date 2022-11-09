package config

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/docs"
	ifilepath "github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/manager"
)

type resourceFileInfo struct {
	configFileInfo

	// Need to track the resource that came from the previous read as their
	// absence in an update means they need to be removed.
	inputs     map[string]*input.Config
	processors map[string]*processor.Config
	outputs    map[string]*output.Config
	caches     map[string]*cache.Config
	rateLimits map[string]*ratelimit.Config
}

func resInfoFromConfig(conf *manager.ResourceConfig) resourceFileInfo {
	resInfo := resourceFileInfo{
		inputs:     map[string]*input.Config{},
		processors: map[string]*processor.Config{},
		outputs:    map[string]*output.Config{},
		caches:     map[string]*cache.Config{},
		rateLimits: map[string]*ratelimit.Config{},
	}

	// This is an unlikely race condition, see readMain for more info.
	resInfo.updatedAt = time.Now()

	// New style
	for _, c := range conf.ResourceInputs {
		resInfo.inputs[c.Label] = &c
	}
	for _, c := range conf.ResourceProcessors {
		resInfo.processors[c.Label] = &c
	}
	for _, c := range conf.ResourceOutputs {
		resInfo.outputs[c.Label] = &c
	}
	for _, c := range conf.ResourceCaches {
		resInfo.caches[c.Label] = &c
	}
	for _, c := range conf.ResourceRateLimits {
		resInfo.rateLimits[c.Label] = &c
	}

	return resInfo
}

func (r *Reader) resourcePathsExpanded() ([]string, error) {
	resourcePaths, err := ifilepath.Globs(ifs.OS(), r.resourcePaths)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve resource glob pattern: %w", err)
	}
	return resourcePaths, nil
}

func (r *Reader) readResources(conf *manager.ResourceConfig) (lints []string, err error) {
	resourcesPaths, err := r.resourcePathsExpanded()
	if err != nil {
		return nil, err
	}
	for _, path := range resourcesPaths {
		rconf := manager.NewResourceConfig()
		var rLints []string
		if rLints, err = readResource(path, &rconf); err != nil {
			return
		}
		lints = append(lints, rLints...)

		if err = conf.AddFrom(&rconf); err != nil {
			err = fmt.Errorf("%v: %w", path, err)
			return
		}
		r.resourceFileInfo[filepath.Clean(path)] = resInfoFromConfig(&rconf)
	}
	return
}

func readResource(path string, conf *manager.ResourceConfig) (lints []string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%v: %w", path, err)
		}
	}()

	var confBytes []byte
	var dLints []docs.Lint
	if confBytes, dLints, err = ReadFileEnvSwap(path); err != nil {
		return
	}
	for _, l := range dLints {
		lints = append(lints, l.Error())
	}

	var rawNode yaml.Node
	if err = yaml.Unmarshal(confBytes, &rawNode); err != nil {
		return
	}
	if !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE")) {
		allowTest := append(docs.FieldSpecs{
			tdocs.ConfigSpec(),
		}, manager.Spec()...)
		for _, lint := range allowTest.LintYAML(docs.NewLintContext(), &rawNode) {
			lints = append(lints, fmt.Sprintf("%v%v", path, lint.Error()))
		}
	}

	err = rawNode.Decode(conf)
	return
}

func (r *Reader) reactResourceUpdate(mgr bundle.NewManagement, strict bool, path string) bool {
	r.resourceFileInfoMut.Lock()
	defer r.resourceFileInfoMut.Unlock()

	if _, exists := r.resourceFileInfo[path]; !exists {
		mgr.Logger().Warnf("Skipping resource update for unknown path: %v", path)
		return true
	}

	mgr.Logger().Infof("Resource %v config updated, attempting to update resources.", path)

	newResConf := manager.NewResourceConfig()
	lints, err := readResource(path, &newResConf)
	if err != nil {
		mgr.Logger().Errorf("Failed to read updated resources config: %v", err)
		return true
	}

	lintlog := mgr.Logger()
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}
	if strict && len(lints) > 0 {
		mgr.Logger().Errorln("Rejecting updated resource config due to linter errors, to allow linting errors run Benthos with --chilled")
		return true
	}

	// TODO: Should we error out if the new config is missing some resources?
	// (as they will continue to exist). Also, we could avoid restarting
	// resources where the config hasn't changed.

	newInfo := resInfoFromConfig(&newResConf)
	if !newInfo.applyChanges(mgr) {
		return false
	}

	r.resourceFileInfo[path] = newInfo
	return true
}

func (i *resourceFileInfo) applyChanges(mgr bundle.NewManagement) bool {
	// Kind of arbitrary, but I feel better about having some sort of timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// WARNING: The order here is actually kind of important, we want to start
	// with components that could be dependencies of other components. This is
	// a "best attempt", so not all edge cases need to be accounted for.
	for k, v := range i.rateLimits {
		if err := mgr.StoreRateLimit(ctx, k, *v); err != nil {
			mgr.Logger().Errorf("Failed to update resource %v: %v", k, err)
			return false
		}
		mgr.Logger().Infof("Updated resource %v config from file.", k)
	}
	for k, v := range i.caches {
		if err := mgr.StoreCache(ctx, k, *v); err != nil {
			mgr.Logger().Errorf("Failed to update resource %v: %v", k, err)
			return false
		}
		mgr.Logger().Infof("Updated resource %v config from file.", k)
	}
	for k, v := range i.processors {
		if err := mgr.StoreProcessor(ctx, k, *v); err != nil {
			mgr.Logger().Errorf("Failed to update resource %v: %v", k, err)
			return false
		}
		mgr.Logger().Infof("Updated resource %v config from file.", k)
	}
	for k, v := range i.inputs {
		if err := mgr.StoreInput(ctx, k, *v); err != nil {
			mgr.Logger().Errorf("Failed to update resource %v: %v", k, err)
			return false
		}
		mgr.Logger().Infof("Updated resource %v config from file.", k)
	}
	for k, v := range i.outputs {
		if err := mgr.StoreOutput(ctx, k, *v); err != nil {
			mgr.Logger().Errorf("Failed to update resource %v: %v", k, err)
			return false
		}
		mgr.Logger().Infof("Updated resource %v config from file.", k)
	}

	return true
}
