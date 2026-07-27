package main

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"
)

func TestExtractDeclarationsRecognizesConstructorsAndSorts(t *testing.T) {
	t.Parallel()
	files := []sourceFile{
		{
			path: "z/config.go",
			body: []byte(`package other
import "go.temporal.io/server/common/dynamicconfig"
var z = dynamicconfig.NewNamespaceBoolSetting("z.key", true, "z")
`),
		},
		{
			path: "a/config.go",
			body: []byte(`package dynamicconfig
var a = NewGlobalTypedSetting[map[string]any]("a.key", map[string]any{"b": 2}, "a")
`),
		},
	}

	declarations, err := extractDeclarations(files)
	if err != nil {
		t.Fatalf("extract declarations: %v", err)
	}
	if len(declarations) != 2 {
		t.Fatalf("got %d declarations, want 2", len(declarations))
	}
	if declarations[0].Key != "a.key" || declarations[0].Scope != "Global" ||
		declarations[0].ValueKind != "Typed" {
		t.Fatalf("unexpected first declaration: %#v", declarations[0])
	}
	if declarations[0].DefaultExpression != `map[string]any{"b": 2}` {
		t.Fatalf("unexpected rendered default: %q", declarations[0].DefaultExpression)
	}
	if declarations[1].Key != "z.key" || declarations[1].Source != "z/config.go:3" {
		t.Fatalf("unexpected second declaration: %#v", declarations[1])
	}
}

func TestExtractDeclarationsRecognizesConstructorVariants(t *testing.T) {
	t.Parallel()
	declarations, err := extractDeclarations([]sourceFile{{
		path: "config.go",
		body: []byte(`package other
import "go.temporal.io/server/common/dynamicconfig"
var converted = dynamicconfig.NewGlobalTypedSettingWithConverter[float64](
	"converted", convert, 1.5, "converted",
)
var constrained = dynamicconfig.NewTaskQueueIntSettingWithConstrainedDefault(
	"constrained", defaults, "constrained",
)
`),
	}})
	if err != nil {
		t.Fatalf("extract declarations: %v", err)
	}
	if len(declarations) != 2 {
		t.Fatalf("got %d declarations, want 2", len(declarations))
	}
	if declarations[0].DefaultExpression != "defaults" {
		t.Fatalf("unexpected constrained default: %q", declarations[0].DefaultExpression)
	}
	if declarations[1].DefaultExpression != "1.5" {
		t.Fatalf("unexpected converted default: %q", declarations[1].DefaultExpression)
	}
}

func TestExtractFileRejectsNonLiteralKeys(t *testing.T) {
	t.Parallel()
	_, err := extractFile(sourceFile{
		path: "config.go",
		body: []byte(`package dynamicconfig
const key = "a.key"
var a = NewGlobalBoolSetting(key, false, "a")
`),
	})
	if err == nil || !strings.Contains(err.Error(), "non-literal key") {
		t.Fatalf("got %v, want a non-literal-key error", err)
	}
}

func TestExtractFileRejectsUnknownDynamicConstructor(t *testing.T) {
	t.Parallel()
	_, err := extractFile(sourceFile{
		path: "config.go",
		body: []byte(`package dynamicconfig
var a = NewClusterBoolSetting("a.key", false, "a")
`),
	})
	if err == nil || !strings.Contains(err.Error(), "unrecognized") {
		t.Fatalf("got %v, want an unrecognized-constructor error", err)
	}
}

func TestExtractDeclarationsRejectsDuplicates(t *testing.T) {
	t.Parallel()
	_, err := extractDeclarations([]sourceFile{
		{
			path: "a.go",
			body: []byte(`package a
import "go.temporal.io/server/common/dynamicconfig"
var a = dynamicconfig.NewGlobalBoolSetting("duplicate", false, "a")
`),
		},
		{
			path: "b.go",
			body: []byte(`package b
import "go.temporal.io/server/common/dynamicconfig"
var b = dynamicconfig.NewGlobalBoolSetting("duplicate", true, "b")
`),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate dynamic setting") {
		t.Fatalf("got %v, want a duplicate-key error", err)
	}
}

func TestReadGoSourcesExcludesTestsAndConstructorDefinitions(t *testing.T) {
	t.Parallel()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for name, body := range map[string]string{
		"production.go":                       "package p",
		"production_test.go":                  "package p",
		"common/dynamicconfig/setting_gen.go": "package dynamicconfig",
		"README.md":                           "not Go",
	} {
		if err := writer.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(body)),
		}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := writer.Write([]byte(body)); err != nil {
			t.Fatalf("write body: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}

	files, err := readGoSources(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("read Go sources: %v", err)
	}
	if len(files) != 1 || files[0].path != "production.go" {
		t.Fatalf("unexpected source files: %#v", files)
	}
}
