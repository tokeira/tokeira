// temporal-config-audit extracts Temporal dynamic-configuration declarations.
//
// The tool reads a tagged tree through git archive so its result cannot depend on
// uncommitted files in the reference checkout. It intentionally understands only
// Temporal's generated New{Scope}{Type}Setting constructor families: a new
// constructor must be classified explicitly before it can alter the denominator.
package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type settingDeclaration struct {
	Key               string `json:"key"`
	Constructor       string `json:"constructor"`
	Scope             string `json:"scope"`
	ValueKind         string `json:"value_kind"`
	DefaultExpression string `json:"default_expression"`
	Source            string `json:"source"`
}

type sourceFile struct {
	path string
	body []byte
}

var (
	repository = flag.String("repo", "", "path to the Temporal Git checkout")
	tag        = flag.String("tag", "v1.31.0", "Git tag to inspect")
	output     = flag.String("output", "-", "output JSON path, or - for stdout")
)

func main() {
	flag.Parse()
	if *repository == "" {
		exitError(errors.New("--repo is required"))
	}

	files, err := archiveGoSources(*repository, *tag)
	if err != nil {
		exitError(err)
	}
	declarations, err := extractDeclarations(files)
	if err != nil {
		exitError(err)
	}
	encoded, err := json.MarshalIndent(declarations, "", "  ")
	if err != nil {
		exitError(fmt.Errorf("encode declarations: %w", err))
	}
	encoded = append(encoded, '\n')

	if *output == "-" {
		if _, err := os.Stdout.Write(encoded); err != nil {
			exitError(fmt.Errorf("write stdout: %w", err))
		}
		return
	}
	if err := os.WriteFile(*output, encoded, 0o644); err != nil {
		exitError(fmt.Errorf("write %s: %w", *output, err))
	}
}

func exitError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func archiveGoSources(repo, revision string) ([]sourceFile, error) {
	command := exec.Command("git", "-C", repo, "archive", "--format=tar", revision)
	archive, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("git archive %s: %s", revision, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf("git archive %s: %w", revision, err)
	}

	return readGoSources(bytes.NewReader(archive))
}

func readGoSources(archive io.Reader) ([]sourceFile, error) {
	reader := tar.NewReader(archive)
	var files []sourceFile
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read git archive: %w", err)
		}
		if header.Typeflag != tar.TypeReg || !strings.HasSuffix(header.Name, ".go") ||
			strings.HasSuffix(header.Name, "_test.go") ||
			header.Name == "common/dynamicconfig/setting_gen.go" {
			continue
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", header.Name, err)
		}
		files = append(files, sourceFile{path: filepath.ToSlash(header.Name), body: body})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	return files, nil
}

func extractDeclarations(files []sourceFile) ([]settingDeclaration, error) {
	byKey := make(map[string]settingDeclaration)
	for _, file := range files {
		declarations, err := extractFile(file)
		if err != nil {
			return nil, err
		}
		for _, declaration := range declarations {
			if previous, exists := byKey[declaration.Key]; exists {
				return nil, fmt.Errorf(
					"duplicate dynamic setting %q at %s (first declared at %s)",
					declaration.Key,
					declaration.Source,
					previous.Source,
				)
			}
			byKey[declaration.Key] = declaration
		}
	}

	declarations := make([]settingDeclaration, 0, len(byKey))
	for _, declaration := range byKey {
		declarations = append(declarations, declaration)
	}
	sort.Slice(declarations, func(i, j int) bool {
		return declarations[i].Key < declarations[j].Key
	})
	return declarations, nil
}

func extractFile(file sourceFile) ([]settingDeclaration, error) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, file.path, file.body, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", file.path, err)
	}

	var declarations []settingDeclaration
	var visitErr error
	ast.Inspect(parsed, func(node ast.Node) bool {
		if visitErr != nil {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		constructor := calledName(call.Fun)
		if !strings.HasPrefix(constructor, "New") || !strings.Contains(constructor, "Setting") {
			return true
		}
		scope, kind, defaultIndex, recognized := classifyConstructor(constructor)
		if !recognized {
			// Only setting-shaped calls in the dynamicconfig package are relevant.
			// This rejects a new Temporal constructor while ignoring unrelated
			// packages that happen to use a generic NewSetting name.
			if parsed.Name.Name == "dynamicconfig" || selectorPackage(call.Fun) == "dynamicconfig" {
				position := fileSet.Position(call.Pos())
				visitErr = fmt.Errorf(
					"unrecognized dynamic setting constructor %s at %s:%d",
					constructor,
					file.path,
					position.Line,
				)
			}
			return true
		}
		if len(call.Args) <= defaultIndex {
			position := fileSet.Position(call.Pos())
			visitErr = fmt.Errorf(
				"dynamic setting constructor %s has no default argument at %s:%d",
				constructor,
				file.path,
				position.Line,
			)
			return false
		}
		keyLiteral, ok := call.Args[0].(*ast.BasicLit)
		if !ok || keyLiteral.Kind != token.STRING {
			position := fileSet.Position(call.Args[0].Pos())
			visitErr = fmt.Errorf(
				"dynamic setting constructor %s has a non-literal key at %s:%d",
				constructor,
				file.path,
				position.Line,
			)
			return false
		}
		key, err := strconv.Unquote(keyLiteral.Value)
		if err != nil {
			visitErr = fmt.Errorf("decode setting key at %s: %w", file.path, err)
			return false
		}
		var rendered bytes.Buffer
		if err := format.Node(&rendered, fileSet, call.Args[defaultIndex]); err != nil {
			visitErr = fmt.Errorf("render default for %q: %w", key, err)
			return false
		}
		position := fileSet.Position(call.Pos())
		declarations = append(declarations, settingDeclaration{
			Key:               key,
			Constructor:       constructor,
			Scope:             scope,
			ValueKind:         kind,
			DefaultExpression: rendered.String(),
			Source:            fmt.Sprintf("%s:%d", file.path, position.Line),
		})
		return true
	})
	if visitErr != nil {
		return nil, visitErr
	}
	return declarations, nil
}

func calledName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	case *ast.IndexExpr:
		return calledName(value.X)
	case *ast.IndexListExpr:
		return calledName(value.X)
	default:
		return ""
	}
}

func selectorPackage(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.SelectorExpr:
		if identifier, ok := value.X.(*ast.Ident); ok {
			return identifier.Name
		}
	case *ast.IndexExpr:
		return selectorPackage(value.X)
	case *ast.IndexListExpr:
		return selectorPackage(value.X)
	}
	return ""
}

func classifyConstructor(name string) (string, string, int, bool) {
	const prefix = "New"
	const marker = "Setting"
	if !strings.HasPrefix(name, prefix) {
		return "", "", 0, false
	}
	body := strings.TrimPrefix(name, prefix)
	markerAt := strings.Index(body, marker)
	if markerAt == -1 {
		return "", "", 0, false
	}
	middle := body[:markerAt]
	variant := body[markerAt+len(marker):]
	defaultIndex := 1
	switch variant {
	case "":
	case "WithConstrainedDefault":
		if strings.HasSuffix(middle, "Typed") {
			defaultIndex = 2
		}
	case "WithConverter":
		if !strings.HasSuffix(middle, "Typed") {
			return "", "", 0, false
		}
		defaultIndex = 2
	default:
		return "", "", 0, false
	}
	scopes := []string{
		"ChasmTaskType",
		"NamespaceID",
		"Destination",
		"TaskQueue",
		"TaskType",
		"ShardID",
		"Namespace",
		"Global",
	}
	kinds := []string{"Duration", "String", "Float", "Typed", "Bool", "Map", "Int"}
	for _, scope := range scopes {
		if !strings.HasPrefix(middle, scope) {
			continue
		}
		kind := strings.TrimPrefix(middle, scope)
		for _, candidate := range kinds {
			if kind == candidate {
				return scope, candidate, defaultIndex, true
			}
		}
	}
	return "", "", 0, false
}
