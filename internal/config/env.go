package config

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix is prepended to every environment override name.
const EnvPrefix = "KSS_"

// EnvVarFor returns the environment variable that overrides a dotted YAML key,
// following the documented rule: EnvPrefix plus the upper-cased path segments
// joined by underscores. So "scaling.scaleUpCooldown" becomes
// "KSS_SCALING_SCALEUPCOOLDOWN", exactly as docs/requirements.md § 8 specifies.
func EnvVarFor(dottedKey string) string {
	if dottedKey == "" {
		return ""
	}
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(dottedKey, ".", "_"))
}

// EnvVars returns every recognised override name, sorted. Deriving this from
// the struct rather than maintaining a list is what keeps the override surface
// from drifting away from the schema as fields are added.
func EnvVars() []string {
	names := make([]string, 0, 48)
	for key := range schemaKeys() {
		names = append(names, EnvVarFor(key))
	}
	sort.Strings(names)
	return names
}

// Keys returns every dotted YAML key in the schema, sorted.
func Keys() []string {
	keys := make([]string, 0, 48)
	for key := range schemaKeys() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func schemaKeys() map[string]struct{} {
	keys := map[string]struct{}{}
	cfg := &Config{}
	_ = walkFields(reflect.ValueOf(cfg).Elem(), nil, func(path string, _ reflect.Value) error {
		keys[path] = struct{}{}
		return nil
	})
	return keys
}

// applyEnvOverrides applies every set KSS_* variable onto cfg.
//
// Unknown KSS_* variables are rejected rather than ignored. A typo such as
// KSS_CONTROLER_INTERVAL would otherwise leave the controller running on a
// default while the operator believes the override took effect, which is the
// same class of silent-wrong-configuration failure that CR-2 exists to prevent.
func applyEnvOverrides(cfg *Config, getenv func(string) string, environ func() []string) error {
	var problems []FieldError

	err := walkFields(reflect.ValueOf(cfg).Elem(), nil, func(path string, field reflect.Value) error {
		raw, ok := lookup(getenv, EnvVarFor(path))
		if !ok {
			return nil
		}
		if err := assign(field, raw); err != nil {
			problems = append(problems, FieldError{
				Key:     path,
				Value:   raw,
				Problem: err.Error(),
				Ref:     "CR-2",
			})
		}
		return nil
	})
	if err != nil {
		return err
	}

	if environ != nil {
		known := map[string]struct{}{}
		for _, name := range EnvVars() {
			known[name] = struct{}{}
		}
		var unknown []string
		for _, entry := range environ() {
			name, _, found := strings.Cut(entry, "=")
			if !found || !strings.HasPrefix(name, EnvPrefix) {
				continue
			}
			if _, ok := known[name]; !ok {
				unknown = append(unknown, name)
			}
		}
		sort.Strings(unknown)
		for _, name := range unknown {
			problems = append(problems, FieldError{
				Key:     strings.ToLower(strings.TrimPrefix(name, EnvPrefix)),
				Problem: fmt.Sprintf("%s is not a recognised configuration override; see `kubescalesense -print-env` for the full list", name),
				Ref:     "CR-2",
			})
		}
	}

	if len(problems) > 0 {
		return &ValidationError{Errors: problems}
	}
	return nil
}

func lookup(getenv func(string) string, name string) (string, bool) {
	v := getenv(name)
	if v == "" {
		return "", false
	}
	return v, true
}

// walkFields visits every leaf field, passing its dotted YAML path.
func walkFields(v reflect.Value, prefix []string, fn func(path string, field reflect.Value) error) error {
	t := v.Type()
	for i := range t.NumField() {
		sf := t.Field(i)
		tag := strings.Split(sf.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		path := append(prefix, tag)
		field := v.Field(i)

		if field.Kind() == reflect.Struct && field.Type() != reflect.TypeOf(time.Time{}) {
			if err := walkFields(field, path, fn); err != nil {
				return err
			}
			continue
		}
		if err := fn(strings.Join(path, "."), field); err != nil {
			return err
		}
	}
	return nil
}

// assign parses raw into field, with messages that name the expected form.
func assign(field reflect.Value, raw string) error {
	raw = strings.TrimSpace(raw)

	if field.Type() == reflect.TypeOf(Duration(0)) {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("is not a valid duration: expected a form such as %q, %q or %q", "500ms", "30s", "15m")
		}
		field.SetInt(int64(d))
		return nil
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("is not a valid boolean: expected true or false")
		}
		field.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, field.Type().Bits())
		if err != nil {
			return fmt.Errorf("is not a valid integer")
		}
		field.SetInt(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, field.Type().Bits())
		if err != nil {
			return fmt.Errorf("is not a valid number")
		}
		field.SetFloat(f)
	default:
		return fmt.Errorf("has unsupported type %s", field.Type())
	}
	return nil
}
