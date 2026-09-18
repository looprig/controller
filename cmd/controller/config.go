package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/controller/driver"
)

// Config is everything this binary reads from its environment.
//
// EVERY VARIABLE IS REQUIRED AND THERE ARE NO DEFAULTS. A controller's
// namespace, identity, credential allowlist and timings are deployment policy;
// a default chosen here would be a policy nobody wrote down. This type owns
// PARSING; the ranges are decided by kubernetes.New and driver.New, which
// refuse an incoherent configuration themselves.
type Config struct {
	Namespace     string
	ControllerID  string
	ReplicaID     string
	HostSubdomain string
	HostPort      int32
	Credentials   map[string]string
	Sessions      []driver.Key
	Interval      time.Duration
	ClaimTTL      time.Duration
	ItemTimeout   time.Duration
}

// ConfigError names the one variable a person must fix. It never echoes the
// variable's value.
type ConfigError struct {
	Variable string
	Reason   string
}

func (e *ConfigError) Error() string { return "controller: " + e.Variable + ": " + e.Reason }

// Environment is os.LookupEnv's shape.
type Environment func(string) (string, bool)

// LoadConfig reads a Config, reporting the first unusable variable.
func LoadConfig(lookup Environment) (Config, error) {
	r := &reader{lookup: lookup}
	cfg := Config{
		Namespace:     r.text("CONTROLLER_NAMESPACE"),
		ControllerID:  r.text("CONTROLLER_ID"),
		ReplicaID:     r.text("CONTROLLER_REPLICA_ID"),
		HostSubdomain: r.text("CONTROLLER_HOST_SUBDOMAIN"),
		HostPort:      r.port("CONTROLLER_HOST_PORT"),
		Credentials:   r.credentials("CONTROLLER_CREDENTIALS"),
		Sessions:      r.sessions("CONTROLLER_SESSIONS"),
		Interval:      r.duration("CONTROLLER_INTERVAL"),
		ClaimTTL:      r.duration("CONTROLLER_CLAIM_TTL"),
		ItemTimeout:   r.duration("CONTROLLER_ITEM_TIMEOUT"),
	}
	if r.err != nil {
		return Config{}, r.err
	}
	return cfg, nil
}

type reader struct {
	lookup Environment
	err    error
}

func (r *reader) fail(name, reason string) { r.err = &ConfigError{Variable: name, Reason: reason} }

func (r *reader) text(name string) string {
	if r.err != nil {
		return ""
	}
	value, ok := r.lookup(name)
	if !ok || value == "" {
		r.fail(name, "must be set")
		return ""
	}
	return value
}

func (r *reader) duration(name string) time.Duration {
	value := r.text(name)
	if r.err != nil {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		r.fail(name, "must be a positive duration")
		return 0
	}
	return d
}

func (r *reader) port(name string) int32 {
	value := r.text(name)
	if r.err != nil {
		return 0
	}
	n, err := strconv.ParseUint(value, 10, 16)
	if err != nil || n == 0 {
		r.fail(name, "must be a port number 1..65535")
		return 0
	}
	return int32(n)
}

// credentials reads "ref=secret,ref=secret".
func (r *reader) credentials(name string) map[string]string {
	value := r.text(name)
	if r.err != nil {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(value, ",") {
		ref, secret, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || ref == "" || secret == "" {
			r.fail(name, "must be comma-separated reference=secret-name pairs")
			return nil
		}
		if _, dup := out[ref]; dup {
			r.fail(name, "names one credential reference twice")
			return nil
		}
		out[ref] = secret
	}
	return out
}

// sessions reads a JSON array of {"tenant_id","session_id"} objects, strictly.
func (r *reader) sessions(name string) []driver.Key {
	value := r.text(name)
	if r.err != nil {
		return nil
	}
	var raw []struct {
		TenantID  sessionwire.TenantID  `json:"tenant_id"`
		SessionID sessionwire.SessionID `json:"session_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(value)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		r.fail(name, `must be a JSON array of {"tenant_id","session_id"}`)
		return nil
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		r.fail(name, "carries trailing data")
		return nil
	}
	if len(raw) == 0 {
		r.fail(name, "names no session")
		return nil
	}
	keys := make([]driver.Key, 0, len(raw))
	for _, k := range raw {
		keys = append(keys, driver.Key{TenantID: k.TenantID, SessionID: k.SessionID})
	}
	return keys
}
