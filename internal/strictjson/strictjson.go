// Package strictjson is a token-level pre-pass that makes a subsequent
// encoding/json decode genuinely strict.
//
// encoding/json's DisallowUnknownFields is not enough on its own. It matches
// object member names to struct fields CASE-INSENSITIVELY ("IMAGE" fills
// Image), and when a member repeats, the LAST value wins silently. A document
// that a first-wins or case-sensitive reader elsewhere would read differently
// is therefore accepted. Check walks the document with json.Decoder.Token
// before the typed decode and refuses:
//
//   - a repeated member name in ANY object at any depth (map keys included);
//   - in an object the schema describes, any member name that is not EXACTLY
//     (byte-for-byte, case-sensitive) one of the schema's names;
//   - a document with anything after its single top-level value.
//
// It checks structure only; values are validated by the typed decode that
// follows.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrNotStrict classifies every refusal. Messages never echo a value.
var ErrNotStrict = errors.New("strictjson: document is not strict")

// Schema describes the expected shape. A nil Schema accepts any value (its
// objects are still checked for duplicate member names).
type Schema interface{ schema() }

// Object is an object whose member names must be exactly these keys.
type Object map[string]Schema

// Map is an object with free-form keys (still no duplicates), each value
// described by Value.
type Map struct{ Value Schema }

// Array is an array whose every element is described by Elem.
type Array struct{ Elem Schema }

func (Object) schema() {}
func (Map) schema()    {}
func (Array) schema()  {}

// Check reports whether raw is a single JSON value matching root.
func Check(raw []byte, root Schema) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := walk(dec, root); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data after the document", ErrNotStrict)
	}
	return nil
}

func walk(dec *json.Decoder, s Schema) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("%w: malformed document", ErrNotStrict)
	}
	delim, isDelim := tok.(json.Delim)
	switch {
	case isDelim && delim == '{':
		return walkObject(dec, s)
	case isDelim && delim == '[':
		var elem Schema
		switch a := s.(type) {
		case Array:
			elem = a.Elem
		case nil:
		default:
			return fmt.Errorf("%w: unexpected array", ErrNotStrict)
		}
		for dec.More() {
			if err := walk(dec, elem); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("%w: malformed array", ErrNotStrict)
		}
		return nil
	default:
		switch s.(type) {
		case Object, Array:
			return fmt.Errorf("%w: expected a structured value", ErrNotStrict)
		}
		return nil
	}
}

func walkObject(dec *json.Decoder, s Schema) error {
	var fields Object
	var values Schema
	switch o := s.(type) {
	case Object:
		fields = o
	case Map:
		values = o.Value
	case nil:
	default:
		return fmt.Errorf("%w: unexpected object", ErrNotStrict)
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("%w: malformed object", ErrNotStrict)
		}
		name, ok := tok.(string)
		if !ok {
			return fmt.Errorf("%w: malformed object", ErrNotStrict)
		}
		if seen[name] {
			return fmt.Errorf("%w: a member name repeats", ErrNotStrict)
		}
		seen[name] = true
		child := values
		if fields != nil {
			var known bool
			child, known = fields[name]
			if !known {
				return fmt.Errorf("%w: a member name is not exactly one this schema accepts", ErrNotStrict)
			}
		}
		if err := walk(dec, child); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("%w: malformed object", ErrNotStrict)
	}
	return nil
}
