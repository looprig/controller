package strictjson

import (
	"errors"
	"testing"
)

var schema = Object{
	"image": nil,
	"resources": Object{
		"cpu": nil,
	},
	"settings": Map{},
	"list":     Array{Elem: Object{"id": nil}},
}

func TestCheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		ok   bool
	}{
		{"exact", `{"image":"a","resources":{"cpu":"1"},"settings":{"A":"1","a":"2"},"list":[{"id":"x"}]}`, true},
		{"empty object", `{}`, true},
		{"duplicate top-level", `{"image":"a","image":"b"}`, false},
		{"case variant", `{"IMAGE":"a"}`, false},
		{"case variant nested", `{"resources":{"CPU":"1"}}`, false},
		{"unknown member", `{"env":{}}`, false},
		{"duplicate nested", `{"resources":{"cpu":"1","cpu":"2"}}`, false},
		{"duplicate map key", `{"settings":{"A":"1","A":"2"}}`, false},
		{"duplicate in array element", `{"list":[{"id":"x","id":"y"}]}`, false},
		{"case variant in array element", `{"list":[{"ID":"x"}]}`, false},
		{"duplicate inside a leaf value", `{"image":{"x":1,"x":2}}`, false},
		{"trailing document", `{} {}`, false},
		{"not an object", `[]`, false},
		{"truncated", `{"image":`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Check([]byte(tc.doc), schema)
			if tc.ok && err != nil {
				t.Fatalf("Check = %v, want nil", err)
			}
			if !tc.ok && !errors.Is(err, ErrNotStrict) {
				t.Fatalf("Check = %v, want ErrNotStrict", err)
			}
		})
	}
}

func TestCheckArrayRoot(t *testing.T) {
	root := Array{Elem: Object{"tenant_id": nil, "session_id": nil}}
	if err := Check([]byte(`[{"tenant_id":"t","session_id":"s"}]`), root); err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, doc := range []string{
		`[{"tenant_id":"t","Tenant_ID":"u","session_id":"s"}]`,
		`[{"tenant_id":"t","tenant_id":"u","session_id":"s"}]`,
		`{"tenant_id":"t"}`,
	} {
		if err := Check([]byte(doc), root); !errors.Is(err, ErrNotStrict) {
			t.Fatalf("Check(%s) = %v, want ErrNotStrict", doc, err)
		}
	}
}
