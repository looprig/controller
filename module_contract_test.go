package controller

import (
	"bytes"
	"os"
	"testing"
)

func TestModuleContract(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}

	for _, want := range []struct {
		directive []byte
		value     []byte
	}{
		{directive: []byte("module"), value: []byte("github.com/looprig/controller")},
		{directive: []byte("go"), value: []byte("1.26.8")},
	} {
		found := false
		for _, line := range bytes.Split(mod, []byte{'\n'}) {
			fields := bytes.Fields(line)
			if len(fields) >= 2 && bytes.Equal(fields[0], want.directive) && bytes.Equal(fields[1], want.value) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("go.mod does not declare %s %s", want.directive, want.value)
		}
	}

	for _, line := range bytes.Split(mod, []byte{'\n'}) {
		trimmed := bytes.TrimSpace(line)
		fields := bytes.Fields(trimmed)
		if len(fields) > 0 && bytes.Equal(fields[0], []byte("replace")) {
			t.Errorf("go.mod must not declare a replace directive: %q", line)
		}
	}
}
