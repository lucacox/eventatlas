package topology

import (
	"errors"
	"strings"
	"testing"
)

func TestNewService(t *testing.T) {
	// t.Parallel()

	tests := []struct {
		name  string
		value string
		id    string
		env   string
		err   error
		attrs map[string]string
	}{
		{name: "Service_OK", value: "service-1", id: "svc-1", env: "dev ", err: nil, attrs: nil},
		{name: "Service_Name_KO", value: "", id: "svc-1", env: "dev", err: ErrNodeNameEmpty, attrs: nil},
		{name: "Service_ID_KO", value: "service-1", id: "", env: "dev", err: ErrEmptyNodeID, attrs: nil},
		{name: "Service_Attr_OK", value: "service-1", id: "svc-1", env: "dev", err: nil, attrs: map[string]string{
			"attr1": "value1",
			"attr2": "value2",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := NewService(test.id, test.value, test.env, test.attrs)

			if test.err != nil {
				if !errors.Is(err, test.err) {
					t.Errorf("NewService should give error '%v', got '%v'", test.err, err)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			nId, _ := NewNodeID(test.id)
			if s.ID().String() != nId.String() {
				t.Errorf("Service NodeID = %s, want %s", s.ID().String(), nId.String())
			}

			if s.Name() != strings.TrimSpace(test.value) {
				t.Errorf("Service name = %s, want %s", s.Name(), strings.TrimSpace(test.value))
			}

			if s.Environment() != strings.TrimSpace(test.env) {
				t.Errorf("Service environment = %s, want %s", s.Environment(), strings.TrimSpace(test.env))
			}

			if test.attrs != nil {
				for k, v := range test.attrs {
					sv, sok := s.attributes[k]
					if !sok {
						t.Errorf("Attribute %s not found in service", k)
					}
					if sv != v {
						t.Errorf("s[%s] = %s, want %s", k, sv, v)
					}
				}
			} else {
				if s.Attributes() == nil {
					t.Errorf("should have initialize an empty attributes map")
				}
			}
		})
	}
}
