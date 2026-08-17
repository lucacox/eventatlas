package topology

import (
	"errors"
	"strings"
	"testing"
)

func TestNewNode(t *testing.T) {
	// t.Parallel()

	tests := []struct {
		name  string
		value string
		id    string
		env   string
		err   error
		attrs map[string]string
	}{
		{name: "Node_OK", value: "Node-1", id: "svc-1", env: "dev ", err: nil, attrs: nil},
		{name: "Node_Name_KO", value: "", id: "svc-1", env: "dev", err: ErrNodeNameEmpty, attrs: nil},
		{name: "Node_ID_KO", value: "Node-1", id: "", env: "dev", err: ErrEmptyNodeID, attrs: nil},
		{name: "Node_Attr_OK", value: "Node-1", id: "svc-1", env: "dev", err: nil, attrs: map[string]string{
			"attr1": "value1",
			"attr2": "value2",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := NewNode(test.id, test.value, test.attrs)

			if test.err != nil {
				if !errors.Is(err, test.err) {
					t.Errorf("NewNode should give error '%v', got '%v'", test.err, err)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			nId, _ := NewNodeID(test.id)
			if s.ID().String() != nId.String() {
				t.Errorf("Node NodeID = %s, want %s", s.ID().String(), nId.String())
			}

			if s.Name() != strings.TrimSpace(test.value) {
				t.Errorf("Node name = %s, want %s", s.Name(), strings.TrimSpace(test.value))
			}

			if test.attrs != nil {
				for k, v := range test.attrs {
					sv, sok := s.attributes[k]
					if !sok {
						t.Errorf("Attribute %s not found in Node", k)
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

func TestNode_SetAttribute(t *testing.T) {
	s, _ := NewNode("svc-1", "Node1", nil)
	err := s.SetAttribute("attr1", "val")
	if err != nil {
		t.Errorf("Received unexpected error %v", err)
	}
	if s.attributes["attr1"] != "val" {
		t.Errorf("attr1 = %s, want %s", s.attributes["attr1"], "val")
	}

	err = s.SetAttribute("", "val")
	if !errors.Is(err, ErrNodeAttributeNameEmpty) {
		t.Errorf("expected %v got %v", ErrNodeAttributeNameEmpty, err)
	}
	_, ok := s.attributes[""]
	if ok {
		t.Error("empty key should not be present in attributes")
	}
}

func TestNode_Attribute(t *testing.T) {
	s, _ := NewNode("svc-1", "Node1", map[string]string{"attr1": "val"})
	v, ok := s.Attribute("attr1")
	if !ok {
		t.Error("'attr1' key should be present")
	}

	if v != "val" {
		t.Errorf("Attributes[%s] = %s, wnat %s", "attr1", v, "val")
	}
}

func TestNode_DeleteAttribute(t *testing.T) {
	s, _ := NewNode("svc-1", "Node1", map[string]string{"attr1": "val"})
	s.DeleteAttribute("attr1")
	_, ok := s.Attribute("attr1")
	if ok {
		t.Error("'attr1' key should not be present")
	}
}

func TestNode_Attributes(t *testing.T) {
	attrs := map[string]string{"attr1": "val1", "attr2": "val2"}
	s, _ := NewNode("svc-1", "Node1", attrs)
	sAttrs := s.Attributes()

	for k, v := range attrs {
		if sAttrs[k] != v {
			t.Errorf("Node.Attributes[%s] = %s, want %s", k, v, attrs[k])
		}
	}

	attrs["attr1"] = "other"
	if sAttrs["attr1"] == attrs["attr1"] {
		t.Errorf("Attributes() should've returned a copy")
	}
}
