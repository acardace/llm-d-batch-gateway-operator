package v1alpha1

import (
	"encoding/json"
	"testing"
)

func TestComponentExtraEndpointsPreservesEmptyList(t *testing.T) {
	empty := []string{}
	input := struct {
		APIServer BatchAPIConfig      `json:"apiServer"`
		Processor ProcessorConfigSpec `json:"processor"`
	}{
		APIServer: BatchAPIConfig{ExtraEndpoints: &empty},
		Processor: ProcessorConfigSpec{ExtraEndpoints: &empty},
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var output struct {
		APIServer BatchAPIConfig      `json:"apiServer"`
		Processor ProcessorConfigSpec `json:"processor"`
	}
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if output.APIServer.ExtraEndpoints == nil || len(*output.APIServer.ExtraEndpoints) != 0 {
		t.Fatalf("API server extraEndpoints = %v, want an explicit empty list", output.APIServer.ExtraEndpoints)
	}
	if output.Processor.ExtraEndpoints == nil || len(*output.Processor.ExtraEndpoints) != 0 {
		t.Fatalf("processor extraEndpoints = %v, want an explicit empty list", output.Processor.ExtraEndpoints)
	}
}
