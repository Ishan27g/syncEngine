package utils

import (
	"encoding/json"
	"fmt"

	registry "github.com/Ishan27gOrg/registry/golang/registry/package"
)

func PrintJson(js interface{}) string {
	data, err := json.MarshalIndent(js, "", " ")
	if err != nil {
		fmt.Println("error:", err)
	}
	return string(data)
}

func MockRegistry() {
	go func() {
		c := registry.Setup()
		registry.Run("9999", c)

	}()
}
