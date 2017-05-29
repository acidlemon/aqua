package aqua

import (
	"fmt"
)

type OpenHandler func(string, string) (DB, error)

var handler map[string]OpenHandler = make(map[string]OpenHandler)

func Open(provider, driver, path string) (DB, error) {
	h, ok := handler[provider]

	if !ok {
		panic(fmt.Sprintf("no such provider: %s\n", provider))
	}

	d, err := h(driver, path)

	return d, err
}

func RegisterProvider(provider string, h OpenHandler) {
	handler[provider] = h
}
