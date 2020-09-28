package main

// Copied, with modifications, from perkeep.org/pkg/test

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"perkeep.org/pkg/blobserver"
)

// NewLoader
func NewLoader(conf *LowLevelConfig) *Loader {
	return &Loader{
		conf: conf,
	}
}

type Loader struct {
	mu   sync.Mutex
	sto  map[string]blobserver.Storage
	conf *LowLevelConfig
}

var _ blobserver.Loader = (*Loader)(nil)

func (ld *Loader) FindHandlerByType(handlerType string) (prefix string, handler interface{}, err error) {
	panic("NOIMPL")
}

func (ld *Loader) AllHandlers() (map[string]string, map[string]interface{}) {
	panic("NOIMPL")
}

func (ld *Loader) MyPrefix() string {
	return "/lies/"
}

func (ld *Loader) BaseURL() string {
	return "http://localhost:1234"
}

func (ld *Loader) GetHandlerType(prefix string) string {
	log.Printf("test.Loader: GetHandlerType called but not implemented.")
	return ""
}

func (ld *Loader) GetHandler(prefix string) (interface{}, error) {
	log.Printf("test.Loader: GetHandler called but not implemented.")
	return nil, errors.New("doesn't exist")
}

func (ld *Loader) SetStorage(prefix string, s blobserver.Storage) {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if ld.sto == nil {
		ld.sto = make(map[string]blobserver.Storage)
	}
	ld.sto[prefix] = s
}

func (ld *Loader) GetStorage(prefix string) (blobserver.Storage, error) {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if bs, ok := ld.sto[prefix]; ok {
		return bs, nil
	}
	if ld.sto == nil {
		ld.sto = make(map[string]blobserver.Storage)
	}
	stoConf, ok := ld.conf.Prefixes[prefix]
	if !ok {
		return nil, fmt.Errorf("no storage configuration found for this prefix: %q", prefix)
	}
	sto, err := blobserver.CreateStorage(stoConf.StorageHandler, ld, stoConf.StorageHandlerArgs)
	if err != nil {
		return nil, err
	}
	ld.sto[prefix] = sto
	return sto, nil
}
