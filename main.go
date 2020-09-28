// Command pk-verify verifies all of the blobs present in a perkeep blobstore.
//
// "verify" here means "read the contents of a blob and check that the contents
// match the hash".  This is useful as a way to check for hardware failure or
// other data corruption. If all of the hashes match, you have strong assurance
// that none of the data is corrupted.
//
// Note that this "only" checks that each individual blob is valid. To really
// make sure you have not lost any data, you may want to check that the
// identities (i.e. blob refs) of all of these blobs are what you think they
// are. pk-verify provides only a small amount of help with this: it tells you
// _how many_ blobs it verified.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go4.org/jsonconfig"
	"go4.org/syncutil"

	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/serverinit"

	_ "perkeep.org/pkg/blobserver/blobpacked"

	_ "perkeep.org/pkg/sorted/leveldb"
)

// LowLevelConfig and StorageConfig represent part of a Perkeep
// ["low-level configuration"](https://perkeep.org/doc/server-config#lowlevel)
//
// In my perkeep configuration, I have something that looks like this:
//
//	{
//		...
//		"blobPath": "/Users/jeremy/pk/blobs",
//		"packRelated": true,
//		...
//	}
//
// ^ That is an example of a high-level configuration. It expands to the
// following low-level configuration:
//
//	{
//		...
//		"/bs-loose/": {
//			"handler": "storage-filesystem",
//			"handlerArgs": {
//				"path": "/Users/jeremy/pk/blobs"
//			}
//		},
//		"/bs-packed/": {
//			"handler": "storage-filesystem",
//			"handlerArgs": {
//				"path": "/Users/jeremy/pk/blobs/packed"
//			}
//		},
//		"/bs/": {
//			"handler": "storage-blobpacked",
//			"handlerArgs": {
//				"largeBlobs": "/bs-packed/",
//				"metaIndex": {
//					"file": "/Users/jeremy/pk/blobs/packed/packindex.leveldb",
//					"type": "leveldb"
//				},
//				"smallBlobs": "/bs-loose/"
//			}
//		},
//		...
//	}
//
// ^ This information is what is recorded in LowLevelConfig. The above could be written in Go as:
//
//	LowLevelConfig{
//		Prefixes: map[string]StorageConfig{
//			"/bs-loose/": StorageConfig{
//				StorageHandler: "filesystem",
//				StorageHandlerArgs: jsonconfig.Obj{
//					"path": "/Users/jeremy/pk/blobs",
//				},
//			},
//			"/bs-packed/": StorageConfig{
//				StorageHandler: "filesystem",
//				StorageHandlerArgs: jsonconfig.Obj{
//					"path": "/Users/jeremy/pk/blobs/packed",
//				},
//			},
//			"/bs/": StorageConfig{
//				StorageHandler: "filesystem",
//				StorageHandlerArgs: jsonconfig.Obj{
//					"largeBlobs": "/bs-packed/",
//					"metaIndex": map[string]interface{}{
//						"file": "/Users/jeremy/pk/blobs/packed/packindex.leveldb",
//						"type": "leveldb",
//					},
//					"smallBlobs": "/bs-loose",
//				},
//			},
//		},
//	}
//
// In the example above, note that the "handler" field from the json is called
// "StorageHandler" in the Go struct, and that the "storage-" prefix is removed
// in the Go struct.
type (
	LowLevelConfig struct {
		Prefixes map[string]StorageConfig
	}
	StorageConfig struct {
		StorageHandler     string
		StorageHandlerArgs jsonconfig.Obj
	}
)

// deleteUnknownFields deletes unknown fields, which has the effect of
// allowing the object to pass validation even if it contained unknown fields.
func deleteUnknownFields(obj jsonconfig.Obj) {
	for _, f := range obj.UnknownKeys() {
		delete(obj, f)
	}
}

// parseLowLevelConfig converts a jsonconfig.Obj, which can be an arbitrary
// json object, to a LowLevelConfig, which is guaranteed to have the fields I
// need. It was written in the spirit of:
//	https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/
func parseLowLevelConfig(obj jsonconfig.Obj) (*LowLevelConfig, error) {
	prefixes := obj.RequiredObject("prefixes")
	deleteUnknownFields(obj)
	result := &LowLevelConfig{
		Prefixes: make(map[string]StorageConfig, len(prefixes)),
	}
	for prefix := range prefixes {
		if strings.HasPrefix(prefix, "_") {
			continue
		}
		handler := prefixes.RequiredObject(prefix)
		name := handler.RequiredString("handler")
		if storageName := strings.TrimPrefix(name, "storage-"); storageName != name {
			result.Prefixes[prefix] = StorageConfig{
				StorageHandler:     storageName,
				StorageHandlerArgs: handler.RequiredObject("handlerArgs"),
			}
		} else {
			deleteUnknownFields(handler)
		}
		if err := handler.Validate(); err != nil {
			return nil, fmt.Errorf("In prefixes[%q]: %w", prefix, err)
		}

	}
	if err := obj.Validate(); err != nil {
		return nil, err
	}
	if err := prefixes.Validate(); err != nil {
		return nil, fmt.Errorf("In the \"prefixes\" map: %w", err)
	}
	return result, nil
}

func main() {
	// Check arguments.
	if len(os.Args) != 2 {
		stderrf("Usage: %v <path to perkeep server config file>\n", os.Args[0])
		stderrln()
		stderrf("Example: %v ~/.config/perkeep/server-config.json\n", os.Args[0])
		os.Exit(1)
	}

	// Parse config and find the handler for /bs/, the main blob handler.
	config, err := serverinit.LoadFile(os.Args[1])
	if err != nil {
		stderrf("pk-verify: %v\n", err)
		os.Exit(1)
	}
	lowLevelConfig, err := parseLowLevelConfig(config.LowLevelJSONConfig())
	if err != nil {
		stderrln("pk-verify: I do not recognize the format of this server config, and cannot continue :(")
		stderrln()
		stderrf("Here's specifically what surprised me in the (low-level expansion of the) config:\n\n\t%v\n", err)
		os.Exit(1)
	}
	bs, ok := lowLevelConfig.Prefixes["/bs/"]
	if !ok {
		stderrln("pk-verify: I do not recognize the format of this server config, and cannot continue :(")
		stderrln()
		stderrln("Specifically, I expect the low-level expansion of the config to contain a \"/bs/\" prefix, and it does not.")
		os.Exit(1)
	}

	// Initialize the storage handler for bs. (Note that this may
	// recursively initialize other handlers that bs uses).
	sto, err := blobserver.CreateStorage(bs.StorageHandler, NewLoader(lowLevelConfig), bs.StorageHandlerArgs)
	if err != nil {
		stderrf("pk-verify: failed to load blob storage: %v\n", err)
		os.Exit(1)
	}

	// Make sure we have a blob streaming interface.
	// We want to read these blobs fast.
	streamer, ok := sto.(blobserver.BlobStreamer)
	if !ok {
		stderrf("pk-verify does not support the %q blobserver. :(\n", bs.StorageHandler)
		stderrln()
		stderrln("I can only handle blobservers that implement BlobStreamer (that is, blobservers that allow a fast interface to streaming the contents of all blobs).")
		os.Exit(1)
	}

	// The centerpiece: verify all of the blobs.
	blobs := make(chan blobserver.BlobAndToken)
	var wg syncutil.Group
	wg.Go(func() error {
		return streamer.StreamBlobs(context.Background(), blobs, "")
	})
	var valid, invalid int
	for blob := range blobs {
		if blob.ValidContents(context.Background()) == nil {
			valid++
		} else {
			invalid++
			fmt.Println("found invalid blob:", blob.Ref())
		}
		if invalid == 0 {
			fmt.Printf(" verified %v blob%v...\r", valid, plural(valid))
		} else {
			fmt.Printf(" %v invalid blob%v, %v valid blob%v\r", invalid, plural(invalid), valid, plural(valid))
		}
	}
	if invalid == 0 {
		fmt.Printf("verified all %v blobs\n", valid)
	} else {
		fmt.Printf("CORRUPTION DETECTED: %v of %v blobs failed validation. Their refs are listed above.\n", invalid, valid+invalid)
	}

	// Final error handling: check if there were any failures in the
	// blob streaming implementation.
	if err := wg.Err(); err != nil {
		stderrf("pk-verify: error while streaming blobs: %v\n", err)
		os.Exit(1)
	}

	if invalid > 0 {
		os.Exit(2)
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func stderrf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func stderrln(a ...interface{}) {
	fmt.Fprintln(os.Stderr, a...)
}
