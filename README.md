pk-verify
---------

This is a tool to verify all of the blobs present in a [perkeep](https://perkeep.org) blobstore.

"verify" here means "read the contents of a blob and check that the contents match the hash". This is useful as a way to check for hardware failure or other data corruption. If all of the hashes match, you have strong assurance that none of the data is corrupted.

Note that this "only" checks that each individual blob is valid. To really make sure you have not lost any data, you may want to check that the identities (i.e. blob refs) of all of these blobs are what you think they are. pk-verify provides only a small amount of help with this: it tells you _how many_ blobs it verified.
