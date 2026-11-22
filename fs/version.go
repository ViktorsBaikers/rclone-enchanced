package fs

// Version of rclone containing the complete version string
var Version string

// Build provenance injected by the fork build harness. Empty values are not printed.
var (
	BuildCommit   string
	BuildDate     string
	BuildSnapshot string
	BuildDirty    string
)

func init() {
	if Version == "" {
		if VersionSuffix == "" {
			Version = VersionTag
		} else {
			Version = VersionTag + "-" + VersionSuffix
		}
	}
}
