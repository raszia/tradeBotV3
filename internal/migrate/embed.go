package migrate

import "embed"

// FS holds every migration file embedded into the binaries at build time, so a
// deployment ships its schema with the code and needs no external SQL setup.
// embed.FS satisfies fs.FS and is passed to Run/Status/EnsureCurrent.
//
//go:embed migrations/*.sql
var FS embed.FS
