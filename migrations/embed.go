// Package migrations embeds the database schema used by the store package.
package migrations

import _ "embed"

// Schema is the SQL applied when a store is opened.
//
//go:embed 001_init.sql
var Schema string
