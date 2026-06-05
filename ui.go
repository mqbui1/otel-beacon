package main

import (
	_ "embed"
	"net/http"
)

//go:embed static/index.html
var indexHTML []byte

func uiHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		w.Write(indexHTML)
	})
}
