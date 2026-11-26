package main

import "testing"

func TestOpenDBPoolConfig(t *testing.T) {
	db, err := openDB("postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := db.Stats().MaxOpenConnections; got != 50 {
		t.Fatalf("expected pool capped to 50 connections, got %d", got)
	}
}
