package main

import (
	"strings"
	"testing"
)

func TestBuildListCallHistoryQueryPlaceholders(t *testing.T) {
	q1, a1 := buildListCallHistoryQuery("", "", 50)
	if strings.Contains(q1, "WHERE") {
		t.Fatalf("expected no WHERE clause when filters empty, got: %s", q1)
	}
	if !strings.Contains(q1, "LIMIT $1") || len(a1) != 1 || a1[0] != 50 {
		t.Fatalf("expected LIMIT $1 with arg 50, got query=%s args=%v", q1, a1)
	}

	q2, a2 := buildListCallHistoryQuery("global", "sess_abc", 25)
	if !strings.Contains(q2, "COALESCE(ws.platform, 'local') = $1") ||
		!strings.Contains(q2, "cl.session_id = $2") ||
		!strings.Contains(q2, "LIMIT $3") {
		t.Fatalf("unexpected placeholder numbering: %s", q2)
	}
	if len(a2) != 3 || a2[0] != "global" || a2[1] != "sess_abc" || a2[2] != 25 {
		t.Fatalf("unexpected args: %v", a2)
	}

	q3, a3 := buildListCallHistoryQuery("local", "", 10)
	if !strings.Contains(q3, "COALESCE(ws.platform, 'local') = $1") || !strings.Contains(q3, "LIMIT $2") {
		t.Fatalf("unexpected single-filter query: %s", q3)
	}
	if len(a3) != 2 || a3[0] != "local" || a3[1] != 10 {
		t.Fatalf("unexpected args: %v", a3)
	}
}

func TestIsValidUUID(t *testing.T) {
	if !isValidUUID("11111111-2222-3333-4444-555555555555") {
		t.Fatal("expected valid UUID to pass")
	}
	for _, bad := range []string{"", "not-a-uuid", "11111111-2222-3333-4444-55555555555", "default-business-id"} {
		if isValidUUID(bad) {
			t.Fatalf("expected invalid UUID %q to fail", bad)
		}
	}
}

func TestResolveTargetJID(t *testing.T) {
	j, err := resolveTargetJID("+94771234567")
	if err != nil || j.String() != "94771234567@s.whatsapp.net" {
		t.Fatalf("unexpected phone JID: %v %v", j, err)
	}
	g, err := resolveTargetJID("12036301234567890@g.us")
	if err != nil || g.String() != "12036301234567890@g.us" {
		t.Fatalf("unexpected group JID: %v %v", g, err)
	}
	if _, err := resolveTargetJID("status@broadcast"); err == nil {
		t.Fatal("expected broadcast JID to be rejected")
	}
}
