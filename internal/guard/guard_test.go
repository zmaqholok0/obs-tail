package guard

import "testing"

func mustGuard(t *testing.T, highWater float64, keep int) *Guard {
	t.Helper()
	g, err := New(highWater, keep)
	if err != nil {
		t.Fatalf("New(%v, %d): %v", highWater, keep, err)
	}
	return g
}

func TestAllowBelowHighWaterLetsEverythingThrough(t *testing.T) {
	g := mustGuard(t, 0.8, 5)
	for i := range 500 {
		if !g.Allow("trace", 79, 100) {
			t.Fatalf("trace %d shed below high water", i)
		}
	}
	if admitted, shed := g.Stats(); admitted != 500 || shed != 0 {
		t.Fatalf("stats = %d admitted, %d shed; want 500, 0", admitted, shed)
	}
}

func TestAllowAtLimitKeepsOnlyBudget(t *testing.T) {
	g := mustGuard(t, 0.8, 10)
	allowed := 0
	const ids = 10000
	for i := range ids {
		if g.Allow(id(i), 100, 100) {
			allowed++
		}
	}
	// 10% keep budget with hash noise; 8-12% is the useful band.
	if allowed < ids/100*8 || allowed > ids/100*12 {
		t.Fatalf("allowed %d of %d, want 800..1200", allowed, ids)
	}
}

func TestAllowIsStickyPerTrace(t *testing.T) {
	g := mustGuard(t, 0.5, 10)
	for i := range 200 {
		id := id(i)
		first := g.Allow(id, 100, 100)
		for range 3 {
			if g.Allow(id, 100, 100) != first {
				t.Fatalf("trace %s changed answer between calls", id)
			}
		}
	}
}

func TestNewRejectsBadBounds(t *testing.T) {
	cases := []struct {
		highWater float64
		keep      int
	}{
		{0, 10},
		{1.5, 10},
		{0.5, 0},
		{0.5, 101},
	}
	for _, tc := range cases {
		if _, err := New(tc.highWater, tc.keep); err == nil {
			t.Errorf("New(%v, %d) accepted", tc.highWater, tc.keep)
		}
	}
}

func id(i int) string {
	const hex = "0123456789abcdef"
	var b [16]byte
	for j := range b {
		b[j] = hex[(i>>uint(j%4))&0xf]
	}
	return string(b[:])
}
