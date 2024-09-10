package types

import (
	"bytes"
	"math/rand"
	"strings"
	"sync"
	"testing"
)

func TestID(t *testing.T) {
	cases := []struct {
		s1    string
		q1    bool
		s2    string
		q2    bool
		equal bool
	}{
		{"abc", false, "abc", false, true},
		{"Abc", false, "abc", false, true},
		{"aBc", false, "abc", false, true},
		{"ABC", false, "abc", false, true},
		{"create", false, "create", false, true},
		{"create", false, "CREATE", false, true},
		{"abc", false, "abcd", false, false},
		{"abcd", false, "abc", false, false},
		{"abc", false, "ABCD", false, false},
		{"ABCD", false, "abc", false, false},
		{"create", true, "create", false, false},
		{"create", true, "CREATE", false, false},
		{"create", false, "create", true, false},
		{"create", false, "CREATE", true, false},
		{"create", true, "create", true, true},
		{"create", true, "CREATE", true, false},
		{"abc", true, "abc", true, true},
		{"ABC", true, "ABC", true, true},
		{"abc", false, "abc", true, true},
		{"ABC", false, "ABC", true, false},
		{"abc", true, "abc", false, true},
		{"ABC", true, "ABC", false, false},
		{"abc", true, "abcd", true, false},
		{"abcd", true, "abc", true, false},
		{"abc", true, "ABCD", true, false},
		{"ABCD", true, "abc", true, false},
		{
			strings.Repeat("x", MaxIdentifier+12), false,
			strings.Repeat("x", MaxIdentifier+34), false,
			true,
		},
	}

	for _, c := range cases {
		id1 := ID(c.s1, c.q1)
		id2 := ID(c.s2, c.q2)
		if c.equal {
			if id1 != id2 {
				t.Errorf("ID(%s, %v) != ID(%s, %v)", c.s1, c.q1, c.s2, c.q2)
			}
		} else if id1 == id2 {
			t.Errorf("ID(%s, %v) == ID(%s, %v)", c.s1, c.q1, c.s2, c.q2)
		}
	}
}

func TestString(t *testing.T) {
	cases := []struct {
		s string
		q bool
		r string
	}{
		{"abc", false, "abc"},
		{"ABC", false, "abc"},
		{"abc", true, "abc"},
		{"ABC", true, "ABC"},
		{"create", false, "CREATE"},
		{"CREATE", false, "CREATE"},
		{"create", true, "create"},
		{"CREATE", true, "CREATE"},
		{"system", false, "system"},
		{"system", true, "system"},
		{"bigint", false, "BIGINT"},
		{"bigint", true, "bigint"},
		{strings.Repeat("w", MaxIdentifier+12), false, strings.Repeat("w", MaxIdentifier)},
		{strings.Repeat("X", MaxIdentifier+12), false, strings.Repeat("x", MaxIdentifier)},
		{strings.Repeat("y", MaxIdentifier+12), true, strings.Repeat("y", MaxIdentifier)},
		{strings.Repeat("Z", MaxIdentifier+12), true, strings.Repeat("Z", MaxIdentifier)},
	}

	for _, c := range cases {
		id := ID(c.s, c.q)
		s := id.String()
		if s != c.r {
			t.Errorf("ID(%s, %v).String() got %s want %s", c.s, c.q, s, c.r)
		}
	}
}

func TestIsReserved(t *testing.T) {
	cases := []struct {
		s string
		q bool
		r bool
	}{
		{"abc", false, false},
		{"abc", true, false},
		{"create", false, true},
		{"Create", false, true},
		{"CREATE", false, true},
		{"update", false, true},
		{"select", false, true},
		{"bigint", false, false},
		{"BIGINT", true, false},
		{"system", false, false},
		{"SYSTEM", true, false},
	}

	for _, c := range cases {
		id := ID(c.s, c.q)
		r := id.IsReserved()
		if r != c.r {
			t.Errorf("ID(%s, %v).IsReserved() got %v want %v", c.s, c.q, r, c.r)
		}
	}
}

func TestKnown(t *testing.T) {
	cases := []struct {
		s     string
		q     bool
		id    Identifier
		equal bool
	}{
		{"create", false, CREATE, true},
		{"CREATE", false, CREATE, true},
		{"CREATE", true, CREATE, false},
		{"system", false, SYSTEM, true},
		{"system", true, SYSTEM, true},
		{"bigint", false, BIGINT, true},
		{"BIGINT", true, BIGINT, false},
	}

	for _, c := range cases {
		id := ID(c.s, c.q)
		if c.equal {
			if id != c.id {
				t.Errorf("ID(%s, %v) got %d want %d", c.s, c.q, id, c.id)
			}
		} else if id == c.id {
			t.Errorf("ID(%s, %v) got %d same as %d", c.s, c.q, id, c.id)
		}
	}
}

func TestKeywords(t *testing.T) {
	for s := range keywords {
		if s != strings.ToUpper(s) {
			t.Errorf("%q != strings.ToUpper(%q)", s, s)
		}
	}
}

var (
	asciiLetters = []rune{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
		's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
		'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	}
)

func randomString(min, max int, runes []rune) string {
	n := rand.Intn(max-min) + min

	var buf bytes.Buffer
	for n > 0 {
		buf.WriteRune(runes[rand.Intn(len(runes))])
		n -= 1
	}

	return buf.String()
}

func TestConcurrentID(t *testing.T) {
	var strs []string
	for n := 200; n > 0; n -= 1 {
		strs = append(strs, randomString(3, 10, asciiLetters))
	}

	var wg sync.WaitGroup
	var tests [][]Identifier
	for n := 8; n > 0; n -= 1 {
		wg.Add(1)

		ids := make([]Identifier, len(strs))
		tests = append(tests, ids)

		go func(strs []string, ids []Identifier) {
			defer wg.Done()

			perm := rand.Perm(len(strs))
			for _, idx := range perm {
				ids[idx] = ID(strs[idx], false)
			}
		}(strs, ids)
	}

	wg.Wait()

	for idx, s := range strs {
		id := ID(s, false)
		for _, ids := range tests {
			if ids[idx] != id {
				t.Errorf("%d: %d != %d", idx, ids[idx], id)
			}
		}
	}
}
