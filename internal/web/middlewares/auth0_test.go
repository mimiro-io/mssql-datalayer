package middlewares

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifyAudience(t *testing.T) {
	cases := []struct {
		name string
		aud  jwt.ClaimStrings
		cmp  string
		want bool
	}{
		{"empty audience is accepted", nil, "api://x", true},
		{"single matching", jwt.ClaimStrings{"api://x"}, "api://x", true},
		{"single non-matching", jwt.ClaimStrings{"api://y"}, "api://x", false},
		{"multiple with match", jwt.ClaimStrings{"api://y", "api://x"}, "api://x", true},
	}
	for _, c := range cases {
		if got := verifyAudience(c.aud, c.cmp); got != c.want {
			t.Errorf("%s: verifyAudience(%v, %q) = %v, want %v", c.name, c.aud, c.cmp, got, c.want)
		}
	}
}

func TestVerifyIssuer(t *testing.T) {
	cases := []struct {
		name string
		iss  string
		cmp  string
		want bool
	}{
		{"empty issuer is accepted", "", "https://issuer", true},
		{"matching", "https://issuer", "https://issuer", true},
		{"non-matching", "https://other", "https://issuer", false},
	}
	for _, c := range cases {
		if got := verifyIssuer(c.iss, c.cmp); got != c.want {
			t.Errorf("%s: verifyIssuer(%q, %q) = %v, want %v", c.name, c.iss, c.cmp, got, c.want)
		}
	}
}

// TestParseWithClaims verifies the RS256 parser and that the custom + registered
// claims round-trip through jwt v5 as the auth middleware relies on.
func TestParseWithClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	claims := CustomClaims{
		Scope: "datahub:r",
		Gty:   "client-credentials",
		Adm:   true,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "https://issuer",
			Subject:   "user-123",
			Audience:  jwt.ClaimStrings{"api://x"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}

	token, err := parser.ParseWithClaims(signed, &CustomClaims{}, func(*jwt.Token) (interface{}, error) {
		return &key.PublicKey, nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims failed: %v", err)
	}

	got := token.Claims.(*CustomClaims)
	if got.Scope != "datahub:r" || got.Gty != "client-credentials" || !got.Adm {
		t.Errorf("custom claims did not round-trip: %+v", got)
	}
	if got.Issuer != "https://issuer" || got.Subject != "user-123" {
		t.Errorf("registered claims did not round-trip: %+v", got)
	}
	if !verifyAudience(got.Audience, "api://x") || !verifyIssuer(got.Issuer, "https://issuer") {
		t.Errorf("audience/issuer verification failed for valid token: %+v", got)
	}
}

// TestParseRejectsNonRS256 verifies the parser still enforces RS256 only.
func TestParseRejectsNonRS256(t *testing.T) {
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, CustomClaims{}).SignedString([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.ParseWithClaims(signed, &CustomClaims{}, func(*jwt.Token) (interface{}, error) {
		return []byte("secret"), nil
	})
	if err == nil {
		t.Error("expected HS256 token to be rejected by RS256-only parser")
	}
}
