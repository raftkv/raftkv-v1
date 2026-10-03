package main

import (
	"crypto/subtle"
	"net/http"
	"os"
	"strings"
)

type AuthMiddleware struct {
	validTokens map[string]bool
	enabled     bool
}

func NewAuthMiddleware() *AuthMiddleware {
	m := &AuthMiddleware{
		validTokens: make(map[string]bool),
		enabled:     false,
	}
	if token := os.Getenv("AUTH_BEARER_TOKEN"); token != "" {
		m.validTokens[token] = true
		m.enabled = true
	}
	return m
}

func (m *AuthMiddleware) Middleware(next http.HandlerFunc) http.HandlerFunc {
	if !m.enabled {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"missing or invalid authorization header"}`))
			return
		}
		token := auth[7:]
		valid := false
		for t := range m.validTokens {
			if subtle.ConstantTimeCompare([]byte(token), []byte(t)) == 1 {
				valid = true
				break
			}
		}
		if !valid {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid token"}`))
			return
		}
		next(w, r)
	}
}
