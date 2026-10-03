package main

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestSM4TestKeyGuard_ProductionRejects(t *testing.T) {
	testKey, _ := hex.DecodeString(sm4TestKeyHex)
	err := sm4TestKeyGuard(testKey, true)
	if err == nil {
		t.Fatal("期望返回错误, 实际返回 nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("测试密钥")) {
		t.Fatalf("错误信息应包含'测试密钥', 实际: %v", err)
	}
}

func TestSM4TestKeyGuard_DevModeWarns(t *testing.T) {
	testKey, _ := hex.DecodeString(sm4TestKeyHex)
	err := sm4TestKeyGuard(testKey, false)
	if err != nil {
		t.Fatalf("期望返回 nil (仅警告), 实际返回错误: %v", err)
	}
}

func TestSM4TestKeyGuard_NormalKeyUnaffected(t *testing.T) {
	normalKey := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	err := sm4TestKeyGuard(normalKey, true)
	if err != nil {
		t.Fatalf("正常密钥不应被拦截, 实际返回错误: %v", err)
	}
}
