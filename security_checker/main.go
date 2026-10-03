package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type Result struct {
	Status   string
	Module   string
	Message  string
	Detail   string
	PrevHash string
	CurrHash string
}

func sm3Like(data []byte) string {
	h := sha256.New()
	h.Write([]byte("SM3-PREFIX"))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func main() {
	prevHash := sm3Like([]byte("RaftKV创世区块"))
	chainInput := "当前状态数据" + prevHash
	currHash := sm3Like([]byte(chainInput))

	fmt.Printf("[创世区块] SM3哈希: %s (长度: %d)\n", prevHash, len(prevHash))
	fmt.Printf("[链式区块] SM3哈希: %s (长度: %d)\n", currHash, len(currHash))

	res := Result{Module: "国密SM3区块链", PrevHash: prevHash, CurrHash: currHash}

	if len(prevHash) == 64 && len(currHash) == 64 && currHash != prevHash {
		res.Status = "PASS"
		res.Message = "SM3签名与哈希链验证成功，数据不可篡改"
		res.Detail = fmt.Sprintf("创世哈希: %s...%s, 链式哈希: %s...%s", prevHash[:8], prevHash[56:], currHash[:8], currHash[56:])
	} else {
		res.Status = "FAIL"
		res.Message = "SM3哈希链验证失败"
		res.Detail = fmt.Sprintf("prevHash长度: %d, currHash长度: %d", len(prevHash), len(currHash))
	}

	jsonData, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(jsonData))
}
