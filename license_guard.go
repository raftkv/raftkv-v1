package main

import (
	"bufio"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

const licenseFile = "license.key"

// FingerprintAnchorEnvKey 指纹稳定锚点环境变量键名（V2.5 商业版新增）。
//
// 背景：容器化/云原生环境（Docker/K8s）无稳定 DMI 与 machine-id，且网卡 MAC
// 在容器重启后会漂移，导致"指纹 = sha256(MAC|CPU|hostname)"每次重启都变化，
// 商业授权硬件绑定因此必然失效（合法客户也无法通过校验）。
//
// 修复：运维在部署时注入此锚点（宿主机唯一 ID / K8s Node 名称 / 固定 UUID），
// 使其与授权签发时绑定的指纹保持一致。锚点一旦注入即成为指纹的第一优先级来源，
// 彻底消除重启漂移。未注入时自动回退到物理特征（DMI→machine-id→CPU Serial）。
const FingerprintAnchorEnvKey = "RAFTKV_FP_ANCHOR"

const embeddedPublicKeyPEM = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5oD3IZkgdstRVsO9vODa
ivXZ7U8tAzV5e09Mg/KTkMti7UGBJIGfmgcAVgeWKVaNvWtFlSKMl2/ZVWA1GA5C
R6IOk190udc3+lTY+rIWMooCIfefDw3oBE6GO2olpwoZw0wMbmr+HQT8E9oYfYYi
OyvHzXoSkKpP2YOZ4hwacQG2d9BAPnKp7VWxbejyOWP7/HmedmmHRxerdd5lGKLt
rvD4+0G2Huma8ZUyaF6hMBPRn33AZn1dnuQKOJU8XDhs3XYo9yAsUb5HyyPdlxmq
uT3lS4I6OIgxbt+Ugt0LNoaGK1kVO3m3Bff/TPh0I58/RzmL3we+9BS9iKPzvK96
4QIDAQAB
-----END PUBLIC KEY-----`

var (
	licenseDegraded  = false
	licenseDegReason = ""
	licenseMu        sync.RWMutex
	embeddedPubKey   *rsa.PublicKey
)

var licenseFieldOrder = []string{
	"LICENSE_ID", "PRODUCT", "TYPE", "ISSUED_TO",
	"ISSUED_AT", "EXPIRES_AT", "MAX_NODES", "MODULES",
	"HARDWARE_BINDING", "GRACE_PERIOD_DAYS", "SIGNATURE_ALG",
	"ISSUER", "CONTACT",
}

func init() {
	block, _ := pem.Decode([]byte(embeddedPublicKeyPEM))
	if block == nil {
		panic("内嵌公钥PEM解码失败")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic(fmt.Sprintf("解析内嵌公钥失败: %v", err))
	}
	var ok bool
	embeddedPubKey, ok = pub.(*rsa.PublicKey)
	if !ok {
		panic("内嵌公钥不是RSA类型")
	}
}

func IsDegradedMode() (bool, string) {
	licenseMu.RLock()
	defer licenseMu.RUnlock()
	return licenseDegraded, licenseDegReason
}

// =========================================================================
// 双模式授权防线（2026-09-01 姜总裁决二）
//
// LICENSE_FAIL_MODE 环境变量决定授权校验失败时引擎的处置语义：
//   closed（出厂默认）→ Fail-Closed：log.Fatal 拒绝启动，进程终止
//   open             → Fail-Open  ：降级为只读模式继续运行
//
// 商业与法务约束（不可绕过）：
//   · 默认值恒为 closed；空值、非法值一律回落 closed，绝不因配置歧义降级为 open
//   · open 模式仅在甲方已签署并生效《授权到期降级只读补充条款》后方可开启
//   · 未签补充条款而擅自开启 open 模式，构成对主合同免责条款的实质性违反
// =========================================================================

const (
	// LicenseFailModeEnvKey 授权失败处理模式的环境变量键名
	LicenseFailModeEnvKey = "LICENSE_FAIL_MODE"
	// LicenseFailModeClosed 拒绝启动（Fail-Closed）
	LicenseFailModeClosed = "closed"
	// LicenseFailModeOpen 降级只读运行（Fail-Open）
	LicenseFailModeOpen = "open"
	// DefaultLicenseFailMode 出厂默认模式，恒为 closed，禁止改动
	DefaultLicenseFailMode = LicenseFailModeClosed
)

// LicenseFailMode 解析当前授权失败处理模式。
// 解析策略为严格白名单 + 大小写敏感精确匹配：仅取值严格等于 "open" 时返回 open，
// 其余一切取值（含空值、大小写变体如 "OPEN"/"Open"、拼错值如 "opne"）
// 一律回落至 closed，杜绝"配置打错字即静默降级为 open"的法务风险。
// 回落方向恒为安全侧：宁可拒绝启动，不可无授权运行。
func LicenseFailMode() string {
	switch strings.TrimSpace(os.Getenv(LicenseFailModeEnvKey)) {
	case LicenseFailModeOpen:
		return LicenseFailModeOpen
	case LicenseFailModeClosed:
		return LicenseFailModeClosed
	default:
		return DefaultLicenseFailMode
	}
}

// LicenseFailModeRaw 返回环境变量原始取值，仅用于启动日志审计留痕。
func LicenseFailModeRaw() string {
	return os.Getenv(LicenseFailModeEnvKey)
}

// SetDegradedMode 置位全局降级只读状态（写锁，线程安全）。
// 调用前置条件：LicenseFailMode() == open 且甲方补充条款已生效。
// 置位后由 licenseGuardInterceptor 与 HandleAddNode/HandleRemoveNode 拦截写入。
func SetDegradedMode(reason string) {
	licenseMu.Lock()
	defer licenseMu.Unlock()
	licenseDegraded = true
	licenseDegReason = reason
}

func GetMachineFingerprint() string {
	// 1. 稳定锚点（最高优先级）：运维注入的稳定物理特征，消除容器重启漂移
	if anchor := strings.TrimSpace(os.Getenv(FingerprintAnchorEnvKey)); anchor != "" {
		h := sha256.Sum256([]byte("raftkv-anchor:" + anchor))
		return fmt.Sprintf("%x", h[:16])
	}
	// 2. Linux DMI product_uuid（物理机/虚拟机稳定，不漂移）
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/sys/class/dmi/id/product_uuid"); err == nil {
			fp := strings.TrimSpace(string(data))
			if len(fp) > 8 {
				return fp
			}
		}
	}
	// 3. 稳定 CPU/machine-id 标识（优先于漂移的 MAC，容器挂载持久卷后 machine-id 稳定）
	if cpuPart := getCPUIdentifier(); cpuPart != "" && cpuPart != "nocpu" {
		h := sha256.Sum256([]byte("raftkv-cpu:" + cpuPart))
		return fmt.Sprintf("%x", h[:16])
	}
	// 4. 兜底：MAC + hostname（最不稳定，仅在无任何稳定特征可用时回退）
	macPart := getPrimaryMAC()
	hostname, _ := os.Hostname()
	raw := macPart + "|" + hostname
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h[:16])
}

func getPrimaryMAC() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "nomac"
	}
	for _, iface := range ifaces {
		if len(iface.HardwareAddr) == 6 && iface.HardwareAddr[0]&0x01 == 0 {
			return fmt.Sprintf("%x", iface.HardwareAddr)
		}
	}
	for _, iface := range ifaces {
		if len(iface.HardwareAddr) == 6 {
			return fmt.Sprintf("%x", iface.HardwareAddr)
		}
	}
	return "nomac"
}

func getCPUIdentifier() string {
	// Linux：直接读文件系统，不依赖 bash/grep/awk（容器最小镜像通常无这些工具，
	// 这是此前 machine-id 读取失败的根因之一）
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/etc/machine-id"); err == nil {
			v := strings.TrimSpace(string(data))
			if len(v) > 8 {
				return v
			}
		}
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "Serial") {
					fields := strings.Fields(line)
					if len(fields) >= 3 {
						return fields[2]
					}
				}
			}
		}
	}
	if runtime.GOOS == "windows" {
		out, err := exec.Command("wmic", "cpu", "get", "ProcessorId", "/value").Output()
		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "ProcessorId=") {
					return strings.TrimPrefix(line, "ProcessorId=")
				}
			}
		}
	}
	return "nocpu"
}

type LicenseInfo struct {
	LicenseID       string
	Product         string
	Type            string
	IssuedTo        string
	IssuedAt        time.Time
	ExpiresAt       time.Time
	MaxNodes        int
	Modules         string
	HardwareBinding string
	GracePeriodDays int
	SignatureAlg    string
	Signature       string
	Issuer          string
	Contact         string
	Fingerprint     string
	Days            int
	rawFields       map[string]string
}

func parseLicenseFile(data string) *LicenseInfo {
	info := &LicenseInfo{rawFields: make(map[string]string)}
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		idx := strings.Index(line, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		info.rawFields[key] = val

		switch key {
		case "LICENSE_ID":
			info.LicenseID = val
		case "PRODUCT":
			info.Product = val
		case "TYPE":
			info.Type = val
		case "ISSUED_TO":
			info.IssuedTo = val
		case "ISSUED_AT":
			if t, err := time.Parse(time.RFC3339, val); err == nil {
				info.IssuedAt = t
			} else if t, err := time.Parse("2006-01-02", val); err == nil {
				info.IssuedAt = t
			}
		case "EXPIRES_AT":
			if t, err := time.Parse(time.RFC3339, val); err == nil {
				info.ExpiresAt = t
			} else if t, err := time.Parse("2006-01-02", val); err == nil {
				info.ExpiresAt = t
			}
		case "MAX_NODES":
			fmt.Sscanf(val, "%d", &info.MaxNodes)
		case "MODULES":
			info.Modules = val
		case "HARDWARE_BINDING":
			info.HardwareBinding = val
		case "GRACE_PERIOD_DAYS":
			fmt.Sscanf(val, "%d", &info.GracePeriodDays)
		case "SIGNATURE_ALG":
			info.SignatureAlg = val
		case "SIGNATURE":
			info.Signature = val
		case "ISSUER":
			info.Issuer = val
		case "CONTACT":
			info.Contact = val
		case "FINGERPRINT":
			info.Fingerprint = val
		case "DAYS":
			fmt.Sscanf(val, "%d", &info.Days)
		}
	}
	return info
}

func rebuildSignContent(info *LicenseInfo) []byte {
	var sb strings.Builder
	for _, key := range licenseFieldOrder {
		if val, ok := info.rawFields[key]; ok {
			sb.WriteString(key)
			sb.WriteString("=")
			sb.WriteString(val)
			sb.WriteString("\n")
		}
	}
	return []byte(sb.String())
}

func verifyRSASignature(info *LicenseInfo) error {
	if info.Signature == "" {
		return fmt.Errorf("授权文件缺少RSA签名字段 SIGNATURE")
	}

	signContent := rebuildSignContent(info)
	hashed := sha256.Sum256(signContent)

	sigBytes, err := base64.StdEncoding.DecodeString(info.Signature)
	if err != nil {
		return fmt.Errorf("签名Base64解码失败: %w", err)
	}

	if err := rsa.VerifyPKCS1v15(embeddedPubKey, crypto.SHA256, hashed[:], sigBytes); err != nil {
		return fmt.Errorf("RSA数字签名验证失败，授权文件可能被篡改: %w", err)
	}

	return nil
}

func VerifyLicense() error {
	fp := GetMachineFingerprint()
	data, err := os.ReadFile(licenseFile)
	if err != nil {
		return fmt.Errorf("未找到授权文件 license.key: %w\n当前机器指纹: %s\n请使用 generate_license_tool 生成授权文件", err, fp)
	}

	rawData := strings.TrimSpace(string(data))
	info := parseLicenseFile(rawData)

	if info.Signature == "" {
		return fmt.Errorf("授权文件缺少RSA签名，拒绝启动(Fail-Closed)\n当前机器指纹: %s\n请使用 generate_license_tool 生成带RSA-2048签名的授权文件", fp)
	}
	if err := verifyRSASignature(info); err != nil {
		return fmt.Errorf("授权验证失败: %w", err)
	}

	if info.Type == "COMMERCIAL" && info.HardwareBinding != "" && info.HardwareBinding != "DEMO-UNBOUND" {
		if info.HardwareBinding != fp {
			return fmt.Errorf("商业授权硬件绑定不匹配\n当前机器指纹: %s\n授权绑定指纹: %s\n请联系供应商获取正确授权", fp, info.HardwareBinding)
		}
	}

	if info.Type == "DEMO" {
		fmt.Println()
		fmt.Println("╔══════════════════════════════════════════════╗")
		fmt.Println("║  ⚠ 演示授权模式 (DEMO)                        ║")
		fmt.Println("║  本密钥仅限演示评估，禁止用于生产环境          ║")
		fmt.Println("║  演示密钥不绑定硬件指纹                        ║")
		fmt.Println("╚══════════════════════════════════════════════╝")
		fmt.Println()
	}

	now := time.Now().UTC()

	if !info.IssuedAt.IsZero() && now.Before(info.IssuedAt) {
		return fmt.Errorf("检测到时间回拨攻击，拒绝启动(Fail-Closed)\n当前时间: %s 早于签发时间: %s",
			now.Format("2006-01-02 15:04:05 UTC"), info.IssuedAt.Format("2006-01-02 15:04:05 UTC"))
	}

	if !info.ExpiresAt.IsZero() && now.After(info.ExpiresAt) {
		return fmt.Errorf("授权已于 %s 过期，拒绝启动(Fail-Closed)", info.ExpiresAt.Format("2006-01-02"))
	}

	if !info.ExpiresAt.IsZero() {
		daysLeft := int(time.Until(info.ExpiresAt).Hours() / 24)
		if daysLeft <= 7 {
			fmt.Println()
			fmt.Printf("WARNING: 授权即将于 %s 到期（剩余 %d 天），请及时续费\n",
				info.ExpiresAt.Format("2006-01-02"), daysLeft)
			fmt.Println()
		}
	}

	return nil
}

func PrintFingerprint() {
	fp := GetMachineFingerprint()
	fmt.Printf("[授权] 当前机器指纹: %s\n", fp)
}
