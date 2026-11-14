package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestLoadWorkerClientCertificateAcceptsProtectedKeyAndPublicCertificateFiles(t *testing.T) {
	clientCert, _ := runtimeTestCertificate(t, "worker-client", true)
	certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
	if err := os.Chmod(certPath, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []os.FileMode{0o600, 0o400} {
		t.Run(fmt.Sprintf("key mode %o", mode), func(t *testing.T) {
			if err := os.Chmod(keyPath, mode); err != nil {
				t.Fatal(err)
			}
			loaded, err := restserver.LoadWorkerClientCertificate(certPath, keyPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(loaded.Certificate) != 1 || loaded.PrivateKey == nil {
				t.Fatalf("loaded certificate = %+v, want one certificate and a private key", loaded)
			}
		})
	}
}

func TestLoadWorkerClientCertificateRejectsUnsafePrivateKeyFiles(t *testing.T) {
	clientCert, _ := runtimeTestCertificate(t, "worker-client", true)
	for _, test := range []struct {
		name string
		make func(*testing.T, string) string
	}{
		{"missing", func(t *testing.T, _ string) string { return filepath.Join(t.TempDir(), "missing.key") }},
		{"symlink", func(t *testing.T, keyPath string) string {
			link := filepath.Join(t.TempDir(), "worker.key")
			if err := os.Symlink(keyPath, link); err != nil {
				t.Fatal(err)
			}
			return link
		}},
		{"symlink parent", func(t *testing.T, keyPath string) string {
			linkParent := filepath.Join(t.TempDir(), "linked-dir")
			if err := os.Symlink(filepath.Dir(keyPath), linkParent); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(linkParent, filepath.Base(keyPath))
		}},
		{"directory", func(t *testing.T, _ string) string { return t.TempDir() }},
		{"FIFO", func(t *testing.T, _ string) string {
			path := filepath.Join(t.TempDir(), "worker.key")
			if err := syscall.Mkfifo(path, 0o600); err != nil {
				t.Skipf("cannot create FIFO: %v", err)
			}
			return path
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			certPath, validKeyPath := writeWorkerClientCredentials(t, clientCert)
			keyPath := test.make(t, validKeyPath)
			assertWorkerClientCertificateRejected(t, certPath, keyPath)
		})
	}

	for _, mode := range []os.FileMode{0o640, 0o604, 0o200, 0o700} {
		t.Run(fmt.Sprintf("unsafe mode %o", mode), func(t *testing.T) {
			certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
			if err := os.Chmod(keyPath, mode); err != nil {
				t.Fatal(err)
			}
			assertWorkerClientCertificateRejected(t, certPath, keyPath)
		})
	}

	t.Run("wrong owner", func(t *testing.T) {
		certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
		wrongUID := 1
		if os.Geteuid() == wrongUID {
			wrongUID = 2
		}
		if err := os.Chown(keyPath, wrongUID, -1); err != nil {
			t.Skipf("cannot set key file to another UID: %v", err)
		}
		assertWorkerClientCertificateRejected(t, certPath, keyPath)
	})

	t.Run("oversized", func(t *testing.T) {
		certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
		file, err := os.OpenFile(keyPath, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(make([]byte, 1<<20)); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		assertWorkerClientCertificateRejected(t, certPath, keyPath)
	})
}

func TestLoadWorkerClientCertificateErrorsDoNotDisclosePathsOrKey(t *testing.T) {
	clientCert, _ := runtimeTestCertificate(t, "worker-client", true)
	certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
	if err := os.Chmod(keyPath, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := restserver.LoadWorkerClientCertificate(certPath, keyPath)
	if err == nil {
		t.Fatal("unsafe key file was accepted")
	}
	if strings.Contains(err.Error(), certPath) || strings.Contains(err.Error(), keyPath) || strings.Contains(err.Error(), "PRIVATE KEY") {
		t.Fatalf("error disclosed key material or configured path: %v", err)
	}
}

func TestNewWorkerSessionClientDoesNotApplyPrivateKeyPolicyToPublicServerCA(t *testing.T) {
	clientCert, serverCA := runtimeTestCertificate(t, "worker-client", true)
	certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
	caPath := filepath.Join(t.TempDir(), "controller-ca.crt")
	if err := os.WriteFile(caPath, serverCA, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := newWorkerSessionClient(restserver.WorkerControllerConfig{BaseURL: "https://controller.example", ClientCertFile: certPath, ClientKeyFile: keyPath, ServerCAFile: caPath}); err != nil {
		t.Fatalf("newWorkerSessionClient() rejected public CA file: %v", err)
	}
}

func TestNewWorkerSessionClientSanitizesPrivateKeyStartupErrors(t *testing.T) {
	clientCert, serverCA := runtimeTestCertificate(t, "worker-client", true)
	certPath, keyPath := writeWorkerClientCredentials(t, clientCert)
	if err := os.Chmod(keyPath, 0o644); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(t.TempDir(), "controller-ca.crt")
	if err := os.WriteFile(caPath, serverCA, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := newWorkerSessionClient(restserver.WorkerControllerConfig{BaseURL: "https://controller.example", ClientCertFile: certPath, ClientKeyFile: keyPath, ServerCAFile: caPath})
	if err == nil || err.Error() != "load worker controller client certificate" {
		t.Fatalf("newWorkerSessionClient() error = %v, want fixed sanitized startup error", err)
	}
	if strings.Contains(err.Error(), keyPath) {
		t.Fatalf("startup error exposed key file details: %v", err)
	}
}

func writeWorkerClientCredentials(t *testing.T, certificate tls.Certificate) (string, string) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "worker.crt"), filepath.Join(dir, "worker.key")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	if err := os.WriteFile(certPath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func assertWorkerClientCertificateRejected(t *testing.T, certPath, keyPath string) {
	t.Helper()
	_, err := restserver.LoadWorkerClientCertificate(certPath, keyPath)
	if err == nil {
		t.Fatalf("LoadWorkerClientCertificate(%q, key) accepted unsafe private key", certPath)
	}
	if strings.Contains(err.Error(), certPath) || strings.Contains(err.Error(), keyPath) {
		t.Fatalf("error disclosed configured path: %v", err)
	}
}
