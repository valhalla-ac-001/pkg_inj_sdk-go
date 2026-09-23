package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"time"
)

const (
	DefaultOfacListURL = "https://raw.githubusercontent.com/InjectiveLabs/injective-lists/refs/heads/master/json/wallets/ofacAndRestricted.json"
)

var (
	OfacListPath     = "injective_data"
	OfacListFilename = "ofac.json"
)

type OfacChecker struct {
	ofacListPath string
	ofacList     map[string]bool
}

func NewOfacChecker() (*OfacChecker, error) {
	checker := &OfacChecker{
		ofacListPath: GetOfacListPath(),
	}
	if _, err := os.Stat(checker.ofacListPath); os.IsNotExist(err) {
		if err := DownloadOfacList(); err != nil {
			return nil, err
		}
	}
	if err := checker.loadOfacList(); err != nil {
		return nil, err
	}
	return checker, nil
}

// NewOfacCheckerContext is the bounded, cancellable variant used by clients
// whose construction is governed by a caller context. The legacy constructor
// intentionally retains its historical unbounded HTTP behavior.
func NewOfacCheckerContext(ctx context.Context) (*OfacChecker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	checker := &OfacChecker{ofacListPath: GetOfacListPath()}
	if _, err := os.Stat(checker.ofacListPath); os.IsNotExist(err) {
		requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := DownloadOfacListContext(requestCtx)
		cancel()
		if err != nil {
			return nil, err
		}
	}
	if err := checker.loadOfacList(); err != nil {
		return nil, err
	}
	return checker, nil
}

func GetOfacListPath() string {
	return path.Join(OfacListPath, OfacListFilename)
}

func DownloadOfacList() error {
	resp, err := http.Get(DefaultOfacListURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download OFAC list, status code: %d", resp.StatusCode)
	}

	fmt.Printf("Writing OFAC list to file to %s\n", OfacListPath)
	if err := os.MkdirAll(OfacListPath, 0755); err != nil { // nolint:gocritic // 0755 is the correct permission
		return err
	}
	outFile, err := os.Create(GetOfacListPath())
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return err
	}
	_, err = outFile.WriteString("\n")
	if err != nil {
		return err
	}
	return nil
}

// DownloadOfacListContext atomically installs the list so cancellation or a
// failed transfer cannot leave a partially-written file for a later client.
func DownloadOfacListContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, DefaultOfacListURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download OFAC list, status code: %d", resp.StatusCode)
	}
	if err := os.MkdirAll(OfacListPath, 0755); err != nil { // nolint:gocritic // 0755 is the correct permission
		return err
	}
	tmp, err := os.CreateTemp(OfacListPath, ".ofac-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err = io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return err
	}
	if _, err = tmp.WriteString("\n"); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(tmpName, GetOfacListPath())
}

func (oc *OfacChecker) loadOfacList() error {
	file, err := os.ReadFile(oc.ofacListPath)
	if err != nil {
		return err
	}
	var list []string
	err = json.Unmarshal(file, &list)
	if err != nil {
		return err
	}
	oc.ofacList = make(map[string]bool)
	for _, item := range list {
		oc.ofacList[item] = true
	}
	return nil
}

func (oc *OfacChecker) IsBlacklisted(address string) bool {
	return oc.ofacList[address]
}
