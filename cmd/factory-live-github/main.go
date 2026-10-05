package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/leehosanganson/factory/internal/livegithub"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "mint-token" {
		if _, err := livegithub.LoadTargetConfig(os.Getenv); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := livegithub.MintAndExportInstallationToken(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 2 || os.Args[1] != "validate" {
		fmt.Fprintln(os.Stderr, "usage: factory-live-github <validate|mint-token>")
		os.Exit(2)
	}
	if err := livegithub.ValidateTarget(os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("live GitHub sandbox configuration validated")
}
