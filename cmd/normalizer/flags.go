package main

import (
	"errors"
	"flag"
	"io"

	"github.com/jensilin/KubeScaleSense/internal/normalizer"
)

// errFlagHelp marks -h, which is a successful exit rather than a usage error.
var errFlagHelp = errors.New("help requested")

type options struct {
	showVersion  bool
	validateOnly bool
}

// parseFlags keeps the flag set tiny on purpose: everything tunable is an
// environment variable so that a ConfigMap change is the only thing needed to
// retune the workload, with no rebuilt image and no edited manifest.
func parseFlags(args []string, stderr io.Writer) (options, error) {
	var opts options

	fs := flag.NewFlagSet("normalizer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.BoolVar(&opts.showVersion, "version", false, "print the version and exit")
	fs.BoolVar(&opts.validateOnly, "validate", false,
		"validate the "+normalizer.EnvPrefix+"* configuration and exit without listening")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return opts, errFlagHelp
		}
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, errors.New("unexpected positional arguments: the Normalizer is configured through " +
			normalizer.EnvPrefix + "* environment variables")
	}
	return opts, nil
}
