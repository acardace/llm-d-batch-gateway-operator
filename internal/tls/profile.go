package tls

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	configv1 "github.com/openshift/api/config/v1"
	tlspkg "github.com/openshift/controller-runtime-common/pkg/tls"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	bootstrapTimeout  = 10 * time.Second
	defaultNextProtos = "h2,http/1.1"
)

// Result holds the resolved TLS configuration from the cluster profile.
type Result struct {
	Profile          configv1.TLSProfileSpec
	ProfileFetched   bool
	Adherence        configv1.TLSAdherencePolicy
	AdherenceFetched bool
	TLSOpts          []func(*tls.Config)
}

// ProfileValues holds the resolved cluster TLS profile as strings
// ready for injection into helm chart values.
type ProfileValues struct {
	MinVersion   string
	CipherSuites string
	NextProtos   string
}

// ProfileValuesFromSpec converts a configv1.TLSProfileSpec into
// string values suitable for helm chart injection. It returns an error when
// a non-TLS 1.3 profile contains no cipher suite accepted by Batch Gateway.
func ProfileValuesFromSpec(spec configv1.TLSProfileSpec) (ProfileValues, []string, error) {
	vals := ProfileValues{
		NextProtos: defaultNextProtos,
	}
	if spec.MinTLSVersion != "" {
		vals.MinVersion = string(spec.MinTLSVersion)
		if spec.MinTLSVersion != configv1.VersionTLS12 && spec.MinTLSVersion != configv1.VersionTLS13 {
			return vals, nil, fmt.Errorf("TLS profile minimum version %q is not supported by Batch Gateway; supported values are %q and %q", spec.MinTLSVersion, configv1.VersionTLS12, configv1.VersionTLS13)
		}
	}
	if len(spec.Ciphers) > 0 {
		cipherSuites, unsupported := batchGatewayCipherSuiteNames(spec)
		if spec.MinTLSVersion != configv1.VersionTLS13 && !hasTLS12CipherSuite(cipherSuites) {
			details := unsupported
			if len(details) == 0 {
				details = spec.Ciphers
			}
			return vals, unsupported, fmt.Errorf("TLS profile contains no cipher suite usable with TLS 1.2: %s", strings.Join(details, ", "))
		}
		vals.CipherSuites = strings.Join(cipherSuites, ",")
		return vals, unsupported, nil
	}
	return vals, nil, nil
}

// batchGatewayCipherSuiteNames converts OpenShift TLS profile cipher names to
// names accepted by the Batch Gateway API server. It preserves the original
// cipher name when reporting suites that Batch Gateway does not support.
func batchGatewayCipherSuiteNames(spec configv1.TLSProfileSpec) ([]string, []string) {
	if spec.MinTLSVersion == configv1.VersionTLS13 {
		return nil, nil
	}

	supported := make(map[string]struct{}, len(tls.CipherSuites()))
	for _, suite := range tls.CipherSuites() {
		supported[suite.Name] = struct{}{}
	}

	tlsConfigFn, unsupportedProfileCiphers := tlspkg.NewTLSConfigFromProfile(spec)
	tlsConfig := &tls.Config{}
	tlsConfigFn(tlsConfig)

	names := make([]string, 0, len(tlsConfig.CipherSuites))
	unsupported := make([]string, 0)
	codeIndex := 0
	unsupportedIndex := 0
	for _, cipher := range spec.Ciphers {
		if unsupportedIndex < len(unsupportedProfileCiphers) && unsupportedProfileCiphers[unsupportedIndex] == cipher {
			unsupported = append(unsupported, cipher)
			unsupportedIndex++
			continue
		}
		if codeIndex >= len(tlsConfig.CipherSuites) {
			unsupported = append(unsupported, cipher)
			continue
		}

		name := tls.CipherSuiteName(tlsConfig.CipherSuites[codeIndex])
		codeIndex++
		if _, ok := supported[name]; ok {
			names = append(names, name)
			continue
		}
		unsupported = append(unsupported, cipher)
	}
	return names, unsupported
}

func hasTLS12CipherSuite(names []string) bool {
	for _, suite := range tls.CipherSuites() {
		for _, name := range names {
			if suite.Name != name {
				continue
			}
			for _, version := range suite.SupportedVersions {
				if version == tls.VersionTLS12 {
					return true
				}
			}
		}
	}
	return false
}

// Resolve fetches the cluster TLS profile and adherence policy,
// returning a Result with TLS options for controller-runtime servers.
// On non-OpenShift clusters or transient errors, falls back to Intermediate defaults.
func Resolve(ctx context.Context, k8sClient client.Client, logger logr.Logger) (*Result, error) {
	bootstrapCtx, cancel := context.WithTimeout(ctx, bootstrapTimeout)
	defer cancel()

	result := &Result{}

	profile, err := tlspkg.FetchAPIServerTLSProfile(bootstrapCtx, k8sClient)
	if err != nil {
		switch {
		case apimeta.IsNoMatchError(err):
			logger.Info("TLS profile not available (non-OpenShift cluster)")
		case apierrors.IsNotFound(err):
			logger.Info("APIServer resource not found, using defaults")
		case apierrors.IsServiceUnavailable(err),
			apierrors.IsTimeout(err),
			apierrors.IsServerTimeout(err),
			apierrors.IsTooManyRequests(err),
			errors.Is(err, context.DeadlineExceeded):
			logger.Info("Transient API error, using Intermediate defaults", "error", err)
			result.ProfileFetched = true
		default:
			return nil, fmt.Errorf("reading APIServer TLS profile: %w", err)
		}
		profile = *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	} else {
		result.ProfileFetched = true
	}
	result.Profile = profile

	tlsConfigFn, unsupported := tlspkg.NewTLSConfigFromProfile(profile)
	if len(unsupported) > 0 {
		logger.Info("TLS profile contains unsupported ciphers", "unsupported", unsupported)
	}
	result.TLSOpts = append(result.TLSOpts, tlsConfigFn)
	result.TLSOpts = append(result.TLSOpts, func(c *tls.Config) {
		c.NextProtos = []string{"h2", "http/1.1"}
	})

	adherence, adherenceErr := tlspkg.FetchAPIServerTLSAdherencePolicy(bootstrapCtx, k8sClient)
	if adherenceErr != nil {
		switch {
		case apimeta.IsNoMatchError(adherenceErr):
			logger.Info("TLS adherence API not available (non-OpenShift or pre-4.22 cluster)")
		case apierrors.IsNotFound(adherenceErr):
			logger.Info("APIServer resource not found for adherence, skipping")
		case apierrors.IsServiceUnavailable(adherenceErr),
			apierrors.IsTimeout(adherenceErr),
			apierrors.IsServerTimeout(adherenceErr),
			apierrors.IsTooManyRequests(adherenceErr),
			errors.Is(adherenceErr, context.DeadlineExceeded):
			logger.Info("Transient error fetching TLS adherence policy, watcher will retry", "error", adherenceErr)
		default:
			return nil, fmt.Errorf("unable to read TLS adherence policy: %w", adherenceErr)
		}
	} else {
		result.AdherenceFetched = true
	}
	result.Adherence = adherence

	return result, nil
}

// SetupWatcher registers a SecurityProfileWatcher that triggers a graceful
// restart when the TLS profile or adherence policy changes.
func SetupWatcher(mgr manager.Manager, result *Result, cancel context.CancelFunc, logger logr.Logger) error {
	if !result.ProfileFetched {
		return nil
	}

	watcher := &tlspkg.SecurityProfileWatcher{
		Client:                mgr.GetClient(),
		InitialTLSProfileSpec: result.Profile,
		OnProfileChange: func(_ context.Context, _, _ configv1.TLSProfileSpec) {
			logger.Info("TLS profile changed, initiating shutdown to reload")
			cancel()
		},
	}
	if result.AdherenceFetched {
		watcher.InitialTLSAdherencePolicy = result.Adherence
		watcher.OnAdherencePolicyChange = func(_ context.Context, _, _ configv1.TLSAdherencePolicy) {
			logger.Info("TLS adherence policy changed, initiating shutdown to reload")
			cancel()
		}
	}

	if err := watcher.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up TLS profile watcher: %w", err)
	}

	logger.Info("TLS profile watcher registered")
	return nil
}
