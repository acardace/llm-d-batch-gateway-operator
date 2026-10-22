package tls

import (
	"context"
	"crypto/tls"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestProfileValuesFromSpec(t *testing.T) {
	intermediateProfile := configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
	modernProfile := configv1.TLSProfiles[configv1.TLSProfileModernType]

	tests := []struct {
		name             string
		spec             configv1.TLSProfileSpec
		wantMinVersion   string
		wantCipherSuites string
		wantNextProtos   string
	}{
		{
			name:             "empty spec returns defaults",
			spec:             configv1.TLSProfileSpec{},
			wantMinVersion:   "",
			wantCipherSuites: "",
			wantNextProtos:   defaultNextProtos,
		},
		{
			name:             "Intermediate profile",
			spec:             *intermediateProfile,
			wantMinVersion:   string(intermediateProfile.MinTLSVersion),
			wantCipherSuites: "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
			wantNextProtos:   defaultNextProtos,
		},
		{
			name:             "Modern profile",
			spec:             *modernProfile,
			wantMinVersion:   string(modernProfile.MinTLSVersion),
			wantCipherSuites: "",
			wantNextProtos:   defaultNextProtos,
		},
		{
			name: "custom ciphers",
			spec: configv1.TLSProfileSpec{
				MinTLSVersion: "VersionTLS12",
				Ciphers:       []string{"ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384"},
			},
			wantMinVersion:   "VersionTLS12",
			wantCipherSuites: "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
			wantNextProtos:   defaultNextProtos,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, unsupported, err := ProfileValuesFromSpec(tt.spec)
			if err != nil {
				t.Fatalf("ProfileValuesFromSpec() error = %v", err)
			}
			if len(unsupported) != 0 {
				t.Errorf("unsupported = %v, want none", unsupported)
			}
			if got.MinVersion != tt.wantMinVersion {
				t.Errorf("MinVersion = %q, want %q", got.MinVersion, tt.wantMinVersion)
			}
			if got.CipherSuites != tt.wantCipherSuites {
				t.Errorf("CipherSuites = %q, want %q", got.CipherSuites, tt.wantCipherSuites)
			}
			if got.NextProtos != tt.wantNextProtos {
				t.Errorf("NextProtos = %q, want %q", got.NextProtos, tt.wantNextProtos)
			}
		})
	}
}

func TestProfileValuesFromSpec_IANACipherNames(t *testing.T) {
	spec := configv1.TLSProfileSpec{
		MinTLSVersion: "VersionTLS12",
		Ciphers:       []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"},
	}

	got, unsupported, err := ProfileValuesFromSpec(spec)
	if err != nil {
		t.Fatalf("ProfileValuesFromSpec() error = %v", err)
	}
	if len(unsupported) != 0 {
		t.Errorf("unsupported = %v, want none", unsupported)
	}
	if got.CipherSuites != spec.Ciphers[0] {
		t.Errorf("CipherSuites = %q, want %q", got.CipherSuites, spec.Ciphers[0])
	}
}

func TestProfileValuesFromSpec_MixedSupportedAndUnsupportedCiphers(t *testing.T) {
	spec := configv1.TLSProfileSpec{
		MinTLSVersion: "VersionTLS12",
		Ciphers: []string{
			"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			"TLS_RSA_WITH_AES_128_GCM_SHA256",
		},
	}

	got, unsupported, err := ProfileValuesFromSpec(spec)
	if err != nil {
		t.Fatalf("ProfileValuesFromSpec() error = %v", err)
	}
	if len(unsupported) != 1 || unsupported[0] != spec.Ciphers[1] {
		t.Errorf("unsupported = %v, want [%q]", unsupported, spec.Ciphers[1])
	}
	if got.CipherSuites != spec.Ciphers[0] {
		t.Errorf("CipherSuites = %q, want %q", got.CipherSuites, spec.Ciphers[0])
	}
}

func TestProfileValuesFromSpec_AllUnsupportedCiphers(t *testing.T) {
	spec := configv1.TLSProfileSpec{
		MinTLSVersion: "VersionTLS12",
		Ciphers:       []string{"TLS_RSA_WITH_AES_128_GCM_SHA256"},
	}

	if _, unsupported, err := ProfileValuesFromSpec(spec); err == nil {
		t.Fatal("ProfileValuesFromSpec() error = nil, want an error")
	} else if len(unsupported) != 1 || unsupported[0] != spec.Ciphers[0] {
		t.Errorf("unsupported = %v, want [%q]", unsupported, spec.Ciphers[0])
	}
}

func TestProfileValuesFromSpec_UnknownCipher(t *testing.T) {
	spec := configv1.TLSProfileSpec{
		MinTLSVersion: configv1.VersionTLS12,
		Ciphers:       []string{"not-a-cipher"},
	}

	if _, unsupported, err := ProfileValuesFromSpec(spec); err == nil {
		t.Fatal("ProfileValuesFromSpec() error = nil, want an error")
	} else if len(unsupported) != 1 || unsupported[0] != spec.Ciphers[0] {
		t.Errorf("unsupported = %v, want [%q]", unsupported, spec.Ciphers[0])
	}
}

func TestProfileValuesFromSpec_TLS12RejectsTLS13OnlyCiphers(t *testing.T) {
	spec := configv1.TLSProfileSpec{
		MinTLSVersion: configv1.VersionTLS12,
		Ciphers:       []string{"TLS_AES_128_GCM_SHA256"},
	}

	if _, _, err := ProfileValuesFromSpec(spec); err == nil {
		t.Fatal("ProfileValuesFromSpec() error = nil, want an error")
	}
}

func TestProfileValuesFromSpec_RejectsUnsupportedMinTLSVersion(t *testing.T) {
	for _, version := range []configv1.TLSProtocolVersion{configv1.VersionTLS10, configv1.VersionTLS11} {
		t.Run(string(version), func(t *testing.T) {
			spec := configv1.TLSProfileSpec{MinTLSVersion: version}
			if _, _, err := ProfileValuesFromSpec(spec); err == nil {
				t.Fatalf("ProfileValuesFromSpec() error = nil, want an error for %s", version)
			}
		})
	}
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = configv1.Install(s)
	return s
}

func TestResolve(t *testing.T) {
	logger := klog.NewKlogr()
	intermediateProfile := configv1.TLSProfiles[configv1.TLSProfileIntermediateType]

	// IsNoMatchError (non-OpenShift) requires a real REST mapper and is covered by integration tests.
	tests := []struct {
		name               string
		objects            []runtime.Object
		wantErr            bool
		wantProfileFetched bool
		wantMinVersion     configv1.TLSProtocolVersion
	}{
		{
			name:               "APIServer resource not found",
			wantProfileFetched: false,
			wantMinVersion:     intermediateProfile.MinTLSVersion,
		},
		{
			name: "APIServer exists with custom profile",
			objects: []runtime.Object{
				&configv1.APIServer{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
					Spec: configv1.APIServerSpec{
						TLSSecurityProfile: &configv1.TLSSecurityProfile{
							Type: configv1.TLSProfileModernType,
						},
					},
				},
			},
			wantProfileFetched: true,
			wantMinVersion:     configv1.TLSProfiles[configv1.TLSProfileModernType].MinTLSVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(testScheme())
			for _, obj := range tt.objects {
				builder = builder.WithRuntimeObjects(obj)
			}
			c := builder.Build()

			result, err := Resolve(context.Background(), c, logger)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if result.ProfileFetched != tt.wantProfileFetched {
				t.Errorf("ProfileFetched = %v, want %v", result.ProfileFetched, tt.wantProfileFetched)
			}
			if result.Profile.MinTLSVersion != tt.wantMinVersion {
				t.Errorf("MinTLSVersion = %v, want %v", result.Profile.MinTLSVersion, tt.wantMinVersion)
			}
			if len(result.TLSOpts) == 0 {
				t.Error("TLSOpts should not be empty")
			}
			cfg := &tls.Config{}
			for _, fn := range result.TLSOpts {
				fn(cfg)
			}
			if len(cfg.NextProtos) == 0 {
				t.Error("NextProtos should be set")
			}
		})
	}
}

func TestResolve_TLS13ProfileOmitsCipherSuites(t *testing.T) {
	logger := klog.NewKlogr()
	s := testScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(
		&configv1.APIServer{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
			Spec: configv1.APIServerSpec{
				TLSSecurityProfile: &configv1.TLSSecurityProfile{
					Type: configv1.TLSProfileModernType,
				},
			},
		},
	).Build()

	result, err := Resolve(context.Background(), c, logger)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	vals, _, err := ProfileValuesFromSpec(result.Profile)
	if err != nil {
		t.Fatalf("ProfileValuesFromSpec() error = %v", err)
	}
	if vals.MinVersion != string(configv1.VersionTLS13) {
		t.Errorf("MinVersion = %q, want VersionTLS13", vals.MinVersion)
	}
	if vals.CipherSuites != "" {
		t.Errorf("CipherSuites = %q, want empty for TLS 1.3", vals.CipherSuites)
	}
	if vals.NextProtos != defaultNextProtos {
		t.Errorf("NextProtos = %q, want h2,http/1.1", vals.NextProtos)
	}
}
