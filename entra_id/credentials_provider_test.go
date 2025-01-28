package entra_id_test

import (
	"errors"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/go-redis/entra_id"
)

type MockEntraIdIdentityProvider struct {
	Token string
	TTL   time.Duration
	Error error
}

func (m *MockEntraIdIdentityProvider) RequestToken(forceRefresh bool) (string, time.Duration, error) {
	if m.Error != nil {
		return "", 0, m.Error
	}
	return m.Token, m.TTL, nil
}

var _ = Describe("EntraIdCredentialsProvider", func() {
	var (
		provider    *entra_id.EntraIdCredentialsProvider
		mockIDP     *MockEntraIdIdentityProvider
		refreshRate time.Duration
	)

	BeforeEach(func() {
		refreshRate = 1 * time.Minute
		mockIDP = &MockEntraIdIdentityProvider{
			Token: "mock-token",
			TTL:   10 * time.Second,
			Error: nil,
		}
		provider = entra_id.NewEntraIdCredentialsProvider(mockIDP, refreshRate, true)
	})

	AfterEach(func() {
		provider.Stop()
	})

	Context("Initial Token Retrieval", func() {
		It("should retrieve a valid token from the identity provider", func() {
			token, err := provider.GetCredentials()
			Expect(err).To(BeNil())
			Expect(token).To(Equal("mock-token"))
		})

		It("should return an error if the identity provider fails", func() {
			mockIDP.Error = errors.New("identity provider failure")
			_, err := provider.GetCredentials()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("identity provider failure"))
		})
	})

	Context("Automatic Token Renewal", func() {
		It("should automatically refresh the token when it expires", func() {
			token, err := provider.GetCredentials()
			Expect(err).To(BeNil())
			Expect(token).To(Equal("mock-token"))

			time.Sleep(11 * time.Second) // Wait for token expiry and auto-refresh

			newToken, err := provider.GetCredentials()
			Expect(err).To(BeNil())
			Expect(newToken).To(Equal("mock-token")) // Mock still returns the same token
		})
	})

	Context("Stop Streaming", func() {
		It("should stop token renewal and clean up resources when Stop is called", func() {
			provider.GetCredentials() // Start streaming
			// Ensure no further actions or panics occur after stopping, the stopping ocuur in the AfterEach
		})
	})
})
