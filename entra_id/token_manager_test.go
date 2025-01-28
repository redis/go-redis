package entra_id_test

import (
	"errors"
	"sync"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/go-redis/entra_id"
)

var _ = Describe("TokenManager", func() {
	var (
		tokenManager *entra_id.TokenManager
		mockRefresh  func() (string, time.Duration, error)
	)

	BeforeEach(func() {
		mockRefresh = func() (string, time.Duration, error) {
			return "new-token", 10 * time.Second, nil
		}
		tokenManager = entra_id.NewTokenManager(mockRefresh, 1*time.Minute, true)
	})

	AfterEach(func() {
		tokenManager.StopAutoRefresh()
	})

	Context("Token Refresh", func() {
		It("should refresh the token successfully", func() {
			err := tokenManager.RefreshToken()
			Expect(err).To(BeNil())

			token, valid := tokenManager.GetToken()
			Expect(valid).To(BeTrue())
			Expect(token).To(Equal("new-token"))
		})

		It("should return an error if the refresh function fails", func() {
			failingRefresh := func() (string, time.Duration, error) {
				return "", 0, errors.New("refresh failed")
			}
			tokenManager = entra_id.NewTokenManager(failingRefresh, 1*time.Minute, true)
			err := tokenManager.RefreshToken()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("refresh failed"))
		})
	})

	Context("Token Expiry", func() {
		It("should return false if the token has expired", func() {
			tokenManager.SetToken("expired-token", 1*time.Second)
			time.Sleep(2 * time.Second)

			_, valid := tokenManager.GetToken()
			Expect(valid).To(BeFalse())
		})
	})

	Context("Auto-Refresh", func() {
		It("should automatically refresh the token before it expires", func() {
			refreshed := make(chan struct{})
			mockRefresh := func() (string, time.Duration, error) {
				close(refreshed) // Signal that refresh occurred
				return "new-token", 10 * time.Second, nil
			}

			tokenManager := entra_id.NewTokenManager(mockRefresh, 1*time.Second, false)
			tokenManager.SetToken("old-token", 5*time.Second)
			tokenManager.StartAutoRefresh()

			select {
			case <-refreshed:
				// Token refreshed successfully
			case <-time.After(6 * time.Second):
				Fail("Token refresh did not occur in time")
			}

			token, valid := tokenManager.GetToken()
			Expect(valid).To(BeTrue())
			Expect(token).To(Equal("new-token"))

			tokenManager.StopAutoRefresh()
		})

		It("should stop auto-refresh when StopAutoRefresh is called", func() {
			mockRefresh := func() (string, time.Duration, error) {
				return "new-token", 10 * time.Second, nil
			}

			tokenManager := entra_id.NewTokenManager(mockRefresh, 1*time.Second, false)
			tokenManager.StartAutoRefresh()
			tokenManager.StopAutoRefresh()
			time.Sleep(2 * time.Second)

			Expect(tokenManager.GetToken()).ToNot(BeNil()) // Ensure no panic or issues after stopping
		})
	})

	Context("Concurrency", func() {
		It("should handle concurrent access without race conditions", func() {
			wg := sync.WaitGroup{}
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _ = tokenManager.GetToken()
					tokenManager.SetToken("concurrent-token", 10*time.Second)
				}()
			}
			wg.Wait()
		})
	})

	Context("Telemetry", func() {
		It("should log token expiration and validity during monitoring", func() {
			tokenManager.SetToken("telemetry-token", 2*time.Second)
			go tokenManager.MonitorTelemetry()

			time.Sleep(4 * time.Second)
			// Log verification can be done by capturing logs (if necessary)
		})
	})
})
