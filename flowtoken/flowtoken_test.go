package flowtoken_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/flowtoken"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFlowtoken(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "flowtoken")
}

var _ = Describe("Test flowtoken", func() {
	It("all fail, success rate less than 0.02", func() {
		conf := &flowtoken.Config{
			InitCwnd: 400,
			MinCwnd:  3,
			MaxCwnd:  40000,
		}
		ft := flowtoken.NewFlowToken("127.0.0.1:13000", conf)

		exaustCount := 0
		lock := &sync.Mutex{}
		wg := &sync.WaitGroup{}
		lastWnd := conf.InitCwnd
		for l := 0; l <= 10; l++ {
			for m := 0; m <= 100; m++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for n := 0; n <= 100; n++ {
						t, err := ft.GetToken()
						_, ok := flowtoken.IsTokenExhaustedError(err)
						if ok {
							lock.Lock()
							exaustCount++
							lock.Unlock()
							continue
						}

						r := rand.Int63n(10000)
						if r < 10 {
							t.Succ()
						} else {
							t.Fail()
						}
					}

					return
				}()
			}

			wg.Wait()

			snap := ft.GetSnapshot()
			Ω(snap.Cwnd).Should(BeNumerically("<=", lastWnd))
			lastWnd >>= 1
			if lastWnd < conf.MinCwnd {
				lastWnd = conf.MinCwnd
			}

			time.Sleep(1 * time.Second)
		}

		Ω(exaustCount).Should(BeNumerically(">", 0))
		snap := ft.GetSnapshot()
		Ω(snap.Cwnd).Should(BeNumerically("==", conf.MinCwnd))
	})

	It("all succ, success rate more than 0.98", func() {
		conf := &flowtoken.Config{
			InitCwnd: 400,
			MinCwnd:  3,
			MaxCwnd:  40000,
		}
		ft := flowtoken.NewFlowToken("127.0.0.1:13000", conf)

		exaustCount := 0
		lock := &sync.Mutex{}
		wg := &sync.WaitGroup{}
		maxSuccCount := 0
		for l := 0; l <= 10; l++ {
			succCount := 0
			for m := 0; m <= 100; m++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for n := 0; n <= 100; n++ {
						t, err := ft.GetToken()
						_, ok := flowtoken.IsTokenExhaustedError(err)
						if ok {
							lock.Lock()
							exaustCount++
							lock.Unlock()
							continue
						}

						r := rand.Int63n(10000)
						if r > 10 {
							lock.Lock()
							succCount++
							lock.Unlock()
							t.Succ()
						} else {
							t.Fail()
						}
					}

					return
				}()
			}

			wg.Wait()

			snap := ft.GetSnapshot()
			if l == 0 {
				Ω(snap.Cwnd).Should(BeNumerically("==", conf.InitCwnd))
			} else {
				Ω(snap.Cwnd).Should(BeNumerically(">=", maxSuccCount))
			}

			if maxSuccCount < succCount {
				maxSuccCount = succCount
			}

			time.Sleep(1 * time.Second)
		}

		Ω(exaustCount).Should(BeNumerically("==", 0))
		snap := ft.GetSnapshot()
		Ω(snap.Cwnd).Should(BeNumerically(">=", maxSuccCount))
	})

	It("partial fail, success rate between 0.02 and 0.98", func() {
		conf := &flowtoken.Config{
			InitCwnd: 100000,
			MinCwnd:  3,
		}
		ft := flowtoken.NewFlowToken("127.0.0.1:13000", conf)

		t, err := ft.GetToken()

		Expect(err).To(BeNil())
		t.Succ()
		time.Sleep(1 * time.Second)

		wg := &sync.WaitGroup{}
		failCount := 0
		lock := &sync.Mutex{}
		for m := 0; m <= 100; m++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for n := 0; n <= 100; n++ {
					t, err := ft.GetToken()
					if err != nil {
						continue
					}

					r := rand.Int63n(10000)
					if r < 3000 {
						lock.Lock()
						failCount++
						lock.Unlock()
						t.Fail()
					} else {
						t.Succ()
					}
				}

				return
			}()
		}

		time.Sleep(1 * time.Second)
		_, err = ft.GetToken()
		Expect(err).To(BeNil())
		snap := ft.GetSnapshot()
		Ω(snap.Cwnd).Should(BeNumerically("==", (conf.InitCwnd - int64(failCount))))
	})

	It("shut down and recover", func() {
		conf := &flowtoken.Config{
			InitCwnd: 400,
			MinCwnd:  3,
			MaxCwnd:  40000,
		}
		ft := flowtoken.NewFlowToken("127.0.0.1:13000", conf)

		wg := &sync.WaitGroup{}
		for l := 0; l <= 10; l++ {
			for m := 0; m <= 100; m++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for n := 0; n <= 100; n++ {
						t, err := ft.GetToken()
						if err != nil {
							continue
						}

						r := rand.Int63n(10000)
						if r < 10 {
							t.Succ()
						} else {
							t.Fail()
						}
					}

					return
				}()
			}

			wg.Wait()

			snap := ft.GetSnapshot()
			Ω(snap.Cwnd).Should(BeNumerically("<=", conf.InitCwnd))

			time.Sleep(1 * time.Second)
		}

		snap := ft.GetSnapshot()
		Ω(snap.Cwnd).Should(BeNumerically("==", conf.MinCwnd))

		lock := &sync.Mutex{}
		maxSuccCount := 0
		for l := 0; l <= 10; l++ {
			succCount := 0
			for m := 0; m <= 100; m++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for n := 0; n <= 100; n++ {
						t, err := ft.GetToken()
						if err != nil {
							continue
						}

						r := rand.Int63n(10000)
						if r > 10 {
							lock.Lock()
							succCount++
							lock.Unlock()
							t.Succ()
						} else {
							t.Fail()
						}
					}

					return
				}()
			}

			wg.Wait()

			snap := ft.GetSnapshot()
			if l == 0 {
				Ω(snap.Cwnd).Should(BeNumerically("==", conf.MinCwnd))
			} else {
				Ω(snap.Cwnd).Should(BeNumerically(">=", maxSuccCount))
			}

			if maxSuccCount < succCount {
				maxSuccCount = succCount
			}

			time.Sleep(1 * time.Second)
		}

		snap = ft.GetSnapshot()
		Ω(snap.Cwnd).Should(BeNumerically(">=", maxSuccCount))
	})

})
