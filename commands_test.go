package redis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
)

type TimeValue struct {
	time.Time
}

func (t *TimeValue) ScanRedis(s string) (err error) {
	t.Time, err = time.Parse(time.RFC3339Nano, s)
	return
}

var _ = Describe("Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("server", func() {
		It("should Auth", func() {
			cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Auth(ctx, "password")
				pipe.Auth(ctx, "")
				return nil
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR AUTH"))
			Expect(cmds[0].Err().Error()).To(ContainSubstring("ERR AUTH"))
			Expect(cmds[1].Err().Error()).To(ContainSubstring("ERR AUTH"))

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(1)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
			Expect(stats.TotalConns).To(Equal(uint32(1)))
			Expect(stats.IdleConns).To(Equal(uint32(1)))
		})

		It("should hello", func() {
			cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Hello(ctx, 3, "", "", "")
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			m, err := cmds[0].(*redis.MapStringInterfaceCmd).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m["proto"]).To(Equal(int64(3)))
		})

		It("should Echo", func() {
			pipe := client.Pipeline()
			echo := pipe.Echo(ctx, "hello")
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(echo.Err()).NotTo(HaveOccurred())
			Expect(echo.Val()).To(Equal("hello"))
		})

		It("should Ping", func() {
			ping := client.Ping(ctx)
			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
		})

		It("should Wait", func() {
			const wait = 3 * time.Second

			// assume testing on single redis instance
			start := time.Now()
			val, err := client.Wait(ctx, 1, wait).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(0)))
			Expect(time.Now()).To(BeTemporally("~", start.Add(wait), 3*time.Second))
		})

		It("should WaitAOF", func() {
			const waitAOF = 3 * time.Second
			Skip("flaky test")

			// assuming that the redis instance doesn't have AOF enabled
			start := time.Now()
			val, err := client.WaitAOF(ctx, 1, 1, waitAOF).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).NotTo(ContainSubstring("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled"))
			Expect(time.Now()).To(BeTemporally("~", start.Add(waitAOF), 3*time.Second))
		})

		It("should Select", Label("NonRedisEnterprise"), func() {
			pipe := client.Pipeline()
			sel := pipe.Select(ctx, 1)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(sel.Err()).NotTo(HaveOccurred())
			Expect(sel.Val()).To(Equal("OK"))
		})

		It("should SwapDB", Label("NonRedisEnterprise"), func() {
			pipe := client.Pipeline()
			sel := pipe.SwapDB(ctx, 1, 2)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(sel.Err()).NotTo(HaveOccurred())
			Expect(sel.Val()).To(Equal("OK"))
		})

		It("should BgRewriteAOF", func() {
			Skip("flaky test")

			val, err := client.BgRewriteAOF(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(ContainSubstring("Background append only file rewriting"))
		})

		It("should BgSave", func() {
			Skip("flaky test")

			// workaround for "ERR Can't BGSAVE while AOF log rewriting is in progress"
			Eventually(func() string {
				return client.BgSave(ctx).Val()
			}, "30s").Should(Equal("Background saving started"))
		})

		It("Should CommandGetKeys", func() {
			keys, err := client.CommandGetKeys(ctx, "MSET", "a", "b", "c", "d", "e", "f").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"a", "c", "e"}))

			keys, err = client.CommandGetKeys(ctx, "EVAL", "not consulted", "3", "key1", "key2", "key3", "arg1", "arg2", "arg3", "argN").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"key1", "key2", "key3"}))

			keys, err = client.CommandGetKeys(ctx, "SORT", "mylist", "ALPHA", "STORE", "outlist").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"mylist", "outlist"}))

			_, err = client.CommandGetKeys(ctx, "FAKECOMMAND", "arg1", "arg2").Result()
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("ERR Invalid command specified"))
		})

		It("should CommandGetKeysAndFlags", func() {
			keysAndFlags, err := client.CommandGetKeysAndFlags(ctx, "LMOVE", "mylist1", "mylist2", "left", "left").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keysAndFlags).To(Equal([]redis.KeyFlags{
				{
					Key:   "mylist1",
					Flags: []string{"RW", "access", "delete"},
				},
				{
					Key:   "mylist2",
					Flags: []string{"RW", "insert"},
				},
			}))

			_, err = client.CommandGetKeysAndFlags(ctx, "FAKECOMMAND", "arg1", "arg2").Result()
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("ERR Invalid command specified"))
		})

		It("should ClientKill", func() {
			r := client.ClientKill(ctx, "1.1.1.1:1111")
			Expect(r.Err()).To(MatchError("ERR No such client"))
			Expect(r.Val()).To(Equal(""))
		})

		It("should ClientKillByFilter", func() {
			r := client.ClientKillByFilter(ctx, "TYPE", "test")
			Expect(r.Err()).To(MatchError("ERR Unknown client type 'test'"))
			Expect(r.Val()).To(Equal(int64(0)))
		})

		It("should ClientKillByFilter with MAXAGE", func() {
			var s []string
			started := make(chan bool)
			done := make(chan bool)

			go func() {
				defer GinkgoRecover()

				started <- true
				blpop := client.BLPop(ctx, 0, "list")
				Expect(blpop.Val()).To(Equal(s))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BLPOP is not blocked.")
			case <-time.After(2 * time.Second):
				// ok
			}

			killed := client.ClientKillByFilter(ctx, "MAXAGE", "1")
			Expect(killed.Err()).NotTo(HaveOccurred())
			Expect(killed.Val()).To(Equal(int64(2)))

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BLPOP is still blocked.")
			}
		})

		It("should ClientID", func() {
			err := client.ClientID(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(client.ClientID(ctx).Val()).To(BeNumerically(">=", 0))
		})

		It("should ClientUnblock", func() {
			id := client.ClientID(ctx).Val()
			r, err := client.ClientUnblock(ctx, id).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(r).To(Equal(int64(0)))
		})

		It("should ClientUnblockWithError", func() {
			id := client.ClientID(ctx).Val()
			r, err := client.ClientUnblockWithError(ctx, id).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(r).To(Equal(int64(0)))
		})

		It("should ClientInfo", func() {
			info, err := client.ClientInfo(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).NotTo(BeNil())
		})

		It("should ClientPause", Label("NonRedisEnterprise"), func() {
			err := client.ClientPause(ctx, time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			start := time.Now()
			err = client.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(time.Now()).To(BeTemporally("~", start.Add(time.Second), 800*time.Millisecond))
		})

		It("should ClientSetName and ClientGetName", func() {
			pipe := client.Pipeline()
			set := pipe.ClientSetName(ctx, "theclientname")
			get := pipe.ClientGetName(ctx)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(BeTrue())

			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("theclientname"))
		})

		It("should ClientSetInfo", func() {
			pipe := client.Pipeline()

			// Test setting the libName
			libName := "go-redis"
			libInfo := redis.WithLibraryName(libName)
			setInfo := pipe.ClientSetInfo(ctx, libInfo)
			_, err := pipe.Exec(ctx)

			Expect(err).NotTo(HaveOccurred())
			Expect(setInfo.Err()).NotTo(HaveOccurred())
			Expect(setInfo.Val()).To(Equal("OK"))

			// Test setting the libVer
			libVer := "vX.x"
			libInfo = redis.WithLibraryVersion(libVer)
			setInfo = pipe.ClientSetInfo(ctx, libInfo)
			_, err = pipe.Exec(ctx)

			Expect(err).NotTo(HaveOccurred())
			Expect(setInfo.Err()).NotTo(HaveOccurred())
			Expect(setInfo.Val()).To(Equal("OK"))

			// Test setting both fields, expect a panic
			libInfo = redis.LibraryInfo{LibName: &libName, LibVer: &libVer}

			Expect(func() {
				defer func() {
					if r := recover(); r != nil {
						err := r.(error)
						Expect(err).To(MatchError("both LibName and LibVer cannot be set at the same time"))
					}
				}()
				pipe.ClientSetInfo(ctx, libInfo)
			}).To(Panic())

			// Test setting neither field, expect a panic
			libInfo = redis.LibraryInfo{}

			Expect(func() {
				defer func() {
					if r := recover(); r != nil {
						err := r.(error)
						Expect(err).To(MatchError("at least one of LibName and LibVer should be set"))
					}
				}()
				pipe.ClientSetInfo(ctx, libInfo)
			}).To(Panic())
			// Test setting the default options for libName, libName suffix and libVer
			clientInfo := client.ClientInfo(ctx).Val()
			Expect(clientInfo.LibName).To(ContainSubstring("go-redis(go-redis,"))
			// Test setting the libName suffix in options
			opt := redisOptions()
			opt.IdentitySuffix = "suffix"
			client2 := redis.NewClient(opt)
			defer client2.Close()
			clientInfo = client2.ClientInfo(ctx).Val()
			Expect(clientInfo.LibName).To(ContainSubstring("go-redis(suffix,"))

		})

		It("should ConfigGet", func() {
			val, err := client.ConfigGet(ctx, "*").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).NotTo(BeEmpty())
		})

		It("should ConfigResetStat", Label("NonRedisEnterprise"), func() {
			r := client.ConfigResetStat(ctx)
			Expect(r.Err()).NotTo(HaveOccurred())
			Expect(r.Val()).To(Equal("OK"))
		})

		It("should ConfigSet", Label("NonRedisEnterprise"), func() {
			configGet := client.ConfigGet(ctx, "maxmemory")
			Expect(configGet.Err()).NotTo(HaveOccurred())
			Expect(configGet.Val()).To(HaveLen(1))
			_, ok := configGet.Val()["maxmemory"]
			Expect(ok).To(BeTrue())

			configSet := client.ConfigSet(ctx, "maxmemory", configGet.Val()["maxmemory"])
			Expect(configSet.Err()).NotTo(HaveOccurred())
			Expect(configSet.Val()).To(Equal("OK"))
		})

		It("should ConfigRewrite", Label("NonRedisEnterprise"), func() {
			configRewrite := client.ConfigRewrite(ctx)
			Expect(configRewrite.Err()).NotTo(HaveOccurred())
			Expect(configRewrite.Val()).To(Equal("OK"))
		})

		It("should DBSize", func() {
			size, err := client.DBSize(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(0)))
		})

		It("should Info", func() {
			info := client.Info(ctx)
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
		})

		It("should InfoMap", Label("redis.info"), func() {
			info := client.InfoMap(ctx)
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(BeNil())

			info = client.InfoMap(ctx, "dummy")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).To(BeNil())

			info = client.InfoMap(ctx, "server")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).To(HaveLen(1))
		})

		It("should Info cpu", func() {
			info := client.Info(ctx, "cpu")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
		})

		It("should Info cpu and memory", func() {
			info := client.Info(ctx, "cpu", "memory")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
			Expect(info.Val()).To(ContainSubstring(`memory`))
		})

		It("should LastSave", Label("NonRedisEnterprise"), func() {
			lastSave := client.LastSave(ctx)
			Expect(lastSave.Err()).NotTo(HaveOccurred())
			Expect(lastSave.Val()).NotTo(Equal(0))
		})

		It("should Save", Label("NonRedisEnterprise"), func() {
			// workaround for "ERR Background save already in progress"
			Eventually(func() string {
				return client.Save(ctx).Val()
			}, "10s").Should(Equal("OK"))
		})

		It("should SlaveOf", Label("NonRedisEnterprise"), func() {
			slaveOf := client.SlaveOf(ctx, "localhost", "8888")
			Expect(slaveOf.Err()).NotTo(HaveOccurred())
			Expect(slaveOf.Val()).To(Equal("OK"))

			slaveOf = client.SlaveOf(ctx, "NO", "ONE")
			Expect(slaveOf.Err()).NotTo(HaveOccurred())
			Expect(slaveOf.Val()).To(Equal("OK"))
		})

		It("should Time", func() {
			tm, err := client.Time(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(tm).To(BeTemporally("~", time.Now(), 3*time.Second))
		})

		It("should Command", Label("NonRedisEnterprise"), func() {
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cmds)).To(BeNumerically("~", 240, 25))

			cmd := cmds["mget"]
			Expect(cmd.Name).To(Equal("mget"))
			Expect(cmd.Arity).To(Equal(int8(-2)))
			Expect(cmd.Flags).To(ContainElement("readonly"))
			Expect(cmd.FirstKeyPos).To(Equal(int8(1)))
			Expect(cmd.LastKeyPos).To(Equal(int8(-1)))
			Expect(cmd.StepCount).To(Equal(int8(1)))

			cmd = cmds["ping"]
			Expect(cmd.Name).To(Equal("ping"))
			Expect(cmd.Arity).To(Equal(int8(-1)))
			Expect(cmd.Flags).To(ContainElement("fast"))
			Expect(cmd.FirstKeyPos).To(Equal(int8(0)))
			Expect(cmd.LastKeyPos).To(Equal(int8(0)))
			Expect(cmd.StepCount).To(Equal(int8(0)))
		})

		It("should return all command names", func() {
			cmdList := client.CommandList(ctx, nil)
			Expect(cmdList.Err()).NotTo(HaveOccurred())
			cmdNames := cmdList.Val()

			Expect(cmdNames).NotTo(BeEmpty())

			// Assert that some expected commands are present in the list
			Expect(cmdNames).To(ContainElement("get"))
			Expect(cmdNames).To(ContainElement("set"))
			Expect(cmdNames).To(ContainElement("hset"))
		})

		It("should filter commands by module", func() {
			filter := &redis.FilterBy{
				Module: "JSON",
			}
			cmdList := client.CommandList(ctx, filter)
			Expect(cmdList.Err()).NotTo(HaveOccurred())
			Expect(cmdList.Val()).To(HaveLen(0))
		})

		It("should filter commands by ACL category", func() {
			filter := &redis.FilterBy{
				ACLCat: "admin",
			}

			cmdList := client.CommandList(ctx, filter)
			Expect(cmdList.Err()).NotTo(HaveOccurred())
			cmdNames := cmdList.Val()

			// Assert that the returned list only contains commands from the admin ACL category
			Expect(len(cmdNames)).To(BeNumerically(">", 10))
		})

		It("should filter commands by pattern", func() {
			filter := &redis.FilterBy{
				Pattern: "*GET*",
			}
			cmdList := client.CommandList(ctx, filter)
			Expect(cmdList.Err()).NotTo(HaveOccurred())
			cmdNames := cmdList.Val()

			// Assert that the returned list only contains commands that match the given pattern
			Expect(cmdNames).To(ContainElement("get"))
			Expect(cmdNames).To(ContainElement("getbit"))
			Expect(cmdNames).To(ContainElement("getrange"))
			Expect(cmdNames).NotTo(ContainElement("set"))
		})
	})

	Describe("debugging", func() {
		PIt("should DebugObject", func() {
			err := client.DebugObject(ctx, "foo").Err()
			Expect(err).To(MatchError("ERR no such key"))

			err = client.Set(ctx, "foo", "bar", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			s, err := client.DebugObject(ctx, "foo").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(ContainSubstring("serializedlength:4"))
		})

		It("should MemoryUsage", func() {
			err := client.MemoryUsage(ctx, "foo").Err()
			Expect(err).To(Equal(redis.Nil))

			err = client.Set(ctx, "foo", "bar", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.MemoryUsage(ctx, "foo").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).NotTo(BeZero())

			n, err = client.MemoryUsage(ctx, "foo", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).NotTo(BeZero())
		})
	})

	Describe("keys", func() {
		It("should Del", func() {
			err := client.Set(ctx, "key1", "Hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.Set(ctx, "key2", "World", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.Del(ctx, "key1", "key2", "key3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Unlink", func() {
			err := client.Set(ctx, "key1", "Hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.Set(ctx, "key2", "World", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.Unlink(ctx, "key1", "key2", "key3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Dump", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			dump := client.Dump(ctx, "key")
			Expect(dump.Err()).NotTo(HaveOccurred())
			Expect(dump.Val()).NotTo(BeEmpty())
		})

		It("should Exists", func() {
			set := client.Set(ctx, "key1", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			n, err := client.Exists(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.Exists(ctx, "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			n, err = client.Exists(ctx, "key1", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.Exists(ctx, "key1", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Expire", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire(ctx, "key", 10*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(10 * time.Second))

			set = client.Set(ctx, "key", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Duration(-1)))

			ttl = client.TTL(ctx, "nonexistent_key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Duration(-2)))
		})

		It("should ExpireAt", func() {
			setCmd := client.Set(ctx, "key", "Hello", 0)
			Expect(setCmd.Err()).NotTo(HaveOccurred())
			Expect(setCmd.Val()).To(Equal("OK"))

			n, err := client.Exists(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			// Check correct expiration time is set in the future
			expireAt := time.Now().Add(time.Minute)
			expireAtCmd := client.ExpireAt(ctx, "key", expireAt)
			Expect(expireAtCmd.Err()).NotTo(HaveOccurred())
			Expect(expireAtCmd.Val()).To(Equal(true))

			timeCmd := client.ExpireTime(ctx, "key")
			Expect(timeCmd.Err()).NotTo(HaveOccurred())
			Expect(timeCmd.Val().Seconds()).To(BeNumerically("==", expireAt.Unix()))

			// Check correct expiration in the past
			expireAtCmd = client.ExpireAt(ctx, "key", time.Now().Add(-time.Hour))
			Expect(expireAtCmd.Err()).NotTo(HaveOccurred())
			Expect(expireAtCmd.Val()).To(Equal(true))

			n, err = client.Exists(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should Keys", func() {
			mset := client.MSet(ctx, "one", "1", "two", "2", "three", "3", "four", "4")
			Expect(mset.Err()).NotTo(HaveOccurred())
			Expect(mset.Val()).To(Equal("OK"))

			keys := client.Keys(ctx, "*o*")
			Expect(keys.Err()).NotTo(HaveOccurred())
			Expect(keys.Val()).To(ConsistOf([]string{"four", "one", "two"}))

			keys = client.Keys(ctx, "t??")
			Expect(keys.Err()).NotTo(HaveOccurred())
			Expect(keys.Val()).To(Equal([]string{"two"}))

			keys = client.Keys(ctx, "*")
			Expect(keys.Err()).NotTo(HaveOccurred())
			Expect(keys.Val()).To(ConsistOf([]string{"four", "one", "three", "two"}))
		})

		It("should Migrate", Label("NonRedisEnterprise"), func() {
			migrate := client.Migrate(ctx, "localhost", redisSecondaryPort, "key", 0, 0)
			Expect(migrate.Err()).NotTo(HaveOccurred())
			Expect(migrate.Val()).To(Equal("NOKEY"))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			migrate = client.Migrate(ctx, "localhost", redisSecondaryPort, "key", 0, 0)
			Expect(migrate.Err()).To(MatchError("IOERR error or timeout writing to target instance"))
			Expect(migrate.Val()).To(Equal(""))
		})

		It("should Move", Label("NonRedisEnterprise"), func() {
			move := client.Move(ctx, "key", 2)
			Expect(move.Err()).NotTo(HaveOccurred())
			Expect(move.Val()).To(Equal(false))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			move = client.Move(ctx, "key", 2)
			Expect(move.Err()).NotTo(HaveOccurred())
			Expect(move.Val()).To(Equal(true))

			get := client.Get(ctx, "key")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			pipe := client.Pipeline()
			pipe.Select(ctx, 2)
			get = pipe.Get(ctx, "key")
			pipe.FlushDB(ctx)

			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should Object", Label("NonRedisEnterprise"), func() {
			start := time.Now()
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			refCount := client.ObjectRefCount(ctx, "key")
			Expect(refCount.Err()).NotTo(HaveOccurred())
			Expect(refCount.Val()).To(Equal(int64(1)))

			client.ConfigSet(ctx, "maxmemory-policy", "volatile-lfu")
			freq := client.ObjectFreq(ctx, "key")
			Expect(freq.Err()).NotTo(HaveOccurred())
			client.ConfigSet(ctx, "maxmemory-policy", "noeviction") // default

			err := client.ObjectEncoding(ctx, "key").Err()
			Expect(err).NotTo(HaveOccurred())

			idleTime := client.ObjectIdleTime(ctx, "key")
			Expect(idleTime.Err()).NotTo(HaveOccurred())

			// Redis returned milliseconds/1000, which may cause ObjectIdleTime to be at a critical value,
			// should be +1s to deal with the critical value problem.
			// if too much time (>1s) is used during command execution, it may also cause the test to fail.
			// so the ObjectIdleTime result should be <=now-start+1s
			// link: https://github.com/redis/redis/blob/5b48d900498c85bbf4772c1d466c214439888115/src/object.c#L1265-L1272
			Expect(idleTime.Val()).To(BeNumerically("<=", time.Since(start)+time.Second))
		})

		It("should Persist", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire(ctx, "key", 10*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(10 * time.Second))

			persist := client.Persist(ctx, "key")
			Expect(persist.Err()).NotTo(HaveOccurred())
			Expect(persist.Val()).To(Equal(true))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val() < 0).To(Equal(true))
		})

		It("should PExpire", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := 900 * time.Millisecond
			pexpire := client.PExpire(ctx, "key", expiration)
			Expect(pexpire.Err()).NotTo(HaveOccurred())
			Expect(pexpire.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Second))

			pttl := client.PTTL(ctx, "key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 100*time.Millisecond))
		})

		It("should PExpireAt", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := 900 * time.Millisecond
			pexpireat := client.PExpireAt(ctx, "key", time.Now().Add(expiration))
			Expect(pexpireat.Err()).NotTo(HaveOccurred())
			Expect(pexpireat.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Second))

			pttl := client.PTTL(ctx, "key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 100*time.Millisecond))
		})

		It("should PExpireTime", func() {
			// The command returns -1 if the key exists but has no associated expiration time.
			// The command returns -2 if the key does not exist.
			pExpireTime := client.PExpireTime(ctx, "key")
			Expect(pExpireTime.Err()).NotTo(HaveOccurred())
			Expect(pExpireTime.Val() < 0).To(Equal(true))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			timestamp := time.Now().Add(time.Minute)
			expireAt := client.PExpireAt(ctx, "key", timestamp)
			Expect(expireAt.Err()).NotTo(HaveOccurred())
			Expect(expireAt.Val()).To(Equal(true))

			pExpireTime = client.PExpireTime(ctx, "key")
			Expect(pExpireTime.Err()).NotTo(HaveOccurred())
			Expect(pExpireTime.Val().Milliseconds()).To(BeNumerically("==", timestamp.UnixMilli()))
		})

		It("should PTTL", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := time.Second
			expire := client.Expire(ctx, "key", expiration)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			pttl := client.PTTL(ctx, "key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 100*time.Millisecond))
		})

		It("should RandomKey", func() {
			randomKey := client.RandomKey(ctx)
			Expect(randomKey.Err()).To(Equal(redis.Nil))
			Expect(randomKey.Val()).To(Equal(""))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			randomKey = client.RandomKey(ctx)
			Expect(randomKey.Err()).NotTo(HaveOccurred())
			Expect(randomKey.Val()).To(Equal("key"))
		})

		It("should Rename", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			status := client.Rename(ctx, "key", "key1")
			Expect(status.Err()).NotTo(HaveOccurred())
			Expect(status.Val()).To(Equal("OK"))

			get := client.Get(ctx, "key1")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should RenameNX", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			renameNX := client.RenameNX(ctx, "key", "key1")
			Expect(renameNX.Err()).NotTo(HaveOccurred())
			Expect(renameNX.Val()).To(Equal(true))

			get := client.Get(ctx, "key1")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should Restore", func() {
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			dump := client.Dump(ctx, "key")
			Expect(dump.Err()).NotTo(HaveOccurred())

			err = client.Del(ctx, "key").Err()
			Expect(err).NotTo(HaveOccurred())

			restore, err := client.Restore(ctx, "key", 0, dump.Val()).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(restore).To(Equal("OK"))

			type_, err := client.Type(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(type_).To(Equal("string"))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should RestoreReplace", func() {
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			dump := client.Dump(ctx, "key")
			Expect(dump.Err()).NotTo(HaveOccurred())

			restore, err := client.RestoreReplace(ctx, "key", 0, dump.Val()).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(restore).To(Equal("OK"))

			type_, err := client.Type(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(type_).To(Equal("string"))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should Sort RO", func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			els, err := client.SortRO(ctx, "list", &redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Sort", func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			els, err := client.Sort(ctx, "list", &redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Sort and Get", Label("NonRedisEnterprise"), func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			err = client.Set(ctx, "object_2", "value2", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			{
				els, err := client.Sort(ctx, "list", &redis.Sort{
					Get: []string{"object_*"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(els).To(Equal([]string{"", "value2", ""}))
			}

			{
				els, err := client.SortInterfaces(ctx, "list", &redis.Sort{
					Get: []string{"object_*"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(els).To(Equal([]interface{}{nil, "value2", nil}))
			}
		})

		It("should Sort and Store", Label("NonRedisEnterprise"), func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			n, err := client.SortStore(ctx, "list", "list2", &redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			els, err := client.LRange(ctx, "list2", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Touch", func() {
			set1 := client.Set(ctx, "touch1", "hello", 0)
			Expect(set1.Err()).NotTo(HaveOccurred())
			Expect(set1.Val()).To(Equal("OK"))

			set2 := client.Set(ctx, "touch2", "hello", 0)
			Expect(set2.Err()).NotTo(HaveOccurred())
			Expect(set2.Val()).To(Equal("OK"))

			touch := client.Touch(ctx, "touch1", "touch2", "touch3")
			Expect(touch.Err()).NotTo(HaveOccurred())
			Expect(touch.Val()).To(Equal(int64(2)))
		})

		It("should ExpireTime", func() {
			// The command returns -1 if the key exists but has no associated expiration time.
			// The command returns -2 if the key does not exist.
			expireTimeCmd := client.ExpireTime(ctx, "key")
			Expect(expireTimeCmd.Err()).NotTo(HaveOccurred())
			Expect(expireTimeCmd.Val() < 0).To(Equal(true))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expireAt := time.Now().Add(time.Minute)
			expireAtCmd := client.ExpireAt(ctx, "key", expireAt)
			Expect(expireAtCmd.Err()).NotTo(HaveOccurred())
			Expect(expireAtCmd.Val()).To(Equal(true))

			expireTimeCmd = client.ExpireTime(ctx, "key")
			Expect(expireTimeCmd.Err()).NotTo(HaveOccurred())
			Expect(expireTimeCmd.Val().Seconds()).To(BeNumerically("==", expireAt.Unix()))
		})

		It("should TTL", func() {
			// The command returns -1 if the key exists but has no associated expire
			// The command returns -2 if the key does not exist.
			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val() < 0).To(Equal(true))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire(ctx, "key", 60*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(60 * time.Second))
		})

		It("should Type", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			type_ := client.Type(ctx, "key")
			Expect(type_.Err()).NotTo(HaveOccurred())
			Expect(type_.Val()).To(Equal("string"))
		})
	})

	Describe("scanning", func() {
		It("should Scan", func() {
			for i := 0; i < 1000; i++ {
				set := client.Set(ctx, fmt.Sprintf("key%d", i), "hello", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.Scan(ctx, 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should ScanType", func() {
			for i := 0; i < 1000; i++ {
				set := client.Set(ctx, fmt.Sprintf("key%d", i), "hello", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.ScanType(ctx, 0, "", 0, "string").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should SScan", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.SAdd(ctx, "myset", fmt.Sprintf("member%d", i))
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.SScan(ctx, "myset", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should HScan", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.HScan(ctx, "myhash", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			// If we don't get at least two items back, it's really strange.
			Expect(cursor).To(BeNumerically(">=", 2))
			Expect(len(keys)).To(BeNumerically(">=", 2))
			Expect(keys[0]).To(HavePrefix("key"))
			Expect(keys[1]).To(Equal("hello"))
		})

		It("should HScan without values", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.HScanNoValues(ctx, "myhash", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			// If we don't get at least two items back, it's really strange.
			Expect(cursor).To(BeNumerically(">=", 2))
			Expect(len(keys)).To(BeNumerically(">=", 2))
			Expect(keys[0]).To(HavePrefix("key"))
			Expect(keys[1]).To(HavePrefix("key"))
		})

		It("should ZScan", func() {
			for i := 0; i < 1000; i++ {
				err := client.ZAdd(ctx, "myset", redis.Z{
					Score:  float64(i),
					Member: fmt.Sprintf("member%d", i),
				}).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.ZScan(ctx, "myset", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})
	})

	Describe("strings", func() {
		It("should Append", func() {
			n, err := client.Exists(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			appendRes := client.Append(ctx, "key", "Hello")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(5)))

			appendRes = client.Append(ctx, "key", " World")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello World"))
		})

		It("should BitCount", func() {
			set := client.Set(ctx, "key", "foobar", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitCount := client.BitCount(ctx, "key", nil)
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(26)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 0,
				End:   0,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(4)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 1,
				End:   1,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(6)))
		})

		It("should BitOpAnd", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpAnd := client.BitOpAnd(ctx, "dest", "key1", "key2")
			Expect(bitOpAnd.Err()).NotTo(HaveOccurred())
			Expect(bitOpAnd.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should BitOpOr", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpOr := client.BitOpOr(ctx, "dest", "key1", "key2")
			Expect(bitOpOr.Err()).NotTo(HaveOccurred())
			Expect(bitOpOr.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("1"))
		})

		It("should BitOpXor", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key1", "\xff", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "\x0f", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpXor := client.BitOpXor(ctx, "dest", "key1", "key2")
			Expect(bitOpXor.Err()).NotTo(HaveOccurred())
			Expect(bitOpXor.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xf0"))
		})

		It("should BitOpNot", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key1", "\x00", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpNot := client.BitOpNot(ctx, "dest", "key1")
			Expect(bitOpNot.Err()).NotTo(HaveOccurred())
			Expect(bitOpNot.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xff"))
		})

		It("should BitPos", func() {
			err := client.Set(ctx, "mykey", "\xff\xf0\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			pos, err := client.BitPos(ctx, "mykey", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(12)))

			pos, err = client.BitPos(ctx, "mykey", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(0)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 0, -3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 0, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))
		})

		It("should BitPosSpan", func() {
			err := client.Set(ctx, "mykey", "\x00\xff\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			pos, err := client.BitPosSpan(ctx, "mykey", 0, 1, 3, "byte").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPosSpan(ctx, "mykey", 0, 1, 3, "bit").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(1)))
		})

		It("should BitField", func() {
			nn, err := client.BitField(ctx, "mykey", "INCRBY", "i5", 100, 1, "GET", "u4", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nn).To(Equal([]int64{1, 0}))

			nn, err = client.BitField(ctx, "mykey", "set", "i1", 1, 1, "GET", "u4", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nn).To(Equal([]int64{0, 4}))
		})

		It("should BitFieldRO", func() {
			nn, err := client.BitField(ctx, "mykey", "SET", "u8", 8, 255).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nn).To(Equal([]int64{0}))

			nn, err = client.BitFieldRO(ctx, "mykey", "u8", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nn).To(Equal([]int64{0}))

			nn, err = client.BitFieldRO(ctx, "mykey", "u8", 0, "u4", 8, "u4", 12, "u4", 13).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nn).To(Equal([]int64{0, 15, 15, 14}))
		})

		It("should Decr", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decr := client.Decr(ctx, "key")
			Expect(decr.Err()).NotTo(HaveOccurred())
			Expect(decr.Val()).To(Equal(int64(9)))

			set = client.Set(ctx, "key", "234293482390480948029348230948", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decr = client.Decr(ctx, "key")
			Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))
			Expect(decr.Val()).To(Equal(int64(0)))
		})

		It("should DecrBy", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decrBy := client.DecrBy(ctx, "key", 5)
			Expect(decrBy.Err()).NotTo(HaveOccurred())
			Expect(decrBy.Val()).To(Equal(int64(5)))
		})

		It("should Get", func() {
			get := client.Get(ctx, "_")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get = client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should GetBit", func() {
			setBit := client.SetBit(ctx, "key", 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			getBit := client.GetBit(ctx, "key", 0)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))

			getBit = client.GetBit(ctx, "key", 7)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(1)))

			getBit = client.GetBit(ctx, "key", 100)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))
		})

		It("should GetRange", func() {
			set := client.Set(ctx, "key", "This is a string", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getRange := client.GetRange(ctx, "key", 0, 3)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This"))

			getRange = client.GetRange(ctx, "key", -3, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("ing"))

			getRange = client.GetRange(ctx, "key", 0, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This is a string"))

			getRange = client.GetRange(ctx, "key", 10, 100)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("string"))
		})

		It("should GetSet", func() {
			incr := client.Incr(ctx, "key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(1)))

			getSet := client.GetSet(ctx, "key", "0")
			Expect(getSet.Err()).NotTo(HaveOccurred())
			Expect(getSet.Val()).To(Equal("1"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should GetEX", func() {
			set := client.Set(ctx, "key", "value", 100*time.Second)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(BeNumerically("~", 100*time.Second, 3*time.Second))

			getEX := client.GetEx(ctx, "key", 200*time.Second)
			Expect(getEX.Err()).NotTo(HaveOccurred())
			Expect(getEX.Val()).To(Equal("value"))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(BeNumerically("~", 200*time.Second, 3*time.Second))
		})

		It("should GetDel", func() {
			set := client.Set(ctx, "key", "value", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getDel := client.GetDel(ctx, "key")
			Expect(getDel.Err()).NotTo(HaveOccurred())
			Expect(getDel.Val()).To(Equal("value"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).To(Equal(redis.Nil))
		})

		It("should Incr", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incr := client.Incr(ctx, "key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("11"))
		})

		It("should IncrBy", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrBy := client.IncrBy(ctx, "key", 5)
			Expect(incrBy.Err()).NotTo(HaveOccurred())
			Expect(incrBy.Val()).To(Equal(int64(15)))
		})

		It("should IncrByFloat", func() {
			set := client.Set(ctx, "key", "10.50", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat := client.IncrByFloat(ctx, "key", 0.1)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(10.6))

			set = client.Set(ctx, "key", "5.0e3", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat = client.IncrByFloat(ctx, "key", 2.0e2)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should IncrByFloatOverflow", func() {
			incrByFloat := client.IncrByFloat(ctx, "key", 996945661)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(996945661)))
		})

		It("should MSetMGet", func() {
			mSet := client.MSet(ctx, "key1", "hello1", "key2", "hello2")
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet := client.MGet(ctx, "key1", "key2", "_")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{"hello1", "hello2", nil}))

			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
			}
			mSet = client.MSet(ctx, &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet = client.MGet(ctx, "set1", "set2", "set3", "set4")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{
				"val1",
				"1024",
				strconv.Itoa(int(2 * time.Millisecond.Nanoseconds())),
				"",
			}))
		})

		It("should scan Mget", func() {
			now := time.Now()

			err := client.MSet(ctx, "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.MGet(ctx, "key1", "key2", "_", "time")
			Expect(res.Err()).NotTo(HaveOccurred())

			type data struct {
				Key1 string    `redis:"key1"`
				Key2 int       `redis:"key2"`
				Time TimeValue `redis:"time"`
			}
			var d data
			Expect(res.Scan(&d)).NotTo(HaveOccurred())
			Expect(d.Time.UnixNano()).To(Equal(now.UnixNano()))
			d.Time.Time = time.Time{}
			Expect(d).To(Equal(data{
				Key1: "hello1",
				Key2: 123,
				Time: TimeValue{Time: time.Time{}},
			}))
		})

		It("should MSetNX", Label("NonRedisEnterprise"), func() {
			mSetNX := client.MSetNX(ctx, "key1", "hello1", "key2", "hello2")
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(true))

			mSetNX = client.MSetNX(ctx, "key2", "hello1", "key3", "hello2")
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(false))

			// set struct
			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
			}
			mSetNX = client.MSetNX(ctx, &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(true))
		})

		It("should SetWithArgs with TTL", func() {
			args := redis.SetArgs{
				TTL: 500 * time.Millisecond,
			}
			err := client.SetArgs(ctx, "key", "hello", args).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "2s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetWithArgs with expiration date", func() {
			expireAt := time.Now().AddDate(1, 1, 1)
			args := redis.SetArgs{
				ExpireAt: expireAt,
			}
			err := client.SetArgs(ctx, "key", "hello", args).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			// check the key has an expiration date
			// (so a TTL value different of -1)
			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).ToNot(Equal(-1))
		})

		It("should SetWithArgs with negative expiration date", func() {
			args := redis.SetArgs{
				ExpireAt: time.Now().AddDate(-3, 1, 1),
			}
			// redis accepts a timestamp less than the current date
			// but returns nil when trying to get the key
			err := client.SetArgs(ctx, "key", "hello", args).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with keepttl", func() {
			// Set with ttl
			argsWithTTL := redis.SetArgs{
				TTL: 5 * time.Second,
			}
			set := client.SetArgs(ctx, "key", "hello", argsWithTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Result()).To(Equal("OK"))

			// Set with keepttl
			argsWithKeepTTL := redis.SetArgs{
				KeepTTL: true,
			}
			set = client.SetArgs(ctx, "key", "hello", argsWithKeepTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Result()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			// set keepttl will Retain the ttl associated with the key
			Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetWithArgs with NX mode and key exists", func() {
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with NX mode and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		It("should SetWithArgs with NX mode and GET option", func() {
			args := redis.SetArgs{
				Mode: "nx",
				Get:  true,
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, NX mode, and key does not exist", func() {
			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetWithArgs with expiration, NX mode, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, NX mode, and GET option", func() {
			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
				Get:  true,
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0).Err()
			Expect(e).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		It("should SetWithArgs with XX mode and GET option, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0).Err()
			Expect(e).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "xx",
				Get:  true,
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should SetWithArgs with XX mode and GET option, and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "xx",
				Get:  true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, XX mode, GET option, and key does not exist", func() {
			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "xx",
				Get:  true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, XX mode, GET option, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "xx",
				Get:  true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetWithArgs with Get and key does not exist yet", func() {
			args := redis.SetArgs{
				Get: true,
			}

			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with Get and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Get: true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should Pipelined SetArgs with Get and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Get: true,
			}

			pipe := client.Pipeline()
			setArgs := pipe.SetArgs(ctx, "key", "world", args)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(setArgs.Err()).NotTo(HaveOccurred())
			Expect(setArgs.Val()).To(Equal("hello"))
		})

		It("should Set with expiration", func() {
			err := client.Set(ctx, "key", "hello", 100*time.Millisecond).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should Set with keepttl", func() {
			// set with ttl
			set := client.Set(ctx, "key", "hello", 5*time.Second)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			// set with keepttl
			set = client.Set(ctx, "key", "hello1", redis.KeepTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			// set keepttl will Retain the ttl associated with the key
			Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetGet", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetEX", func() {
			err := client.SetEx(ctx, "key", "hello", 1*time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "foo").Err()
			}, "2s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetNX", func() {
			setNX := client.SetNX(ctx, "key", "hello", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(true))

			setNX = client.SetNX(ctx, "key", "hello2", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(false))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetNX with expiration", func() {
			isSet, err := client.SetNX(ctx, "key", "hello", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetNX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should SetNX with keepttl", func() {
			isSet, err := client.SetNX(ctx, "key", "hello1", redis.KeepTTL).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val().Nanoseconds()).To(Equal(int64(-1)))
		})

		It("should SetXX", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with expiration", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with keepttl", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", 5*time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetXX(ctx, "key", "hello3", redis.KeepTTL).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello3"))

			// set keepttl will Retain the ttl associated with the key
			ttl, err := client.TTL(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ttl).NotTo(Equal(-1))
		})

		It("should SetRange", func() {
			set := client.Set(ctx, "key", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			range_ := client.SetRange(ctx, "key", 6, "Redis")
			Expect(range_.Err()).NotTo(HaveOccurred())
			Expect(range_.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello Redis"))
		})

		It("should StrLen", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			strLen := client.StrLen(ctx, "key")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(5)))

			strLen = client.StrLen(ctx, "_")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(0)))
		})

		It("should Copy", Label("NonRedisEnterprise"), func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			copy := client.Copy(ctx, "key", "newKey", redisOptions().DB, false)
			Expect(copy.Err()).NotTo(HaveOccurred())
			Expect(copy.Val()).To(Equal(int64(1)))

			// Value is available by both keys now
			getOld := client.Get(ctx, "key")
			Expect(getOld.Err()).NotTo(HaveOccurred())
			Expect(getOld.Val()).To(Equal("hello"))
			getNew := client.Get(ctx, "newKey")
			Expect(getNew.Err()).NotTo(HaveOccurred())
			Expect(getNew.Val()).To(Equal("hello"))

			// Overwriting an existing key should not succeed
			overwrite := client.Copy(ctx, "newKey", "key", redisOptions().DB, false)
			Expect(overwrite.Val()).To(Equal(int64(0)))

			// Overwrite is allowed when replace=rue
			replace := client.Copy(ctx, "newKey", "key", redisOptions().DB, true)
			Expect(replace.Val()).To(Equal(int64(1)))
		})

		It("should acl dryrun", func() {
			dryRun := client.ACLDryRun(ctx, "default", "get", "randomKey")
			Expect(dryRun.Err()).NotTo(HaveOccurred())
			Expect(dryRun.Val()).To(Equal("OK"))
		})

		It("should fail module loadex", Label("NonRedisEnterprise"), func() {
			dryRun := client.ModuleLoadex(ctx, &redis.ModuleLoadexConfig{
				Path: "/path/to/non-existent-library.so",
				Conf: map[string]interface{}{
					"param1": "value1",
				},
				Args: []interface{}{
					"arg1",
				},
			})
			Expect(dryRun.Err()).To(HaveOccurred())
			Expect(dryRun.Err().Error()).To(Equal("ERR Error loading the extension. Please check the server logs."))
		})

		It("converts the module loadex configuration to a slice of arguments correctly", func() {
			conf := &redis.ModuleLoadexConfig{
				Path: "/path/to/your/module.so",
				Conf: map[string]interface{}{
					"param1": "value1",
				},
				Args: []interface{}{
					"arg1",
					"arg2",
					3,
				},
			}

			args := conf.ToArgs()

			// Test if the arguments are in the correct order
			expectedArgs := []interface{}{
				"MODULE",
				"LOADEX",
				"/path/to/your/module.so",
				"CONFIG",
				"param1",
				"value1",
				"ARGS",
				"arg1",
				"ARGS",
				"arg2",
				"ARGS",
				3,
			}

			Expect(args).To(Equal(expectedArgs))
		})

		It("should ACL LOG", Label("NonRedisEnterprise"), func() {
			err := client.Do(ctx, "acl", "setuser", "test", ">test", "on", "allkeys", "+get").Err()
			Expect(err).NotTo(HaveOccurred())

			clientAcl := redis.NewClient(redisOptions())
			clientAcl.Options().Username = "test"
			clientAcl.Options().Password = "test"
			clientAcl.Options().DB = 0
			_ = clientAcl.Set(ctx, "mystring", "foo", 0).Err()
			_ = clientAcl.HSet(ctx, "myhash", "foo", "bar").Err()
			_ = clientAcl.SAdd(ctx, "myset", "foo", "bar").Err()

			logEntries, err := client.ACLLog(ctx, 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(logEntries)).To(Equal(4))

			for _, entry := range logEntries {
				Expect(entry.Reason).To(Equal("command"))
				Expect(entry.Context).To(Equal("toplevel"))
				Expect(entry.Object).NotTo(BeEmpty())
				Expect(entry.Username).To(Equal("test"))
				Expect(entry.AgeSeconds).To(BeNumerically(">=", 0))
				Expect(entry.ClientInfo).NotTo(BeNil())
				Expect(entry.EntryID).To(BeNumerically(">=", 0))
				Expect(entry.TimestampCreated).To(BeNumerically(">=", 0))
				Expect(entry.TimestampLastUpdated).To(BeNumerically(">=", 0))
			}

			limitedLogEntries, err := client.ACLLog(ctx, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(limitedLogEntries)).To(Equal(2))
		})

		It("should ACL LOG RESET", Label("NonRedisEnterprise"), func() {
			// Call ACL LOG RESET
			resetCmd := client.ACLLogReset(ctx)
			Expect(resetCmd.Err()).NotTo(HaveOccurred())
			Expect(resetCmd.Val()).To(Equal("OK"))

			// Verify that the log is empty after the reset
			logEntries, err := client.ACLLog(ctx, 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(logEntries)).To(Equal(0))
		})
	})

	Describe("hashes", func() {
		It("should HDel", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hDel := client.HDel(ctx, "hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(1)))

			hDel = client.HDel(ctx, "hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(0)))
		})

		It("should HExists", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hExists := client.HExists(ctx, "hash", "key")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(true))

			hExists = client.HExists(ctx, "hash", "key1")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(false))
		})

		It("should HGet", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			hGet = client.HGet(ctx, "hash", "key1")
			Expect(hGet.Err()).To(Equal(redis.Nil))
			Expect(hGet.Val()).To(Equal(""))
		})

		It("should HGetAll", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			m, err := client.HGetAll(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{"key1": "hello1", "key2": "hello2"}))
		})

		It("should scan", func() {
			now := time.Now()

			err := client.HMSet(ctx, "hash", "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.HGetAll(ctx, "hash")
			Expect(res.Err()).NotTo(HaveOccurred())

			type data struct {
				Key1 string    `redis:"key1"`
				Key2 int       `redis:"key2"`
				Time TimeValue `redis:"time"`
			}
			var d data
			Expect(res.Scan(&d)).NotTo(HaveOccurred())
			Expect(d.Time.UnixNano()).To(Equal(now.UnixNano()))
			d.Time.Time = time.Time{}
			Expect(d).To(Equal(data{
				Key1: "hello1",
				Key2: 123,
				Time: TimeValue{Time: time.Time{}},
			}))

			type data2 struct {
				Key1 string    `redis:"key1"`
				Key2 int       `redis:"key2"`
				Time time.Time `redis:"time"`
			}
			err = client.HSet(ctx, "hash", &data2{
				Key1: "hello2",
				Key2: 200,
				Time: now,
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			var d2 data2
			err = client.HMGet(ctx, "hash", "key1", "key2", "time").Scan(&d2)
			Expect(err).NotTo(HaveOccurred())
			Expect(d2.Key1).To(Equal("hello2"))
			Expect(d2.Key2).To(Equal(200))
			Expect(d2.Time.Unix()).To(Equal(now.Unix()))
		})

		It("should HIncrBy", func() {
			hSet := client.HSet(ctx, "hash", "key", "5")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hIncrBy := client.HIncrBy(ctx, "hash", "key", 1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(6)))

			hIncrBy = client.HIncrBy(ctx, "hash", "key", -1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(5)))

			hIncrBy = client.HIncrBy(ctx, "hash", "key", -10)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(-5)))
		})

		It("should HIncrByFloat", func() {
			hSet := client.HSet(ctx, "hash", "field", "10.50")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hIncrByFloat := client.HIncrByFloat(ctx, "hash", "field", 0.1)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(10.6))

			hSet = client.HSet(ctx, "hash", "field", "5.0e3")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(0)))

			hIncrByFloat = client.HIncrByFloat(ctx, "hash", "field", 2.0e2)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should HKeys", func() {
			hkeys := client.HKeys(ctx, "hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{}))

			hset := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hset.Err()).NotTo(HaveOccurred())
			hset = client.HSet(ctx, "hash", "key2", "hello2")
			Expect(hset.Err()).NotTo(HaveOccurred())

			hkeys = client.HKeys(ctx, "hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{"key1", "key2"}))
		})

		It("should HLen", func() {
			hSet := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			hSet = client.HSet(ctx, "hash", "key2", "hello2")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hLen := client.HLen(ctx, "hash")
			Expect(hLen.Err()).NotTo(HaveOccurred())
			Expect(hLen.Val()).To(Equal(int64(2)))
		})

		It("should HMGet", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.HMGet(ctx, "hash", "key1", "key2", "_").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"hello1", "hello2", nil}))
		})

		It("should HSet", func() {
			ok, err := client.HSet(ctx, "hash", map[string]interface{}{
				"key1": "hello1",
				"key2": "hello2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(2)))

			v, err := client.HGet(ctx, "hash", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello1"))

			v, err = client.HGet(ctx, "hash", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello2"))

			keys, err := client.HKeys(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf([]string{"key1", "key2"}))
		})

		It("should HSet", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			// set struct
			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
				Set6 string                 `redis:"set6,omitempty"`
			}

			hSet = client.HSet(ctx, "hash", &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(4)))

			hMGet := client.HMGet(ctx, "hash", "set1", "set2", "set3", "set4", "set5", "set6")
			Expect(hMGet.Err()).NotTo(HaveOccurred())
			Expect(hMGet.Val()).To(Equal([]interface{}{
				"val1",
				"1024",
				strconv.Itoa(int(2 * time.Millisecond.Nanoseconds())),
				"",
				nil,
				nil,
			}))

			hSet = client.HSet(ctx, "hash2", &set{
				Set1: "val2",
				Set6: "val",
			})
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(5)))

			hMGet = client.HMGet(ctx, "hash2", "set1", "set6")
			Expect(hMGet.Err()).NotTo(HaveOccurred())
			Expect(hMGet.Val()).To(Equal([]interface{}{
				"val2",
				"val",
			}))
		})

		It("should HSetNX", func() {
			hSetNX := client.HSetNX(ctx, "hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(true))

			hSetNX = client.HSetNX(ctx, "hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(false))

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))
		})

		It("should HVals", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.HVals(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]string{"hello1", "hello2"}))

			var slice []string
			err = client.HVals(ctx, "hash").ScanSlice(&slice)
			Expect(err).NotTo(HaveOccurred())
			Expect(slice).To(Equal([]string{"hello1", "hello2"}))
		})

		It("should HRandField", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			v := client.HRandField(ctx, "hash", 1)
			Expect(v.Err()).NotTo(HaveOccurred())
			Expect(v.Val()).To(Or(Equal([]string{"key1"}), Equal([]string{"key2"})))

			v = client.HRandField(ctx, "hash", 0)
			Expect(v.Err()).NotTo(HaveOccurred())
			Expect(v.Val()).To(HaveLen(0))

			kv, err := client.HRandFieldWithValues(ctx, "hash", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(kv).To(Or(
				Equal([]redis.KeyValue{{Key: "key1", Value: "hello1"}}),
				Equal([]redis.KeyValue{{Key: "key2", Value: "hello2"}}),
			))
		})

		It("should HExpire", Label("hash-expiration", "NonRedisEnterprise"), func() {
			res, err := client.HExpire(ctx, "no_such_key", 10, "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err = client.HExpire(ctx, "myhash", 10, "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, 1, -2}))
		})

		It("should HPExpire", Label("hash-expiration", "NonRedisEnterprise"), func() {
			_, err := client.HPExpire(ctx, "no_such_key", 10, "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HPExpire(ctx, "myhash", 10, "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, 1, -2}))
		})

		It("should HExpireAt", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HExpireAt(ctx, "no_such_key", time.Now().Add(10*time.Second), "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HExpireAt(ctx, "myhash", time.Now().Add(10*time.Second), "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, 1, -2}))
		})

		It("should HPExpireAt", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HPExpireAt(ctx, "no_such_key", time.Now().Add(10*time.Second), "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HPExpireAt(ctx, "myhash", time.Now().Add(10*time.Second), "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, 1, -2}))
		})

		It("should HPersist", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HPersist(ctx, "no_such_key", "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HPersist(ctx, "myhash", "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{-1, -1, -2}))

			res, err = client.HExpire(ctx, "myhash", 10, "key1", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, -2}))

			res, err = client.HPersist(ctx, "myhash", "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, -1, -2}))
		})

		It("should HExpireTime", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HExpireTime(ctx, "no_such_key", "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HExpire(ctx, "myhash", 10, "key1", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, -2}))

			res, err = client.HExpireTime(ctx, "myhash", "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(BeNumerically("~", time.Now().Add(10*time.Second).Unix(), 1))
		})

		It("should HPExpireTime", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HPExpireTime(ctx, "no_such_key", "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HExpire(ctx, "myhash", 10, "key1", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, -2}))

			res, err = client.HPExpireTime(ctx, "myhash", "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeEquivalentTo([]int64{time.Now().Add(10 * time.Second).UnixMilli(), -1, -2}))
		})

		It("should HTTL", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HTTL(ctx, "no_such_key", "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HExpire(ctx, "myhash", 10, "key1", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, -2}))

			res, err = client.HTTL(ctx, "myhash", "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{10, -1, -2}))
		})

		It("should HPTTL", Label("hash-expiration", "NonRedisEnterprise"), func() {

			_, err := client.HPTTL(ctx, "no_such_key", "field1", "field2", "field3").Result()
			Expect(err).To(BeNil())
			for i := 0; i < 100; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			res, err := client.HExpire(ctx, "myhash", 10, "key1", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]int64{1, -2}))

			res, err = client.HPTTL(ctx, "myhash", "key1", "key2", "key200").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(BeNumerically("~", 10*time.Second.Milliseconds(), 1))
		})
	})

	Describe("hyperloglog", func() {
		It("should PFMerge", Label("NonRedisEnterprise"), func() {
			pfAdd := client.PFAdd(ctx, "hll1", "1", "2", "3", "4", "5")
			Expect(pfAdd.Err()).NotTo(HaveOccurred())

			pfCount := client.PFCount(ctx, "hll1")
			Expect(pfCount.Err()).NotTo(HaveOccurred())
			Expect(pfCount.Val()).To(Equal(int64(5)))

			pfAdd = client.PFAdd(ctx, "hll2", "a", "b", "c", "d", "e")
			Expect(pfAdd.Err()).NotTo(HaveOccurred())

			pfMerge := client.PFMerge(ctx, "hllMerged", "hll1", "hll2")
			Expect(pfMerge.Err()).NotTo(HaveOccurred())

			pfCount = client.PFCount(ctx, "hllMerged")
			Expect(pfCount.Err()).NotTo(HaveOccurred())
			Expect(pfCount.Val()).To(Equal(int64(10)))

			pfCount = client.PFCount(ctx, "hll1", "hll2")
			Expect(pfCount.Err()).NotTo(HaveOccurred())
			Expect(pfCount.Val()).To(Equal(int64(10)))
		})
	})

	Describe("lists", func() {
		It("should BLPop", Label("NonRedisEnterprise"), func() {
			rPush := client.RPush(ctx, "list1", "a", "b", "c")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			bLPop := client.BLPop(ctx, 0, "list1", "list2")
			Expect(bLPop.Err()).NotTo(HaveOccurred())
			Expect(bLPop.Val()).To(Equal([]string{"list1", "a"}))
		})

		It("should BLPopBlocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				bLPop := client.BLPop(ctx, 0, "list")
				Expect(bLPop.Err()).NotTo(HaveOccurred())
				Expect(bLPop.Val()).To(Equal([]string{"list", "a"}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BLPop is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			rPush := client.RPush(ctx, "list", "a")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BLPop is still blocked")
			}
		})

		It("should BLPop timeout", func() {
			val, err := client.BLPop(ctx, time.Second, "list1").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(BeNil())

			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(2)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
		})

		It("should BRPop", Label("NonRedisEnterprise"), func() {
			rPush := client.RPush(ctx, "list1", "a", "b", "c")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			bRPop := client.BRPop(ctx, 0, "list1", "list2")
			Expect(bRPop.Err()).NotTo(HaveOccurred())
			Expect(bRPop.Val()).To(Equal([]string{"list1", "c"}))
		})

		It("should BRPop blocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				brpop := client.BRPop(ctx, 0, "list")
				Expect(brpop.Err()).NotTo(HaveOccurred())
				Expect(brpop.Val()).To(Equal([]string{"list", "a"}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BRPop is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			rPush := client.RPush(ctx, "list", "a")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BRPop is still blocked")
				// ok
			}
		})

		It("should BRPopLPush", Label("NonRedisEnterprise"), func() {
			_, err := client.BRPopLPush(ctx, "list1", "list2", time.Second).Result()
			Expect(err).To(Equal(redis.Nil))

			err = client.RPush(ctx, "list1", "a", "b", "c").Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.BRPopLPush(ctx, "list1", "list2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("c"))
		})

		It("should LCS", Label("NonRedisEnterprise"), func() {
			err := client.MSet(ctx, "key1", "ohmytext", "key2", "mynewtext").Err()
			Expect(err).NotTo(HaveOccurred())

			lcs, err := client.LCS(ctx, &redis.LCSQuery{
				Key1: "key1",
				Key2: "key2",
			}).Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(lcs.MatchString).To(Equal("mytext"))

			lcs, err = client.LCS(ctx, &redis.LCSQuery{
				Key1: "nonexistent_key1",
				Key2: "key2",
			}).Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(lcs.MatchString).To(Equal(""))

			lcs, err = client.LCS(ctx, &redis.LCSQuery{
				Key1: "key1",
				Key2: "key2",
				Len:  true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(lcs.MatchString).To(Equal(""))
			Expect(lcs.Len).To(Equal(int64(6)))

			lcs, err = client.LCS(ctx, &redis.LCSQuery{
				Key1: "key1",
				Key2: "key2",
				Idx:  true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(lcs.MatchString).To(Equal(""))
			Expect(lcs.Len).To(Equal(int64(6)))
			Expect(lcs.Matches).To(Equal([]redis.LCSMatchedPosition{
				{
					Key1:     redis.LCSPosition{Start: 4, End: 7},
					Key2:     redis.LCSPosition{Start: 5, End: 8},
					MatchLen: 0,
				},
				{
					Key1:     redis.LCSPosition{Start: 2, End: 3},
					Key2:     redis.LCSPosition{Start: 0, End: 1},
					MatchLen: 0,
				},
			}))

			lcs, err = client.LCS(ctx, &redis.LCSQuery{
				Key1:         "key1",
				Key2:         "key2",
				Idx:          true,
				MinMatchLen:  3,
				WithMatchLen: true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(lcs.MatchString).To(Equal(""))
			Expect(lcs.Len).To(Equal(int64(6)))
			Expect(lcs.Matches).To(Equal([]redis.LCSMatchedPosition{
				{
					Key1:     redis.LCSPosition{Start: 4, End: 7},
					Key2:     redis.LCSPosition{Start: 5, End: 8},
					MatchLen: 4,
				},
			}))

			_, err = client.Set(ctx, "keywithstringvalue", "golang", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.LPush(ctx, "keywithnonstringvalue", "somevalue").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.LCS(ctx, &redis.LCSQuery{
				Key1: "keywithstringvalue",
				Key2: "keywithnonstringvalue",
			}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("ERR The specified keys must contain string values"))
		})

		It("should LIndex", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, "list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lIndex := client.LIndex(ctx, "list", 0)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("Hello"))

			lIndex = client.LIndex(ctx, "list", -1)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("World"))

			lIndex = client.LIndex(ctx, "list", 3)
			Expect(lIndex.Err()).To(Equal(redis.Nil))
			Expect(lIndex.Val()).To(Equal(""))
		})

		It("should LInsert", func() {
			rPush := client.RPush(ctx, "list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lInsert := client.LInsert(ctx, "list", "BEFORE", "World", "There")
			Expect(lInsert.Err()).NotTo(HaveOccurred())
			Expect(lInsert.Val()).To(Equal(int64(3)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "There", "World"}))
		})

		It("should LMPop", Label("NonRedisEnterprise"), func() {
			err := client.LPush(ctx, "list1", "one", "two", "three", "four", "five").Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.LPush(ctx, "list2", "a", "b", "c", "d", "e").Err()
			Expect(err).NotTo(HaveOccurred())

			key, val, err := client.LMPop(ctx, "left", 3, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list1"))
			Expect(val).To(Equal([]string{"five", "four", "three"}))

			key, val, err = client.LMPop(ctx, "right", 3, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list1"))
			Expect(val).To(Equal([]string{"one", "two"}))

			key, val, err = client.LMPop(ctx, "left", 1, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list2"))
			Expect(val).To(Equal([]string{"e"}))

			key, val, err = client.LMPop(ctx, "right", 10, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list2"))
			Expect(val).To(Equal([]string{"a", "b", "c", "d"}))

			err = client.LMPop(ctx, "left", 10, "list1", "list2").Err()
			Expect(err).To(Equal(redis.Nil))

			err = client.Set(ctx, "list3", 1024, 0).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.LMPop(ctx, "left", 10, "list1", "list2", "list3").Err()
			Expect(err.Error()).To(Equal("WRONGTYPE Operation against a key holding the wrong kind of value"))

			err = client.LMPop(ctx, "right", 0, "list1", "list2").Err()
			Expect(err).To(HaveOccurred())
		})

		It("should BLMPop", Label("NonRedisEnterprise"), func() {
			err := client.LPush(ctx, "list1", "one", "two", "three", "four", "five").Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.LPush(ctx, "list2", "a", "b", "c", "d", "e").Err()
			Expect(err).NotTo(HaveOccurred())

			key, val, err := client.BLMPop(ctx, 0, "left", 3, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list1"))
			Expect(val).To(Equal([]string{"five", "four", "three"}))

			key, val, err = client.BLMPop(ctx, 0, "right", 3, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list1"))
			Expect(val).To(Equal([]string{"one", "two"}))

			key, val, err = client.BLMPop(ctx, 0, "left", 1, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list2"))
			Expect(val).To(Equal([]string{"e"}))

			key, val, err = client.BLMPop(ctx, 0, "right", 10, "list1", "list2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("list2"))
			Expect(val).To(Equal([]string{"a", "b", "c", "d"}))
		})

		It("should BLMPopBlocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				key, val, err := client.BLMPop(ctx, 0, "left", 1, "list_list").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(key).To(Equal("list_list"))
				Expect(val).To(Equal([]string{"a"}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BLMPop is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			_, err := client.LPush(ctx, "list_list", "a").Result()
			Expect(err).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BLMPop is still blocked")
			}
		})

		It("should BLMPop timeout", func() {
			_, val, err := client.BLMPop(ctx, time.Second, "left", 1, "list1").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(BeNil())

			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(2)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
		})

		It("should LLen", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, "list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lLen := client.LLen(ctx, "list")
			Expect(lLen.Err()).NotTo(HaveOccurred())
			Expect(lLen.Val()).To(Equal(int64(2)))
		})

		It("should LPop", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPop := client.LPop(ctx, "list")
			Expect(lPop.Err()).NotTo(HaveOccurred())
			Expect(lPop.Val()).To(Equal("one"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should LPopCount", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "four")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPopCount := client.LPopCount(ctx, "list", 2)
			Expect(lPopCount.Err()).NotTo(HaveOccurred())
			Expect(lPopCount.Val()).To(Equal([]string{"one", "two"}))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"three", "four"}))
		})

		It("should LPos", func() {
			rPush := client.RPush(ctx, "list", "a")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "b")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "c")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "b")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPos := client.LPos(ctx, "list", "b", redis.LPosArgs{})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal(int64(1)))

			lPos = client.LPos(ctx, "list", "b", redis.LPosArgs{Rank: 2})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal(int64(3)))

			lPos = client.LPos(ctx, "list", "b", redis.LPosArgs{Rank: -2})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal(int64(1)))

			lPos = client.LPos(ctx, "list", "b", redis.LPosArgs{Rank: 2, MaxLen: 1})
			Expect(lPos.Err()).To(Equal(redis.Nil))

			lPos = client.LPos(ctx, "list", "z", redis.LPosArgs{})
			Expect(lPos.Err()).To(Equal(redis.Nil))
		})

		It("should LPosCount", func() {
			rPush := client.RPush(ctx, "list", "a")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "b")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "c")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "b")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPos := client.LPosCount(ctx, "list", "b", 2, redis.LPosArgs{})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal([]int64{1, 3}))

			lPos = client.LPosCount(ctx, "list", "b", 2, redis.LPosArgs{Rank: 2})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal([]int64{3}))

			lPos = client.LPosCount(ctx, "list", "b", 1, redis.LPosArgs{Rank: 1, MaxLen: 1})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal([]int64{}))

			lPos = client.LPosCount(ctx, "list", "b", 1, redis.LPosArgs{Rank: 1, MaxLen: 0})
			Expect(lPos.Err()).NotTo(HaveOccurred())
			Expect(lPos.Val()).To(Equal([]int64{1}))
		})

		It("should LPush", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, "list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should LPushX", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lPushX := client.LPushX(ctx, "list", "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(2)))

			lPush = client.LPush(ctx, "list1", "three")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			Expect(lPush.Val()).To(Equal(int64(1)))

			lPushX = client.LPushX(ctx, "list1", "two", "one")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(3)))

			lPushX = client.LPushX(ctx, "list2", "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange(ctx, "list1", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRange", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange(ctx, "list", 0, 0)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one"}))

			lRange = client.LRange(ctx, "list", -3, 2)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list", -100, 100)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list", 5, 10)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRem", func() {
			rPush := client.RPush(ctx, "list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "key")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRem := client.LRem(ctx, "list", -2, "hello")
			Expect(lRem.Err()).NotTo(HaveOccurred())
			Expect(lRem.Val()).To(Equal(int64(2)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"hello", "key"}))
		})

		It("should LSet", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lSet := client.LSet(ctx, "list", 0, "four")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lSet = client.LSet(ctx, "list", -2, "five")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"four", "five", "three"}))
		})

		It("should LTrim", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lTrim := client.LTrim(ctx, "list", 1, -1)
			Expect(lTrim.Err()).NotTo(HaveOccurred())
			Expect(lTrim.Val()).To(Equal("OK"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should RPop", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			rPop := client.RPop(ctx, "list")
			Expect(rPop.Err()).NotTo(HaveOccurred())
			Expect(rPop.Val()).To(Equal("three"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))
		})

		It("should RPopCount", func() {
			rPush := client.RPush(ctx, "list", "one", "two", "three", "four")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(4)))

			rPopCount := client.RPopCount(ctx, "list", 2)
			Expect(rPopCount.Err()).NotTo(HaveOccurred())
			Expect(rPopCount.Val()).To(Equal([]string{"four", "three"}))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))
		})

		It("should RPopLPush", Label("NonRedisEnterprise"), func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			rPopLPush := client.RPopLPush(ctx, "list", "list2")
			Expect(rPopLPush.Err()).NotTo(HaveOccurred())
			Expect(rPopLPush.Val()).To(Equal("three"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"three"}))
		})

		It("should RPush", func() {
			rPush := client.RPush(ctx, "list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPush = client.RPush(ctx, "list", "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(2)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should RPushX", func() {
			rPush := client.RPush(ctx, "list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX := client.RPushX(ctx, "list", "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(2)))

			rPush = client.RPush(ctx, "list1", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX = client.RPushX(ctx, "list1", "two", "three")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(3)))

			rPushX = client.RPushX(ctx, "list2", "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange(ctx, "list1", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LMove", Label("NonRedisEnterprise"), func() {
			rPush := client.RPush(ctx, "lmove1", "ichi")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPush = client.RPush(ctx, "lmove1", "ni")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(2)))

			rPush = client.RPush(ctx, "lmove1", "san")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(3)))

			lMove := client.LMove(ctx, "lmove1", "lmove2", "RIGHT", "LEFT")
			Expect(lMove.Err()).NotTo(HaveOccurred())
			Expect(lMove.Val()).To(Equal("san"))

			lRange := client.LRange(ctx, "lmove2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"san"}))
		})

		It("should BLMove", Label("NonRedisEnterprise"), func() {
			rPush := client.RPush(ctx, "blmove1", "ichi")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPush = client.RPush(ctx, "blmove1", "ni")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(2)))

			rPush = client.RPush(ctx, "blmove1", "san")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(3)))

			blMove := client.BLMove(ctx, "blmove1", "blmove2", "RIGHT", "LEFT", time.Second)
			Expect(blMove.Err()).NotTo(HaveOccurred())
			Expect(blMove.Val()).To(Equal("san"))

			lRange := client.LRange(ctx, "blmove2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"san"}))
		})
	})

	Describe("sets", func() {
		It("should SAdd", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(0)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SAdd strings", func() {
			set := []string{"Hello", "World", "World"}
			sAdd := client.SAdd(ctx, "set", set)
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(2)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SCard", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sCard := client.SCard(ctx, "set")
			Expect(sCard.Err()).NotTo(HaveOccurred())
			Expect(sCard.Val()).To(Equal(int64(2)))
		})

		It("should SDiff", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sDiff := client.SDiff(ctx, "set1", "set2")
			Expect(sDiff.Err()).NotTo(HaveOccurred())
			Expect(sDiff.Val()).To(ConsistOf([]string{"a", "b"}))
		})

		It("should SDiffStore", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sDiffStore := client.SDiffStore(ctx, "set", "set1", "set2")
			Expect(sDiffStore.Err()).NotTo(HaveOccurred())
			Expect(sDiffStore.Val()).To(Equal(int64(2)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"a", "b"}))
		})

		It("should SInter", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sInter := client.SInter(ctx, "set1", "set2")
			Expect(sInter.Err()).NotTo(HaveOccurred())
			Expect(sInter.Val()).To(Equal([]string{"c"}))
		})

		It("should SInterCard", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			// limit 0 means no limit,see https://redis.io/commands/sintercard/ for more details
			sInterCard := client.SInterCard(ctx, 0, "set1", "set2")
			Expect(sInterCard.Err()).NotTo(HaveOccurred())
			Expect(sInterCard.Val()).To(Equal(int64(2)))

			sInterCard = client.SInterCard(ctx, 1, "set1", "set2")
			Expect(sInterCard.Err()).NotTo(HaveOccurred())
			Expect(sInterCard.Val()).To(Equal(int64(1)))

			sInterCard = client.SInterCard(ctx, 3, "set1", "set2")
			Expect(sInterCard.Err()).NotTo(HaveOccurred())
			Expect(sInterCard.Val()).To(Equal(int64(2)))
		})

		It("should SInterStore", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sInterStore := client.SInterStore(ctx, "set", "set1", "set2")
			Expect(sInterStore.Err()).NotTo(HaveOccurred())
			Expect(sInterStore.Val()).To(Equal(int64(1)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(Equal([]string{"c"}))
		})

		It("should IsMember", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sIsMember := client.SIsMember(ctx, "set", "one")
			Expect(sIsMember.Err()).NotTo(HaveOccurred())
			Expect(sIsMember.Val()).To(Equal(true))

			sIsMember = client.SIsMember(ctx, "set", "two")
			Expect(sIsMember.Err()).NotTo(HaveOccurred())
			Expect(sIsMember.Val()).To(Equal(false))
		})

		It("should SMIsMember", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMIsMember := client.SMIsMember(ctx, "set", "one", "two")
			Expect(sMIsMember.Err()).NotTo(HaveOccurred())
			Expect(sMIsMember.Val()).To(Equal([]bool{true, false}))
		})

		It("should SMembers", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SMembersMap", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMembersMap := client.SMembersMap(ctx, "set")
			Expect(sMembersMap.Err()).NotTo(HaveOccurred())
			Expect(sMembersMap.Val()).To(Equal(map[string]struct{}{"Hello": {}, "World": {}}))
		})

		It("should SMove", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMove := client.SMove(ctx, "set1", "set2", "two")
			Expect(sMove.Err()).NotTo(HaveOccurred())
			Expect(sMove.Val()).To(Equal(true))

			sMembers := client.SMembers(ctx, "set1")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(Equal([]string{"one"}))

			sMembers = client.SMembers(ctx, "set2")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"three", "two"}))
		})

		It("should SPop", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sPop := client.SPop(ctx, "set")
			Expect(sPop.Err()).NotTo(HaveOccurred())
			Expect(sPop.Val()).NotTo(Equal(""))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(2))
		})

		It("should SPopN", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "four")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sPopN := client.SPopN(ctx, "set", 1)
			Expect(sPopN.Err()).NotTo(HaveOccurred())
			Expect(sPopN.Val()).NotTo(Equal([]string{""}))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(3))

			sPopN = client.SPopN(ctx, "set", 4)
			Expect(sPopN.Err()).NotTo(HaveOccurred())
			Expect(sPopN.Val()).To(HaveLen(3))

			sMembers = client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(0))
		})

		It("should SRandMember and SRandMemberN", func() {
			err := client.SAdd(ctx, "set", "one").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.SAdd(ctx, "set", "two").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.SAdd(ctx, "set", "three").Err()
			Expect(err).NotTo(HaveOccurred())

			members, err := client.SMembers(ctx, "set").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(HaveLen(3))

			member, err := client.SRandMember(ctx, "set").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(member).NotTo(Equal(""))

			members, err = client.SRandMemberN(ctx, "set", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(HaveLen(2))
		})

		It("should SRem", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sRem := client.SRem(ctx, "set", "one")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(1)))

			sRem = client.SRem(ctx, "set", "four")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(0)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"three", "two"}))
		})

		It("should SUnion", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sUnion := client.SUnion(ctx, "set1", "set2")
			Expect(sUnion.Err()).NotTo(HaveOccurred())
			Expect(sUnion.Val()).To(HaveLen(5))
		})

		It("should SUnionStore", Label("NonRedisEnterprise"), func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sUnionStore := client.SUnionStore(ctx, "set", "set1", "set2")
			Expect(sUnionStore.Err()).NotTo(HaveOccurred())
			Expect(sUnionStore.Val()).To(Equal(int64(5)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(5))
		})
	})

	Describe("sorted sets", func() {
		It("should BZPopMax", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			member, err := client.BZPopMax(ctx, 0, "zset1", "zset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(member).To(Equal(&redis.ZWithKey{
				Z: redis.Z{
					Score:  3,
					Member: "three",
				},
				Key: "zset1",
			}))
		})

		It("should BZPopMax blocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				bZPopMax := client.BZPopMax(ctx, 0, "zset")
				Expect(bZPopMax.Err()).NotTo(HaveOccurred())
				Expect(bZPopMax.Val()).To(Equal(&redis.ZWithKey{
					Z: redis.Z{
						Member: "a",
						Score:  1,
					},
					Key: "zset",
				}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BZPopMax is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			zAdd := client.ZAdd(ctx, "zset", redis.Z{
				Member: "a",
				Score:  1,
			})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BZPopMax is still blocked")
			}
		})

		It("should BZPopMax timeout", func() {
			val, err := client.BZPopMax(ctx, time.Second, "zset1").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(BeNil())

			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(2)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
		})

		It("should BZPopMin", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			member, err := client.BZPopMin(ctx, 0, "zset1", "zset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(member).To(Equal(&redis.ZWithKey{
				Z: redis.Z{
					Score:  1,
					Member: "one",
				},
				Key: "zset1",
			}))
		})

		It("should BZPopMin blocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				bZPopMin := client.BZPopMin(ctx, 0, "zset")
				Expect(bZPopMin.Err()).NotTo(HaveOccurred())
				Expect(bZPopMin.Val()).To(Equal(&redis.ZWithKey{
					Z: redis.Z{
						Member: "a",
						Score:  1,
					},
					Key: "zset",
				}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BZPopMin is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			zAdd := client.ZAdd(ctx, "zset", redis.Z{
				Member: "a",
				Score:  1,
			})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BZPopMin is still blocked")
			}
		})

		It("should BZPopMin timeout", func() {
			val, err := client.BZPopMin(ctx, time.Second, "zset1").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(BeNil())

			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(2)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
		})

		It("should ZAdd", func() {
			added, err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "uno",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: "two",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  1,
				Member: "uno",
			}, {
				Score:  3,
				Member: "two",
			}}))
		})

		It("should ZAdd bytes", func() {
			added, err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: []byte("one"),
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: []byte("uno"),
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: []byte("two"),
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: []byte("two"),
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  1,
				Member: "uno",
			}, {
				Score:  3,
				Member: "two",
			}}))
		})

		It("should ZAddArgsGTAndLT", func() {
			// Test only the GT+LT options.
			added, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				GT:      true,
				Members: []redis.Z{{Score: 1, Member: "one"}},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

			added, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				GT:      true,
				Members: []redis.Z{{Score: 2, Member: "one"}},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))

			added, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				LT:      true,
				Members: []redis.Z{{Score: 1, Member: "one"}},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
		})

		It("should ZAddArgsLT", func() {
			added, err := client.ZAddLT(ctx, "zset", redis.Z{
				Score:  2,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))

			added, err = client.ZAddLT(ctx, "zset", redis.Z{
				Score:  3,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))

			added, err = client.ZAddLT(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
		})

		It("should ZAddArgsGT", func() {
			added, err := client.ZAddGT(ctx, "zset", redis.Z{
				Score:  2,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))

			added, err = client.ZAddGT(ctx, "zset", redis.Z{
				Score:  3,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 3, Member: "one"}}))

			added, err = client.ZAddGT(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 3, Member: "one"}}))
		})

		It("should ZAddArgsNX", func() {
			added, err := client.ZAddNX(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

			added, err = client.ZAddNX(ctx, "zset", redis.Z{
				Score:  2,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
		})

		It("should ZAddArgsXX", func() {
			added, err := client.ZAddXX(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(BeEmpty())

			added, err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAddXX(ctx, "zset", redis.Z{
				Score:  2,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
		})

		It("should ZAddArgsCh", func() {
			changed, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				Ch: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(int64(1)))

			changed, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				Ch: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(int64(0)))
		})

		It("should ZAddArgsNXCh", func() {
			changed, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				NX: true,
				Ch: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

			changed, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				NX: true,
				Ch: true,
				Members: []redis.Z{
					{Score: 2, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(int64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}}))
		})

		It("should ZAddArgsXXCh", func() {
			changed, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				XX: true,
				Ch: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(int64(0)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(BeEmpty())

			added, err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			changed, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				XX: true,
				Ch: true,
				Members: []redis.Z{
					{Score: 2, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(int64(1)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
		})

		It("should ZAddArgsIncr", func() {
			score, err := client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(score).To(Equal(float64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

			score, err = client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(score).To(Equal(float64(2)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
		})

		It("should ZAddArgsIncrNX", func() {
			score, err := client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
				NX: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(score).To(Equal(float64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

			score, err = client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
				NX: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(score).To(Equal(float64(0)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
		})

		It("should ZAddArgsIncrXX", func() {
			score, err := client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
				XX: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(score).To(Equal(float64(0)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(BeEmpty())

			added, err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			score, err = client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
				XX: true,
				Members: []redis.Z{
					{Score: 1, Member: "one"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(score).To(Equal(float64(2)))

			vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
		})

		It("should ZCard", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			card, err := client.ZCard(ctx, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(card).To(Equal(int64(2)))
		})

		It("should ZCount", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			count, err := client.ZCount(ctx, "zset", "-inf", "+inf").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(int64(3)))

			count, err = client.ZCount(ctx, "zset", "(1", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(int64(2)))

			count, err = client.ZLexCount(ctx, "zset", "-", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(int64(3)))
		})

		It("should ZIncrBy", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.ZIncrBy(ctx, "zset", 2, "one").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(float64(3)))

			val, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{
				Score:  2,
				Member: "two",
			}, {
				Score:  3,
				Member: "one",
			}}))
		})

		It("should ZInterStore", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset3", redis.Z{Score: 3, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.ZInterStore(ctx, "out", &redis.ZStore{
				Keys:    []string{"zset1", "zset2"},
				Weights: []float64{2, 3},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			vals, err := client.ZRangeWithScores(ctx, "out", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  5,
				Member: "one",
			}, {
				Score:  10,
				Member: "two",
			}}))
		})

		It("should ZMPop", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			key, elems, err := client.ZMPop(ctx, "min", 1, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("zset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}}))

			_, _, err = client.ZMPop(ctx, "min", 1, "nosuchkey").Result()
			Expect(err).To(Equal(redis.Nil))

			err = client.ZAdd(ctx, "myzset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "myzset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "myzset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			key, elems, err = client.ZMPop(ctx, "min", 1, "myzset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("myzset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}}))

			key, elems, err = client.ZMPop(ctx, "max", 10, "myzset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("myzset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}}))

			err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 4, Member: "four"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 5, Member: "five"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 6, Member: "six"}).Err()
			Expect(err).NotTo(HaveOccurred())

			key, elems, err = client.ZMPop(ctx, "min", 10, "myzset", "myzset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("myzset2"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  4,
				Member: "four",
			}, {
				Score:  5,
				Member: "five",
			}, {
				Score:  6,
				Member: "six",
			}}))
		})

		It("should BZMPop", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			key, elems, err := client.BZMPop(ctx, 0, "min", 1, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("zset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}}))
			key, elems, err = client.BZMPop(ctx, 0, "max", 1, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("zset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}}))
			key, elems, err = client.BZMPop(ctx, 0, "min", 10, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("zset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  2,
				Member: "two",
			}}))

			key, elems, err = client.BZMPop(ctx, 0, "max", 10, "zset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("zset2"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  1,
				Member: "one",
			}}))

			err = client.ZAdd(ctx, "myzset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			key, elems, err = client.BZMPop(ctx, 0, "min", 10, "myzset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("myzset"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}}))

			err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 4, Member: "four"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 5, Member: "five"}).Err()
			Expect(err).NotTo(HaveOccurred())

			key, elems, err = client.BZMPop(ctx, 0, "min", 10, "myzset", "myzset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("myzset2"))
			Expect(elems).To(Equal([]redis.Z{{
				Score:  4,
				Member: "four",
			}, {
				Score:  5,
				Member: "five",
			}}))
		})

		It("should BZMPopBlocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				key, elems, err := client.BZMPop(ctx, 0, "min", 1, "list_list").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(key).To(Equal("list_list"))
				Expect(elems).To(Equal([]redis.Z{{
					Score:  1,
					Member: "one",
				}}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BZMPop is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			err := client.ZAdd(ctx, "list_list", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BZMPop is still blocked")
			}
		})

		It("should BZMPop timeout", func() {
			_, val, err := client.BZMPop(ctx, time.Second, "min", 1, "list1").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(BeNil())

			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(2)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
		})

		It("should ZMScore", func() {
			zmScore := client.ZMScore(ctx, "zset", "one", "three")
			Expect(zmScore.Err()).NotTo(HaveOccurred())
			Expect(zmScore.Val()).To(HaveLen(2))
			Expect(zmScore.Val()[0]).To(Equal(float64(0)))

			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zmScore = client.ZMScore(ctx, "zset", "one", "three")
			Expect(zmScore.Err()).NotTo(HaveOccurred())
			Expect(zmScore.Val()).To(HaveLen(2))
			Expect(zmScore.Val()[0]).To(Equal(float64(1)))

			zmScore = client.ZMScore(ctx, "zset", "four")
			Expect(zmScore.Err()).NotTo(HaveOccurred())
			Expect(zmScore.Val()).To(HaveLen(1))

			zmScore = client.ZMScore(ctx, "zset", "four", "one")
			Expect(zmScore.Err()).NotTo(HaveOccurred())
			Expect(zmScore.Val()).To(HaveLen(2))
		})

		It("should ZPopMax", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			members, err := client.ZPopMax(ctx, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}}))

			// adding back 3
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			members, err = client.ZPopMax(ctx, "zset", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}}))

			// adding back 2 & 3
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			members, err = client.ZPopMax(ctx, "zset", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  1,
				Member: "one",
			}}))
		})

		It("should ZPopMin", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  3,
				Member: "three",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			members, err := client.ZPopMin(ctx, "zset").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}}))

			// adding back 1
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			members, err = client.ZPopMin(ctx, "zset", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  2,
				Member: "two",
			}}))

			// adding back 1 & 2
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  1,
				Member: "one",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  2,
				Member: "two",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			members, err = client.ZPopMin(ctx, "zset", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  3,
				Member: "three",
			}}))
		})

		It("should ZRange", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRange := client.ZRange(ctx, "zset", 0, -1)
			Expect(zRange.Err()).NotTo(HaveOccurred())
			Expect(zRange.Val()).To(Equal([]string{"one", "two", "three"}))

			zRange = client.ZRange(ctx, "zset", 2, 3)
			Expect(zRange.Err()).NotTo(HaveOccurred())
			Expect(zRange.Val()).To(Equal([]string{"three"}))

			zRange = client.ZRange(ctx, "zset", -2, -1)
			Expect(zRange.Err()).NotTo(HaveOccurred())
			Expect(zRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should ZRangeWithScores", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  3,
				Member: "three",
			}}))

			vals, err = client.ZRangeWithScores(ctx, "zset", 2, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 3, Member: "three"}}))

			vals, err = client.ZRangeWithScores(ctx, "zset", -2, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  2,
				Member: "two",
			}, {
				Score:  3,
				Member: "three",
			}}))
		})

		It("should ZRangeArgs", func() {
			added, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				Members: []redis.Z{
					{Score: 1, Member: "one"},
					{Score: 2, Member: "two"},
					{Score: 3, Member: "three"},
					{Score: 4, Member: "four"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(4)))

			zRange, err := client.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:     "zset",
				Start:   1,
				Stop:    4,
				ByScore: true,
				Rev:     true,
				Offset:  1,
				Count:   2,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(zRange).To(Equal([]string{"three", "two"}))

			zRange, err = client.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:    "zset",
				Start:  "-",
				Stop:   "+",
				ByLex:  true,
				Rev:    true,
				Offset: 2,
				Count:  2,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(zRange).To(Equal([]string{"two", "one"}))

			zRange, err = client.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:     "zset",
				Start:   "(1",
				Stop:    "(4",
				ByScore: true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(zRange).To(Equal([]string{"two", "three"}))

			// withScores.
			zSlice, err := client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
				Key:     "zset",
				Start:   1,
				Stop:    4,
				ByScore: true,
				Rev:     true,
				Offset:  1,
				Count:   2,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(zSlice).To(Equal([]redis.Z{
				{Score: 3, Member: "three"},
				{Score: 2, Member: "two"},
			}))
		})

		It("should ZRangeByScore", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRangeByScore := client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{"one", "two", "three"}))

			zRangeByScore = client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
				Min: "1",
				Max: "2",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{"one", "two"}))

			zRangeByScore = client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
				Min: "(1",
				Max: "2",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{"two"}))

			zRangeByScore = client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
				Min: "(1",
				Max: "(2",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{}))
		})

		It("should ZRangeByLex", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{
				Score:  0,
				Member: "a",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  0,
				Member: "b",
			}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{
				Score:  0,
				Member: "c",
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRangeByLex := client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
				Min: "-",
				Max: "+",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{"a", "b", "c"}))

			zRangeByLex = client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
				Min: "[a",
				Max: "[b",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{"a", "b"}))

			zRangeByLex = client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
				Min: "(a",
				Max: "[b",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{"b"}))

			zRangeByLex = client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
				Min: "(a",
				Max: "(b",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{}))
		})

		It("should ZRangeByScoreWithScoresMap", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  3,
				Member: "three",
			}}))

			vals, err = client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
				Min: "1",
				Max: "2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  2,
				Member: "two",
			}}))

			vals, err = client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
				Min: "(1",
				Max: "2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "two"}}))

			vals, err = client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
				Min: "(1",
				Max: "(2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{}))
		})

		It("should ZRangeStore", Label("NonRedisEnterprise"), func() {
			added, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
				Members: []redis.Z{
					{Score: 1, Member: "one"},
					{Score: 2, Member: "two"},
					{Score: 3, Member: "three"},
					{Score: 4, Member: "four"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(4)))

			rangeStore, err := client.ZRangeStore(ctx, "new-zset", redis.ZRangeArgs{
				Key:     "zset",
				Start:   1,
				Stop:    4,
				ByScore: true,
				Rev:     true,
				Offset:  1,
				Count:   2,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(rangeStore).To(Equal(int64(2)))

			zRange, err := client.ZRange(ctx, "new-zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(zRange).To(Equal([]string{"two", "three"}))
		})

		It("should ZRank", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRank := client.ZRank(ctx, "zset", "three")
			Expect(zRank.Err()).NotTo(HaveOccurred())
			Expect(zRank.Val()).To(Equal(int64(2)))

			zRank = client.ZRank(ctx, "zset", "four")
			Expect(zRank.Err()).To(Equal(redis.Nil))
			Expect(zRank.Val()).To(Equal(int64(0)))
		})

		It("should ZRankWithScore", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRankWithScore := client.ZRankWithScore(ctx, "zset", "one")
			Expect(zRankWithScore.Err()).NotTo(HaveOccurred())
			Expect(zRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 0, Score: 1}))

			zRankWithScore = client.ZRankWithScore(ctx, "zset", "two")
			Expect(zRankWithScore.Err()).NotTo(HaveOccurred())
			Expect(zRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 1, Score: 2}))

			zRankWithScore = client.ZRankWithScore(ctx, "zset", "three")
			Expect(zRankWithScore.Err()).NotTo(HaveOccurred())
			Expect(zRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 2, Score: 3}))

			zRankWithScore = client.ZRankWithScore(ctx, "zset", "four")
			Expect(zRankWithScore.Err()).To(HaveOccurred())
			Expect(zRankWithScore.Err()).To(Equal(redis.Nil))
		})

		It("should ZRem", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRem := client.ZRem(ctx, "zset", "two")
			Expect(zRem.Err()).NotTo(HaveOccurred())
			Expect(zRem.Val()).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  1,
				Member: "one",
			}, {
				Score:  3,
				Member: "three",
			}}))
		})

		It("should ZRemRangeByRank", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRemRangeByRank := client.ZRemRangeByRank(ctx, "zset", 0, 1)
			Expect(zRemRangeByRank.Err()).NotTo(HaveOccurred())
			Expect(zRemRangeByRank.Val()).To(Equal(int64(2)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}}))
		})

		It("should ZRemRangeByScore", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRemRangeByScore := client.ZRemRangeByScore(ctx, "zset", "-inf", "(2")
			Expect(zRemRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRemRangeByScore.Val()).To(Equal(int64(1)))

			vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  2,
				Member: "two",
			}, {
				Score:  3,
				Member: "three",
			}}))
		})

		It("should ZRemRangeByLex", func() {
			zz := []redis.Z{
				{Score: 0, Member: "aaaa"},
				{Score: 0, Member: "b"},
				{Score: 0, Member: "c"},
				{Score: 0, Member: "d"},
				{Score: 0, Member: "e"},
				{Score: 0, Member: "foo"},
				{Score: 0, Member: "zap"},
				{Score: 0, Member: "zip"},
				{Score: 0, Member: "ALPHA"},
				{Score: 0, Member: "alpha"},
			}
			for _, z := range zz {
				err := client.ZAdd(ctx, "zset", z).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			n, err := client.ZRemRangeByLex(ctx, "zset", "[alpha", "[omega").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(6)))

			vals, err := client.ZRange(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"ALPHA", "aaaa", "zap", "zip"}))
		})

		It("should ZRevRange", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRevRange := client.ZRevRange(ctx, "zset", 0, -1)
			Expect(zRevRange.Err()).NotTo(HaveOccurred())
			Expect(zRevRange.Val()).To(Equal([]string{"three", "two", "one"}))

			zRevRange = client.ZRevRange(ctx, "zset", 2, 3)
			Expect(zRevRange.Err()).NotTo(HaveOccurred())
			Expect(zRevRange.Val()).To(Equal([]string{"one"}))

			zRevRange = client.ZRevRange(ctx, "zset", -2, -1)
			Expect(zRevRange.Err()).NotTo(HaveOccurred())
			Expect(zRevRange.Val()).To(Equal([]string{"two", "one"}))
		})

		It("should ZRevRangeWithScoresMap", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.ZRevRangeWithScores(ctx, "zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  1,
				Member: "one",
			}}))

			val, err = client.ZRevRangeWithScores(ctx, "zset", 2, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

			val, err = client.ZRevRangeWithScores(ctx, "zset", -2, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{
				Score:  2,
				Member: "two",
			}, {
				Score:  1,
				Member: "one",
			}}))
		})

		It("should ZRevRangeByScore", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByScore(
				ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"three", "two", "one"}))

			vals, err = client.ZRevRangeByScore(
				ctx, "zset", &redis.ZRangeBy{Max: "2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"two"}))

			vals, err = client.ZRevRangeByScore(
				ctx, "zset", &redis.ZRangeBy{Max: "(2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{}))
		})

		It("should ZRevRangeByLex", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 0, Member: "a"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 0, Member: "b"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 0, Member: "c"}).Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByLex(
				ctx, "zset", &redis.ZRangeBy{Max: "+", Min: "-"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"c", "b", "a"}))

			vals, err = client.ZRevRangeByLex(
				ctx, "zset", &redis.ZRangeBy{Max: "[b", Min: "(a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"b"}))

			vals, err = client.ZRevRangeByLex(
				ctx, "zset", &redis.ZRangeBy{Max: "(b", Min: "(a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{}))
		})

		It("should ZRevRangeByScoreWithScores", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByScoreWithScores(
				ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  1,
				Member: "one",
			}}))
		})

		It("should ZRevRangeByScoreWithScoresMap", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByScoreWithScores(
				ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}, {
				Score:  2,
				Member: "two",
			}, {
				Score:  1,
				Member: "one",
			}}))

			vals, err = client.ZRevRangeByScoreWithScores(
				ctx, "zset", &redis.ZRangeBy{Max: "2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "two"}}))

			vals, err = client.ZRevRangeByScoreWithScores(
				ctx, "zset", &redis.ZRangeBy{Max: "(2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{}))
		})

		It("should ZRevRank", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRevRank := client.ZRevRank(ctx, "zset", "one")
			Expect(zRevRank.Err()).NotTo(HaveOccurred())
			Expect(zRevRank.Val()).To(Equal(int64(2)))

			zRevRank = client.ZRevRank(ctx, "zset", "four")
			Expect(zRevRank.Err()).To(Equal(redis.Nil))
			Expect(zRevRank.Val()).To(Equal(int64(0)))
		})

		It("should ZRevRankWithScore", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			zRevRankWithScore := client.ZRevRankWithScore(ctx, "zset", "one")
			Expect(zRevRankWithScore.Err()).NotTo(HaveOccurred())
			Expect(zRevRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 2, Score: 1}))

			zRevRankWithScore = client.ZRevRankWithScore(ctx, "zset", "two")
			Expect(zRevRankWithScore.Err()).NotTo(HaveOccurred())
			Expect(zRevRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 1, Score: 2}))

			zRevRankWithScore = client.ZRevRankWithScore(ctx, "zset", "three")
			Expect(zRevRankWithScore.Err()).NotTo(HaveOccurred())
			Expect(zRevRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 0, Score: 3}))

			zRevRankWithScore = client.ZRevRankWithScore(ctx, "zset", "four")
			Expect(zRevRankWithScore.Err()).To(HaveOccurred())
			Expect(zRevRankWithScore.Err()).To(Equal(redis.Nil))
		})

		It("should ZScore", func() {
			zAdd := client.ZAdd(ctx, "zset", redis.Z{Score: 1.001, Member: "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zScore := client.ZScore(ctx, "zset", "one")
			Expect(zScore.Err()).NotTo(HaveOccurred())
			Expect(zScore.Val()).To(Equal(1.001))
		})

		It("should ZUnion", Label("NonRedisEnterprise"), func() {
			err := client.ZAddArgs(ctx, "zset1", redis.ZAddArgs{
				Members: []redis.Z{
					{Score: 1, Member: "one"},
					{Score: 2, Member: "two"},
				},
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ZAddArgs(ctx, "zset2", redis.ZAddArgs{
				Members: []redis.Z{
					{Score: 1, Member: "one"},
					{Score: 2, Member: "two"},
					{Score: 3, Member: "three"},
				},
			}).Err()
			Expect(err).NotTo(HaveOccurred())

			union, err := client.ZUnion(ctx, redis.ZStore{
				Keys:      []string{"zset1", "zset2"},
				Weights:   []float64{2, 3},
				Aggregate: "sum",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(union).To(Equal([]string{"one", "three", "two"}))

			unionScores, err := client.ZUnionWithScores(ctx, redis.ZStore{
				Keys:      []string{"zset1", "zset2"},
				Weights:   []float64{2, 3},
				Aggregate: "sum",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(unionScores).To(Equal([]redis.Z{
				{Score: 5, Member: "one"},
				{Score: 9, Member: "three"},
				{Score: 10, Member: "two"},
			}))
		})

		It("should ZUnionStore", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.ZUnionStore(ctx, "out", &redis.ZStore{
				Keys:    []string{"zset1", "zset2"},
				Weights: []float64{2, 3},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))

			val, err := client.ZRangeWithScores(ctx, "out", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{
				Score:  5,
				Member: "one",
			}, {
				Score:  9,
				Member: "three",
			}, {
				Score:  10,
				Member: "two",
			}}))
		})

		It("should ZRandMember", func() {
			err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())

			v := client.ZRandMember(ctx, "zset", 1)
			Expect(v.Err()).NotTo(HaveOccurred())
			Expect(v.Val()).To(Or(Equal([]string{"one"}), Equal([]string{"two"})))

			v = client.ZRandMember(ctx, "zset", 0)
			Expect(v.Err()).NotTo(HaveOccurred())
			Expect(v.Val()).To(HaveLen(0))

			kv, err := client.ZRandMemberWithScores(ctx, "zset", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(kv).To(Or(
				Equal([]redis.Z{{Member: "one", Score: 1}}),
				Equal([]redis.Z{{Member: "two", Score: 2}}),
			))
		})

		It("should ZDiff", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.ZDiff(ctx, "zset1", "zset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]string{"two", "three"}))
		})

		It("should ZDiffWithScores", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.ZDiffWithScores(ctx, "zset1", "zset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]redis.Z{
				{
					Member: "two",
					Score:  2,
				},
				{
					Member: "three",
					Score:  3,
				},
			}))
		})

		It("should ZInter", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.ZInter(ctx, &redis.ZStore{
				Keys: []string{"zset1", "zset2"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]string{"one", "two"}))
		})

		It("should ZInterCard", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			// limit 0 means no limit
			sInterCard := client.ZInterCard(ctx, 0, "zset1", "zset2")
			Expect(sInterCard.Err()).NotTo(HaveOccurred())
			Expect(sInterCard.Val()).To(Equal(int64(2)))

			sInterCard = client.ZInterCard(ctx, 1, "zset1", "zset2")
			Expect(sInterCard.Err()).NotTo(HaveOccurred())
			Expect(sInterCard.Val()).To(Equal(int64(1)))

			sInterCard = client.ZInterCard(ctx, 3, "zset1", "zset2")
			Expect(sInterCard.Err()).NotTo(HaveOccurred())
			Expect(sInterCard.Val()).To(Equal(int64(2)))
		})

		It("should ZInterWithScores", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.ZInterWithScores(ctx, &redis.ZStore{
				Keys:      []string{"zset1", "zset2"},
				Weights:   []float64{2, 3},
				Aggregate: "Max",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]redis.Z{
				{
					Member: "one",
					Score:  3,
				},
				{
					Member: "two",
					Score:  6,
				},
			}))
		})

		It("should ZDiffStore", Label("NonRedisEnterprise"), func() {
			err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
			Expect(err).NotTo(HaveOccurred())
			v, err := client.ZDiffStore(ctx, "out1", "zset1", "zset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(0)))
			v, err = client.ZDiffStore(ctx, "out1", "zset2", "zset1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(int64(1)))
			vals, err := client.ZRangeWithScores(ctx, "out1", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{
				Score:  3,
				Member: "three",
			}}))
		})
	})

	Describe("streams", func() {
		BeforeEach(func() {
			id, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream",
				ID:     "1-0",
				Values: map[string]interface{}{"uno": "un"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal("1-0"))

			// Values supports []interface{}.
			id, err = client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream",
				ID:     "2-0",
				Values: []interface{}{"dos", "deux"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal("2-0"))

			// Value supports []string.
			id, err = client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream",
				ID:     "3-0",
				Values: []string{"tres", "troix"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal("3-0"))
		})

		It("should XTrimMaxLen", func() {
			n, err := client.XTrimMaxLen(ctx, "stream", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))
		})

		It("should XTrimMaxLenApprox", func() {
			n, err := client.XTrimMaxLenApprox(ctx, "stream", 0, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))
		})

		It("should XTrimMinID", func() {
			n, err := client.XTrimMinID(ctx, "stream", "4-0").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))
		})

		It("should XTrimMinIDApprox", func() {
			n, err := client.XTrimMinIDApprox(ctx, "stream", "4-0", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))
		})

		It("should XAdd", func() {
			id, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream",
				Values: map[string]interface{}{"quatro": "quatre"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.XRange(ctx, "stream", "-", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.XMessage{
				{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
				{ID: id, Values: map[string]interface{}{"quatro": "quatre"}},
			}))
		})

		It("should XAdd with MaxLen", func() {
			id, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream",
				MaxLen: 1,
				Values: map[string]interface{}{"quatro": "quatre"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.XRange(ctx, "stream", "-", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.XMessage{
				{ID: id, Values: map[string]interface{}{"quatro": "quatre"}},
			}))
		})

		It("should XAdd with MinID", func() {
			id, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: "stream",
				MinID:  "5-0",
				ID:     "4-0",
				Values: map[string]interface{}{"quatro": "quatre"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal("4-0"))

			vals, err := client.XRange(ctx, "stream", "-", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(HaveLen(0))
		})

		It("should XDel", func() {
			n, err := client.XDel(ctx, "stream", "1-0", "2-0", "3-0").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))
		})

		It("should XLen", func() {
			n, err := client.XLen(ctx, "stream").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))
		})

		It("should XRange", func() {
			msgs, err := client.XRange(ctx, "stream", "-", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
			}))

			msgs, err = client.XRange(ctx, "stream", "2", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
			}))

			msgs, err = client.XRange(ctx, "stream", "-", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
			}))
		})

		It("should XRangeN", func() {
			msgs, err := client.XRangeN(ctx, "stream", "-", "+", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
			}))

			msgs, err = client.XRangeN(ctx, "stream", "2", "+", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
			}))

			msgs, err = client.XRangeN(ctx, "stream", "-", "2", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
			}))
		})

		It("should XRevRange", func() {
			msgs, err := client.XRevRange(ctx, "stream", "+", "-").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
				{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
			}))

			msgs, err = client.XRevRange(ctx, "stream", "+", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
			}))
		})

		It("should XRevRangeN", func() {
			msgs, err := client.XRevRangeN(ctx, "stream", "+", "-", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
				{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
			}))

			msgs, err = client.XRevRangeN(ctx, "stream", "+", "2", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(Equal([]redis.XMessage{
				{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
			}))
		})

		It("should XRead", func() {
			res, err := client.XReadStreams(ctx, "stream", "0").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]redis.XStream{
				{
					Stream: "stream",
					Messages: []redis.XMessage{
						{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
						{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
						{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
					},
				},
			}))

			_, err = client.XReadStreams(ctx, "stream", "3").Result()
			Expect(err).To(Equal(redis.Nil))
		})

		It("should XRead", func() {
			res, err := client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{"stream", "0"},
				Count:   2,
				Block:   100 * time.Millisecond,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]redis.XStream{
				{
					Stream: "stream",
					Messages: []redis.XMessage{
						{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
						{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
					},
				},
			}))

			_, err = client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{"stream", "3"},
				Count:   1,
				Block:   100 * time.Millisecond,
			}).Result()
			Expect(err).To(Equal(redis.Nil))
		})

		Describe("group", func() {
			BeforeEach(func() {
				err := client.XGroupCreate(ctx, "stream", "group", "0").Err()
				Expect(err).NotTo(HaveOccurred())

				res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    "group",
					Consumer: "consumer",
					Streams:  []string{"stream", ">"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal([]redis.XStream{
					{
						Stream: "stream",
						Messages: []redis.XMessage{
							{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
							{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
							{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
						},
					},
				}))
			})

			AfterEach(func() {
				n, err := client.XGroupDestroy(ctx, "stream", "group").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))
			})

			It("should XReadGroup skip empty", func() {
				n, err := client.XDel(ctx, "stream", "2-0").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))

				res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    "group",
					Consumer: "consumer",
					Streams:  []string{"stream", "0"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal([]redis.XStream{
					{
						Stream: "stream",
						Messages: []redis.XMessage{
							{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
							{ID: "2-0", Values: nil},
							{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
						},
					},
				}))
			})

			It("should XGroupCreateMkStream", func() {
				err := client.XGroupCreateMkStream(ctx, "stream2", "group", "0").Err()
				Expect(err).NotTo(HaveOccurred())

				err = client.XGroupCreateMkStream(ctx, "stream2", "group", "0").Err()
				Expect(err).To(Equal(proto.RedisError("BUSYGROUP Consumer Group name already exists")))

				n, err := client.XGroupDestroy(ctx, "stream2", "group").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))

				n, err = client.Del(ctx, "stream2").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))
			})

			It("should XPending", func() {
				info, err := client.XPending(ctx, "stream", "group").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(info).To(Equal(&redis.XPending{
					Count:     3,
					Lower:     "1-0",
					Higher:    "3-0",
					Consumers: map[string]int64{"consumer": 3},
				}))
				args := &redis.XPendingExtArgs{
					Stream:   "stream",
					Group:    "group",
					Start:    "-",
					End:      "+",
					Count:    10,
					Consumer: "consumer",
				}
				infoExt, err := client.XPendingExt(ctx, args).Result()
				Expect(err).NotTo(HaveOccurred())
				for i := range infoExt {
					infoExt[i].Idle = 0
				}
				Expect(infoExt).To(Equal([]redis.XPendingExt{
					{ID: "1-0", Consumer: "consumer", Idle: 0, RetryCount: 1},
					{ID: "2-0", Consumer: "consumer", Idle: 0, RetryCount: 1},
					{ID: "3-0", Consumer: "consumer", Idle: 0, RetryCount: 1},
				}))

				args.Idle = 72 * time.Hour
				infoExt, err = client.XPendingExt(ctx, args).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(infoExt).To(HaveLen(0))
			})

			It("should XGroup Create Delete Consumer", func() {
				n, err := client.XGroupCreateConsumer(ctx, "stream", "group", "c1").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))

				n, err = client.XGroupDelConsumer(ctx, "stream", "group", "consumer").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(3)))
			})

			It("should XAutoClaim", func() {
				xca := &redis.XAutoClaimArgs{
					Stream:   "stream",
					Group:    "group",
					Consumer: "consumer",
					Start:    "-",
					Count:    2,
				}
				msgs, start, err := client.XAutoClaim(ctx, xca).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(start).To(Equal("3-0"))
				Expect(msgs).To(Equal([]redis.XMessage{{
					ID:     "1-0",
					Values: map[string]interface{}{"uno": "un"},
				}, {
					ID:     "2-0",
					Values: map[string]interface{}{"dos": "deux"},
				}}))

				xca.Start = start
				msgs, start, err = client.XAutoClaim(ctx, xca).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(start).To(Equal("0-0"))
				Expect(msgs).To(Equal([]redis.XMessage{{
					ID:     "3-0",
					Values: map[string]interface{}{"tres": "troix"},
				}}))

				ids, start, err := client.XAutoClaimJustID(ctx, xca).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(start).To(Equal("0-0"))
				Expect(ids).To(Equal([]string{"3-0"}))
			})

			It("should XClaim", func() {
				msgs, err := client.XClaim(ctx, &redis.XClaimArgs{
					Stream:   "stream",
					Group:    "group",
					Consumer: "consumer",
					Messages: []string{"1-0", "2-0", "3-0"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(msgs).To(Equal([]redis.XMessage{{
					ID:     "1-0",
					Values: map[string]interface{}{"uno": "un"},
				}, {
					ID:     "2-0",
					Values: map[string]interface{}{"dos": "deux"},
				}, {
					ID:     "3-0",
					Values: map[string]interface{}{"tres": "troix"},
				}}))

				ids, err := client.XClaimJustID(ctx, &redis.XClaimArgs{
					Stream:   "stream",
					Group:    "group",
					Consumer: "consumer",
					Messages: []string{"1-0", "2-0", "3-0"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ids).To(Equal([]string{"1-0", "2-0", "3-0"}))
			})

			It("should XAck", func() {
				n, err := client.XAck(ctx, "stream", "group", "1-0", "2-0", "4-0").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(2)))
			})
		})

		Describe("xinfo", func() {
			BeforeEach(func() {
				err := client.XGroupCreate(ctx, "stream", "group1", "0").Err()
				Expect(err).NotTo(HaveOccurred())

				res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    "group1",
					Consumer: "consumer1",
					Streams:  []string{"stream", ">"},
					Count:    2,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal([]redis.XStream{
					{
						Stream: "stream",
						Messages: []redis.XMessage{
							{ID: "1-0", Values: map[string]interface{}{"uno": "un"}},
							{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
						},
					},
				}))

				res, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    "group1",
					Consumer: "consumer2",
					Streams:  []string{"stream", ">"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal([]redis.XStream{
					{
						Stream: "stream",
						Messages: []redis.XMessage{
							{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
						},
					},
				}))

				err = client.XGroupCreate(ctx, "stream", "group2", "1-0").Err()
				Expect(err).NotTo(HaveOccurred())

				res, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    "group2",
					Consumer: "consumer1",
					Streams:  []string{"stream", ">"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal([]redis.XStream{
					{
						Stream: "stream",
						Messages: []redis.XMessage{
							{ID: "2-0", Values: map[string]interface{}{"dos": "deux"}},
							{ID: "3-0", Values: map[string]interface{}{"tres": "troix"}},
						},
					},
				}))
			})

			AfterEach(func() {
				n, err := client.XGroupDestroy(ctx, "stream", "group1").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))
				n, err = client.XGroupDestroy(ctx, "stream", "group2").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(1)))
			})

			It("should XINFO STREAM", func() {
				res, err := client.XInfoStream(ctx, "stream").Result()
				Expect(err).NotTo(HaveOccurred())
				res.RadixTreeKeys = 0
				res.RadixTreeNodes = 0

				Expect(res).To(Equal(&redis.XInfoStream{
					Length:            3,
					RadixTreeKeys:     0,
					RadixTreeNodes:    0,
					Groups:            2,
					LastGeneratedID:   "3-0",
					MaxDeletedEntryID: "0-0",
					EntriesAdded:      3,
					FirstEntry: redis.XMessage{
						ID:     "1-0",
						Values: map[string]interface{}{"uno": "un"},
					},
					LastEntry: redis.XMessage{
						ID:     "3-0",
						Values: map[string]interface{}{"tres": "troix"},
					},
					RecordedFirstEntryID: "1-0",
				}))

				// stream is empty
				n, err := client.XDel(ctx, "stream", "1-0", "2-0", "3-0").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(3)))

				res, err = client.XInfoStream(ctx, "stream").Result()
				Expect(err).NotTo(HaveOccurred())
				res.RadixTreeKeys = 0
				res.RadixTreeNodes = 0

				Expect(res).To(Equal(&redis.XInfoStream{
					Length:               0,
					RadixTreeKeys:        0,
					RadixTreeNodes:       0,
					Groups:               2,
					LastGeneratedID:      "3-0",
					MaxDeletedEntryID:    "3-0",
					EntriesAdded:         3,
					FirstEntry:           redis.XMessage{},
					LastEntry:            redis.XMessage{},
					RecordedFirstEntryID: "0-0",
				}))
			})

			It("should XINFO STREAM FULL", func() {
				res, err := client.XInfoStreamFull(ctx, "stream", 2).Result()
				Expect(err).NotTo(HaveOccurred())
				res.RadixTreeKeys = 0
				res.RadixTreeNodes = 0

				// Verify DeliveryTime
				now := time.Now()
				maxElapsed := 10 * time.Minute
				for k, g := range res.Groups {
					for k2, p := range g.Pending {
						Expect(now.Sub(p.DeliveryTime)).To(BeNumerically("<=", maxElapsed))
						res.Groups[k].Pending[k2].DeliveryTime = time.Time{}
					}
					for k3, c := range g.Consumers {
						Expect(now.Sub(c.SeenTime)).To(BeNumerically("<=", maxElapsed))
						Expect(now.Sub(c.ActiveTime)).To(BeNumerically("<=", maxElapsed))
						res.Groups[k].Consumers[k3].SeenTime = time.Time{}
						res.Groups[k].Consumers[k3].ActiveTime = time.Time{}

						for k4, p := range c.Pending {
							Expect(now.Sub(p.DeliveryTime)).To(BeNumerically("<=", maxElapsed))
							res.Groups[k].Consumers[k3].Pending[k4].DeliveryTime = time.Time{}
						}
					}
				}

				Expect(res.Groups).To(Equal([]redis.XInfoStreamGroup{
					{
						Name:            "group1",
						LastDeliveredID: "3-0",
						EntriesRead:     3,
						Lag:             0,
						PelCount:        3,
						Pending: []redis.XInfoStreamGroupPending{
							{ID: "1-0", Consumer: "consumer1", DeliveryTime: time.Time{}, DeliveryCount: 1},
							{ID: "2-0", Consumer: "consumer1", DeliveryTime: time.Time{}, DeliveryCount: 1},
						},
						Consumers: []redis.XInfoStreamConsumer{
							{
								Name:       "consumer1",
								SeenTime:   time.Time{},
								ActiveTime: time.Time{},
								PelCount:   2,
								Pending: []redis.XInfoStreamConsumerPending{
									{ID: "1-0", DeliveryTime: time.Time{}, DeliveryCount: 1},
									{ID: "2-0", DeliveryTime: time.Time{}, DeliveryCount: 1},
								},
							},
							{
								Name:       "consumer2",
								SeenTime:   time.Time{},
								ActiveTime: time.Time{},
								PelCount:   1,
								Pending: []redis.XInfoStreamConsumerPending{
									{ID: "3-0", DeliveryTime: time.Time{}, DeliveryCount: 1},
								},
							},
						},
					},
					{
						Name:            "group2",
						LastDeliveredID: "3-0",
						EntriesRead:     3,
						Lag:             0,
						PelCount:        2,
						Pending: []redis.XInfoStreamGroupPending{
							{ID: "2-0", Consumer: "consumer1", DeliveryTime: time.Time{}, DeliveryCount: 1},
							{ID: "3-0", Consumer: "consumer1", DeliveryTime: time.Time{}, DeliveryCount: 1},
						},
						Consumers: []redis.XInfoStreamConsumer{
							{
								Name:       "consumer1",
								SeenTime:   time.Time{},
								ActiveTime: time.Time{},
								PelCount:   2,
								Pending: []redis.XInfoStreamConsumerPending{
									{ID: "2-0", DeliveryTime: time.Time{}, DeliveryCount: 1},
									{ID: "3-0", DeliveryTime: time.Time{}, DeliveryCount: 1},
								},
							},
						},
					},
				}))

				// entries-read = nil
				Expect(client.Del(ctx, "xinfo-stream-full-stream").Err()).NotTo(HaveOccurred())
				id, err := client.XAdd(ctx, &redis.XAddArgs{
					Stream: "xinfo-stream-full-stream",
					ID:     "*",
					Values: []any{"k1", "v1"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(client.XGroupCreateMkStream(ctx, "xinfo-stream-full-stream", "xinfo-stream-full-group", "0").Err()).NotTo(HaveOccurred())
				res, err = client.XInfoStreamFull(ctx, "xinfo-stream-full-stream", 0).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal(&redis.XInfoStreamFull{
					Length:            1,
					RadixTreeKeys:     1,
					RadixTreeNodes:    2,
					LastGeneratedID:   id,
					MaxDeletedEntryID: "0-0",
					EntriesAdded:      1,
					Entries:           []redis.XMessage{{ID: id, Values: map[string]any{"k1": "v1"}}},
					Groups: []redis.XInfoStreamGroup{
						{
							Name:            "xinfo-stream-full-group",
							LastDeliveredID: "0-0",
							EntriesRead:     0,
							Lag:             1,
							PelCount:        0,
							Pending:         []redis.XInfoStreamGroupPending{},
							Consumers:       []redis.XInfoStreamConsumer{},
						},
					},
					RecordedFirstEntryID: id,
				}))
			})

			It("should XINFO GROUPS", func() {
				res, err := client.XInfoGroups(ctx, "stream").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal([]redis.XInfoGroup{
					{Name: "group1", Consumers: 2, Pending: 3, LastDeliveredID: "3-0", EntriesRead: 3},
					{Name: "group2", Consumers: 1, Pending: 2, LastDeliveredID: "3-0", EntriesRead: 3},
				}))
			})

			It("should XINFO CONSUMERS", func() {
				res, err := client.XInfoConsumers(ctx, "stream", "group1").Result()
				Expect(err).NotTo(HaveOccurred())
				for i := range res {
					res[i].Idle = 0
					res[i].Inactive = 0
				}

				Expect(res).To(Equal([]redis.XInfoConsumer{
					{Name: "consumer1", Pending: 2, Idle: 0, Inactive: 0},
					{Name: "consumer2", Pending: 1, Idle: 0, Inactive: 0},
				}))
			})
		})
	})

	Describe("Geo add and radius search", func() {
		BeforeEach(func() {
			n, err := client.GeoAdd(
				ctx,
				"Sicily",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "Catania"},
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should not add same geo location", func() {
			geoAdd := client.GeoAdd(
				ctx,
				"Sicily",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo"},
			)
			Expect(geoAdd.Err()).NotTo(HaveOccurred())
			Expect(geoAdd.Val()).To(Equal(int64(0)))
		})

		It("should search geo radius", func() {
			res, err := client.GeoRadius(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius: 200,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res[0].Name).To(Equal("Palermo"))
			Expect(res[1].Name).To(Equal("Catania"))
		})

		It("should geo radius and store the result", Label("NonRedisEnterprise"), func() {
			n, err := client.GeoRadiusStore(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius: 200,
				Store:  "result",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			res, err := client.ZRangeWithScores(ctx, "result", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainElement(redis.Z{
				Score:  3.479099956230698e+15,
				Member: "Palermo",
			}))
			Expect(res).To(ContainElement(redis.Z{
				Score:  3.479447370796909e+15,
				Member: "Catania",
			}))
		})

		It("should geo radius and store dist", Label("NonRedisEnterprise"), func() {
			n, err := client.GeoRadiusStore(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius:    200,
				StoreDist: "result",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			res, err := client.ZRangeWithScores(ctx, "result", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainElement(redis.Z{
				Score:  190.44242984775784,
				Member: "Palermo",
			}))
			Expect(res).To(ContainElement(redis.Z{
				Score:  56.4412578701582,
				Member: "Catania",
			}))
		})

		It("should search geo radius with options", func() {
			res, err := client.GeoRadius(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius:      200,
				Unit:        "km",
				WithGeoHash: true,
				WithCoord:   true,
				WithDist:    true,
				Count:       2,
				Sort:        "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res[1].Name).To(Equal("Palermo"))
			Expect(res[1].Dist).To(Equal(190.4424))
			Expect(res[1].GeoHash).To(Equal(int64(3479099956230698)))
			Expect(res[1].Longitude).To(Equal(13.361389338970184))
			Expect(res[1].Latitude).To(Equal(38.115556395496299))
			Expect(res[0].Name).To(Equal("Catania"))
			Expect(res[0].Dist).To(Equal(56.4413))
			Expect(res[0].GeoHash).To(Equal(int64(3479447370796909)))
			Expect(res[0].Longitude).To(Equal(15.087267458438873))
			Expect(res[0].Latitude).To(Equal(37.50266842333162))
		})

		It("should search geo radius with WithDist=false", func() {
			res, err := client.GeoRadius(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius:      200,
				Unit:        "km",
				WithGeoHash: true,
				WithCoord:   true,
				Count:       2,
				Sort:        "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res[1].Name).To(Equal("Palermo"))
			Expect(res[1].Dist).To(Equal(float64(0)))
			Expect(res[1].GeoHash).To(Equal(int64(3479099956230698)))
			Expect(res[1].Longitude).To(Equal(13.361389338970184))
			Expect(res[1].Latitude).To(Equal(38.115556395496299))
			Expect(res[0].Name).To(Equal("Catania"))
			Expect(res[0].Dist).To(Equal(float64(0)))
			Expect(res[0].GeoHash).To(Equal(int64(3479447370796909)))
			Expect(res[0].Longitude).To(Equal(15.087267458438873))
			Expect(res[0].Latitude).To(Equal(37.50266842333162))
		})

		It("should search geo radius by member with options", func() {
			res, err := client.GeoRadiusByMember(ctx, "Sicily", "Catania", &redis.GeoRadiusQuery{
				Radius:      200,
				Unit:        "km",
				WithGeoHash: true,
				WithCoord:   true,
				WithDist:    true,
				Count:       2,
				Sort:        "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(2))
			Expect(res[0].Name).To(Equal("Catania"))
			Expect(res[0].Dist).To(Equal(0.0))
			Expect(res[0].GeoHash).To(Equal(int64(3479447370796909)))
			Expect(res[0].Longitude).To(Equal(15.087267458438873))
			Expect(res[0].Latitude).To(Equal(37.50266842333162))
			Expect(res[1].Name).To(Equal("Palermo"))
			Expect(res[1].Dist).To(Equal(166.2742))
			Expect(res[1].GeoHash).To(Equal(int64(3479099956230698)))
			Expect(res[1].Longitude).To(Equal(13.361389338970184))
			Expect(res[1].Latitude).To(Equal(38.115556395496299))
		})

		It("should search geo radius with no results", func() {
			res, err := client.GeoRadius(ctx, "Sicily", 99, 37, &redis.GeoRadiusQuery{
				Radius:      200,
				Unit:        "km",
				WithGeoHash: true,
				WithCoord:   true,
				WithDist:    true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(0))
		})

		It("should get geo distance with unit options", func() {
			// From Redis CLI, note the difference in rounding in m vs
			// km on Redis itself.
			//
			// GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
			// GEODIST Sicily Palermo Catania m
			// "166274.15156960033"
			// GEODIST Sicily Palermo Catania km
			// "166.27415156960032"
			dist, err := client.GeoDist(ctx, "Sicily", "Palermo", "Catania", "km").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(dist).To(BeNumerically("~", 166.27, 0.01))

			dist, err = client.GeoDist(ctx, "Sicily", "Palermo", "Catania", "m").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(dist).To(BeNumerically("~", 166274.15, 0.01))
		})

		It("should get geo hash in string representation", func() {
			hashes, err := client.GeoHash(ctx, "Sicily", "Palermo", "Catania").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(hashes).To(ConsistOf([]string{"sqc8b49rny0", "sqdtr74hyu0"}))
		})

		It("should return geo position", func() {
			pos, err := client.GeoPos(ctx, "Sicily", "Palermo", "Catania", "NonExisting").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(ConsistOf([]*redis.GeoPos{
				{
					Longitude: 13.361389338970184,
					Latitude:  38.1155563954963,
				},
				{
					Longitude: 15.087267458438873,
					Latitude:  37.50266842333162,
				},
				nil,
			}))
		})

		It("should geo search", func() {
			q := &redis.GeoSearchQuery{
				Member:    "Catania",
				BoxWidth:  400,
				BoxHeight: 100,
				BoxUnit:   "km",
				Sort:      "asc",
			}
			val, err := client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.BoxHeight = 400
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania", "Palermo"}))

			q.Count = 1
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.CountAny = true
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Palermo"}))

			q = &redis.GeoSearchQuery{
				Member:     "Catania",
				Radius:     100,
				RadiusUnit: "km",
				Sort:       "asc",
			}
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.Radius = 400
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania", "Palermo"}))

			q.Count = 1
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.CountAny = true
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Palermo"}))

			q = &redis.GeoSearchQuery{
				Longitude: 15,
				Latitude:  37,
				BoxWidth:  200,
				BoxHeight: 200,
				BoxUnit:   "km",
				Sort:      "asc",
			}
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.BoxWidth, q.BoxHeight = 400, 400
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania", "Palermo"}))

			q.Count = 1
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.CountAny = true
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Palermo"}))

			q = &redis.GeoSearchQuery{
				Longitude:  15,
				Latitude:   37,
				Radius:     100,
				RadiusUnit: "km",
				Sort:       "asc",
			}
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.Radius = 200
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania", "Palermo"}))

			q.Count = 1
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Catania"}))

			q.CountAny = true
			val, err = client.GeoSearch(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]string{"Palermo"}))
		})

		It("should geo search with options", func() {
			q := &redis.GeoSearchLocationQuery{
				GeoSearchQuery: redis.GeoSearchQuery{
					Longitude:  15,
					Latitude:   37,
					Radius:     200,
					RadiusUnit: "km",
					Sort:       "asc",
				},
				WithHash:  true,
				WithDist:  true,
				WithCoord: true,
			}
			val, err := client.GeoSearchLocation(ctx, "Sicily", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.GeoLocation{
				{
					Name:      "Catania",
					Longitude: 15.08726745843887329,
					Latitude:  37.50266842333162032,
					Dist:      56.4413,
					GeoHash:   3479447370796909,
				},
				{
					Name:      "Palermo",
					Longitude: 13.36138933897018433,
					Latitude:  38.11555639549629859,
					Dist:      190.4424,
					GeoHash:   3479099956230698,
				},
			}))
		})

		It("should geo search store", Label("NonRedisEnterprise"), func() {
			q := &redis.GeoSearchStoreQuery{
				GeoSearchQuery: redis.GeoSearchQuery{
					Longitude:  15,
					Latitude:   37,
					Radius:     200,
					RadiusUnit: "km",
					Sort:       "asc",
				},
				StoreDist: false,
			}

			val, err := client.GeoSearchStore(ctx, "Sicily", "key1", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(2)))

			q.StoreDist = true
			val, err = client.GeoSearchStore(ctx, "Sicily", "key2", q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(2)))

			loc, err := client.GeoSearchLocation(ctx, "key1", &redis.GeoSearchLocationQuery{
				GeoSearchQuery: q.GeoSearchQuery,
				WithCoord:      true,
				WithDist:       true,
				WithHash:       true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(loc).To(Equal([]redis.GeoLocation{
				{
					Name:      "Catania",
					Longitude: 15.08726745843887329,
					Latitude:  37.50266842333162032,
					Dist:      56.4413,
					GeoHash:   3479447370796909,
				},
				{
					Name:      "Palermo",
					Longitude: 13.36138933897018433,
					Latitude:  38.11555639549629859,
					Dist:      190.4424,
					GeoHash:   3479099956230698,
				},
			}))

			v, err := client.ZRangeWithScores(ctx, "key2", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]redis.Z{
				{
					Score:  56.441257870158204,
					Member: "Catania",
				},
				{
					Score:  190.44242984775784,
					Member: "Palermo",
				},
			}))
		})
	})

	Describe("marshaling/unmarshaling", func() {
		type convTest struct {
			value  interface{}
			wanted string
			dest   interface{}
		}

		convTests := []convTest{
			{nil, "", nil},
			{"hello", "hello", new(string)},
			{[]byte("hello"), "hello", new([]byte)},
			{1, "1", new(int)},
			{int8(1), "1", new(int8)},
			{int16(1), "1", new(int16)},
			{int32(1), "1", new(int32)},
			{int64(1), "1", new(int64)},
			{uint(1), "1", new(uint)},
			{uint8(1), "1", new(uint8)},
			{uint16(1), "1", new(uint16)},
			{uint32(1), "1", new(uint32)},
			{uint64(1), "1", new(uint64)},
			{float32(1.0), "1", new(float32)},
			{1.0, "1", new(float64)},
			{true, "1", new(bool)},
			{false, "0", new(bool)},
		}

		It("should convert to string", func() {
			for _, test := range convTests {
				err := client.Set(ctx, "key", test.value, 0).Err()
				Expect(err).NotTo(HaveOccurred())

				s, err := client.Get(ctx, "key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(s).To(Equal(test.wanted))

				if test.dest == nil {
					continue
				}

				err = client.Get(ctx, "key").Scan(test.dest)
				Expect(err).NotTo(HaveOccurred())
				Expect(deref(test.dest)).To(Equal(test.value))
			}
		})
	})

	Describe("json marshaling/unmarshaling", func() {
		BeforeEach(func() {
			value := &numberStruct{Number: 42}
			err := client.Set(ctx, "key", value, 0).Err()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should marshal custom values using json", func() {
			s, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal(`{"Number":42}`))
		})

		It("should scan custom values using json", func() {
			value := &numberStruct{}
			err := client.Get(ctx, "key").Scan(value)
			Expect(err).NotTo(HaveOccurred())
			Expect(value.Number).To(Equal(42))
		})
	})

	Describe("Eval", func() {
		It("returns keys and values", func() {
			vals, err := client.Eval(
				ctx,
				"return {KEYS[1],ARGV[1]}",
				[]string{"key"},
				"hello",
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"key", "hello"}))
		})

		It("returns all values after an error", func() {
			vals, err := client.Eval(
				ctx,
				`return {12, {err="error"}, "abc"}`,
				nil,
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{int64(12), proto.RedisError("error"), "abc"}))
		})
	})

	Describe("EvalRO", func() {
		It("returns keys and values", func() {
			vals, err := client.EvalRO(
				ctx,
				"return {KEYS[1],ARGV[1]}",
				[]string{"key"},
				"hello",
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"key", "hello"}))
		})

		It("returns all values after an error", func() {
			vals, err := client.EvalRO(
				ctx,
				`return {12, {err="error"}, "abc"}`,
				nil,
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{int64(12), proto.RedisError("error"), "abc"}))
		})
	})

	Describe("Functions", func() {
		var (
			q        redis.FunctionListQuery
			lib1Code string
			lib2Code string
			lib1     redis.Library
			lib2     redis.Library
		)

		BeforeEach(func() {
			flush := client.FunctionFlush(ctx)
			Expect(flush.Err()).NotTo(HaveOccurred())

			lib1 = redis.Library{
				Name:   "mylib1",
				Engine: "LUA",
				Functions: []redis.Function{
					{
						Name:        "lib1_func1",
						Description: "This is the func-1 of lib 1",
						Flags:       []string{"allow-oom", "allow-stale"},
					},
				},
				Code: `#!lua name=%s
					
                     local function f1(keys, args)
                        local hash = keys[1]  -- Get the key name
                        local time = redis.call('TIME')[1]  -- Get the current time from the Redis server

                        -- Add the current timestamp to the arguments that the user passed to the function, stored in args
                        table.insert(args, '_updated_at')
                        table.insert(args, time)

                        -- Run HSET with the updated argument list
                        return redis.call('HSET', hash, unpack(args))
                     end

					redis.register_function{
						function_name='%s',
						description ='%s',
						callback=f1,
						flags={'%s', '%s'}
					}`,
			}

			lib2 = redis.Library{
				Name:   "mylib2",
				Engine: "LUA",
				Functions: []redis.Function{
					{
						Name:  "lib2_func1",
						Flags: []string{},
					},
					{
						Name:        "lib2_func2",
						Description: "This is the func-2 of lib 2",
						Flags:       []string{"no-writes"},
					},
				},
				Code: `#!lua name=%s

					local function f1(keys, args)
						 return 'Function 1'
					end
					
					local function f2(keys, args)
						 return 'Function 2'
					end
					
					redis.register_function('%s', f1)
					redis.register_function{
						function_name='%s',
						description ='%s',
						callback=f2,
						flags={'%s'}
					}`,
			}

			lib1Code = fmt.Sprintf(lib1.Code, lib1.Name, lib1.Functions[0].Name,
				lib1.Functions[0].Description, lib1.Functions[0].Flags[0], lib1.Functions[0].Flags[1])
			lib2Code = fmt.Sprintf(lib2.Code, lib2.Name, lib2.Functions[0].Name,
				lib2.Functions[1].Name, lib2.Functions[1].Description, lib2.Functions[1].Flags[0])

			q = redis.FunctionListQuery{}
		})

		It("Loads a new library", Label("NonRedisEnterprise"), func() {
			functionLoad := client.FunctionLoad(ctx, lib1Code)
			Expect(functionLoad.Err()).NotTo(HaveOccurred())
			Expect(functionLoad.Val()).To(Equal(lib1.Name))

			functionList := client.FunctionList(ctx, q)
			Expect(functionList.Err()).NotTo(HaveOccurred())
			Expect(functionList.Val()).To(HaveLen(1))
		})

		It("Loads and replaces a new library", Label("NonRedisEnterprise"), func() {
			// Load a library for the first time
			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())

			newFuncName := "replaces_func_name"
			newFuncDesc := "replaces_func_desc"
			flag1, flag2 := "allow-stale", "no-cluster"
			newCode := fmt.Sprintf(lib1.Code, lib1.Name, newFuncName, newFuncDesc, flag1, flag2)

			// And then replace it
			functionLoadReplace := client.FunctionLoadReplace(ctx, newCode)
			Expect(functionLoadReplace.Err()).NotTo(HaveOccurred())
			Expect(functionLoadReplace.Val()).To(Equal(lib1.Name))

			lib, err := client.FunctionList(ctx, q).First()
			Expect(err).NotTo(HaveOccurred())
			Expect(lib.Functions).To(Equal([]redis.Function{
				{
					Name:        newFuncName,
					Description: newFuncDesc,
					Flags:       []string{flag1, flag2},
				},
			}))
		})

		It("Deletes a library", func() {
			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.FunctionDelete(ctx, lib1.Name).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.FunctionList(ctx, redis.FunctionListQuery{
				LibraryNamePattern: lib1.Name,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(HaveLen(0))
		})

		It("Flushes all libraries", func() {
			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.FunctionLoad(ctx, lib2Code).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.FunctionFlush(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.FunctionList(ctx, q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(HaveLen(0))
		})

		It("Flushes all libraries asynchronously", func() {
			functionLoad := client.FunctionLoad(ctx, lib1Code)
			Expect(functionLoad.Err()).NotTo(HaveOccurred())

			// we only verify the command result.
			functionFlush := client.FunctionFlushAsync(ctx)
			Expect(functionFlush.Err()).NotTo(HaveOccurred())
		})

		It("Kills a running function", func() {
			functionKill := client.FunctionKill(ctx)
			Expect(functionKill.Err()).To(MatchError("NOTBUSY No scripts in execution right now."))

			// Add test for a long-running function, once we make the test for `function stats` pass
		})

		It("Lists registered functions", Label("NonRedisEnterprise"), func() {
			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.FunctionList(ctx, redis.FunctionListQuery{
				LibraryNamePattern: "*",
				WithCode:           true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(HaveLen(1))
			Expect(val[0].Name).To(Equal(lib1.Name))
			Expect(val[0].Engine).To(Equal(lib1.Engine))
			Expect(val[0].Code).To(Equal(lib1Code))
			Expect(val[0].Functions).Should(ConsistOf(lib1.Functions))

			err = client.FunctionLoad(ctx, lib2Code).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err = client.FunctionList(ctx, redis.FunctionListQuery{
				WithCode: true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(HaveLen(2))

			lib, err := client.FunctionList(ctx, redis.FunctionListQuery{
				LibraryNamePattern: lib2.Name,
				WithCode:           false,
			}).First()
			Expect(err).NotTo(HaveOccurred())
			Expect(lib.Name).To(Equal(lib2.Name))
			Expect(lib.Code).To(Equal(""))

			_, err = client.FunctionList(ctx, redis.FunctionListQuery{
				LibraryNamePattern: "non_lib",
				WithCode:           true,
			}).First()
			Expect(err).To(Equal(redis.Nil))
		})

		It("Dump and restores all libraries", Label("NonRedisEnterprise"), func() {
			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.FunctionLoad(ctx, lib2Code).Err()
			Expect(err).NotTo(HaveOccurred())

			dump, err := client.FunctionDump(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(dump).NotTo(BeEmpty())

			err = client.FunctionRestore(ctx, dump).Err()
			Expect(err).To(HaveOccurred())

			err = client.FunctionFlush(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			list, err := client.FunctionList(ctx, q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(list).To(HaveLen(0))

			err = client.FunctionRestore(ctx, dump).Err()
			Expect(err).NotTo(HaveOccurred())

			list, err = client.FunctionList(ctx, q).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(list).To(HaveLen(2))
		})

		It("Calls a function", func() {
			lib1Code = fmt.Sprintf(lib1.Code, lib1.Name, lib1.Functions[0].Name,
				lib1.Functions[0].Description, lib1.Functions[0].Flags[0], lib1.Functions[0].Flags[1])

			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())

			x := client.FCall(ctx, lib1.Functions[0].Name, []string{"my_hash"}, "a", 1, "b", 2)
			Expect(x.Err()).NotTo(HaveOccurred())
			Expect(x.Int()).To(Equal(3))
		})

		It("Calls a function as read-only", func() {
			lib1Code = fmt.Sprintf(lib1.Code, lib1.Name, lib1.Functions[0].Name,
				lib1.Functions[0].Description, lib1.Functions[0].Flags[0], lib1.Functions[0].Flags[1])

			err := client.FunctionLoad(ctx, lib1Code).Err()
			Expect(err).NotTo(HaveOccurred())

			// This function doesn't have a "no-writes" flag
			x := client.FCallRo(ctx, lib1.Functions[0].Name, []string{"my_hash"}, "a", 1, "b", 2)

			Expect(x.Err()).To(HaveOccurred())

			lib2Code = fmt.Sprintf(lib2.Code, lib2.Name, lib2.Functions[0].Name, lib2.Functions[1].Name,
				lib2.Functions[1].Description, lib2.Functions[1].Flags[0])

			// This function has a "no-writes" flag
			err = client.FunctionLoad(ctx, lib2Code).Err()
			Expect(err).NotTo(HaveOccurred())

			x = client.FCallRo(ctx, lib2.Functions[1].Name, []string{})

			Expect(x.Err()).NotTo(HaveOccurred())
			Expect(x.Text()).To(Equal("Function 2"))
		})

		It("Shows function stats", func() {
			defer client.FunctionKill(ctx)

			// We can not run blocking commands in Redis functions, so we're using an infinite loop,
			// but we're killing the function after calling FUNCTION STATS
			lib := redis.Library{
				Name:   "mylib1",
				Engine: "LUA",
				Functions: []redis.Function{
					{
						Name:        "lib1_func1",
						Description: "This is the func-1 of lib 1",
						Flags:       []string{"no-writes"},
					},
				},
				Code: `#!lua name=%s
					local function f1(keys, args)
						local i = 0
					   	while true do
							i = i + 1
					   	end
					end

					redis.register_function{
						function_name='%s',
						description ='%s',
						callback=f1,
						flags={'%s'}
					}`,
			}
			libCode := fmt.Sprintf(lib.Code, lib.Name, lib.Functions[0].Name,
				lib.Functions[0].Description, lib.Functions[0].Flags[0])
			err := client.FunctionLoad(ctx, libCode).Err()

			Expect(err).NotTo(HaveOccurred())

			r, err := client.FunctionStats(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(r.Engines)).To(Equal(1))
			Expect(r.Running()).To(BeFalse())

			started := make(chan bool)
			go func() {
				defer GinkgoRecover()

				client2 := redis.NewClient(redisOptions())

				started <- true
				_, err = client2.FCall(ctx, lib.Functions[0].Name, nil).Result()
				Expect(err).To(HaveOccurred())
			}()

			<-started
			time.Sleep(1 * time.Second)
			r, err = client.FunctionStats(ctx).Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(len(r.Engines)).To(Equal(1))
			rs, isRunning := r.RunningScript()
			Expect(isRunning).To(BeTrue())
			Expect(rs.Name).To(Equal(lib.Functions[0].Name))
			Expect(rs.Duration > 0).To(BeTrue())

			close(started)
		})
	})

	Describe("SlowLogGet", func() {
		It("returns slow query result", func() {
			const key = "slowlog-log-slower-than"

			old := client.ConfigGet(ctx, key).Val()
			client.ConfigSet(ctx, key, "0")
			defer client.ConfigSet(ctx, key, old[key])

			err := client.Do(ctx, "slowlog", "reset").Err()
			Expect(err).NotTo(HaveOccurred())

			client.Set(ctx, "test", "true", 0)

			result, err := client.SlowLogGet(ctx, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).NotTo(BeZero())
		})
	})
})

type numberStruct struct {
	Number int
}

func (s *numberStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *numberStruct) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, s)
}

func deref(viface interface{}) interface{} {
	v := reflect.ValueOf(viface)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.Interface()
}
