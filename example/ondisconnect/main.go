package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

func main() {
	cancelCh := make(chan struct{})

	client := redis.NewClient(&redis.Options{
		Addr:         "65.108.145.58:2716",
		Username:     "defi",
		Password:     "SWdmHmf2ekus26Wx",
		DB:           0,
		OnDisconnect: redis.DefaultOnDisconnect(5*time.Second, cancelCh),
	})

	if err := client.Ping(context.Background()); err != nil {
		log.Fatalln(err)
	}

	for {
		p := client.Publish(context.Background(), "test", "hello world")
		if p.Err() != nil {
			log.Println(p.Err())
		}
		time.Sleep(10 * time.Second)
	}

}
