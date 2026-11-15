// Command sluicectl is a small CLI for a sluice cluster.
//
//	sluicectl create-topic -topic orders -partitions 3 -rf 3
//	sluicectl topics
//	echo hello | sluicectl produce -topic orders
//	sluicectl consume -topic orders -group cli -from-earliest
//	sluicectl share-consume -topic orders -group workers
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/protocol"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	bootstrap := fs.String("bootstrap", envOr("SLUICE_BOOTSTRAP", "localhost:9092"), "comma-separated broker addresses")
	topic := fs.String("topic", "", "topic name")
	partitions := fs.Int("partitions", 1, "create-topic: partitions")
	rf := fs.Int("rf", 1, "create-topic: replication factor")
	group := fs.String("group", "sluicectl", "consumer or share group")
	key := fs.String("key", "", "produce: record key")
	fromEarliest := fs.Bool("from-earliest", false, "consume: start at the log start when there is no committed offset")
	n := fs.Int("n", 0, "consume: stop after n records (0: run until interrupted)")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	cl, err := client.New(ctx, client.Config{Bootstrap: strings.Split(*bootstrap, ","), ClientID: "sluicectl"})
	check(err)
	defer cl.Close()

	switch cmd {
	case "create-topic":
		need(*topic)
		check(cl.CreateTopic(ctx, *topic, *partitions, *rf))
		fmt.Printf("created %s (%d partitions, rf %d)\n", *topic, *partitions, *rf)
	case "topics":
		md, err := cl.Metadata(ctx)
		check(err)
		for _, b := range md.Brokers {
			fmt.Printf("broker %d %s:%d\n", b.NodeID, b.Host, b.Port)
		}
		for _, t := range md.Topics {
			for _, p := range t.Partitions {
				fmt.Printf("%s-%d leader=%d replicas=%v isr=%v\n", t.Name, p.ID, p.Leader, p.Replicas, p.ISR)
			}
		}
	case "produce":
		need(*topic)
		p, err := cl.NewProducer(ctx, client.ProducerConfig{})
		check(err)
		sc := bufio.NewScanner(os.Stdin)
		var k []byte
		if *key != "" {
			k = []byte(*key)
		}
		for sc.Scan() {
			off, err := p.ProduceSync(ctx, client.Message{Topic: *topic, Key: k, Value: []byte(sc.Text())})
			check(err)
			fmt.Fprintf(os.Stderr, "offset %d\n", off)
		}
		p.Close()
	case "consume":
		need(*topic)
		c, err := cl.NewConsumer(ctx, client.ConsumerConfig{Group: *group, Topics: []string{*topic}, FromEarliest: *fromEarliest, AutoCommit: true})
		check(err)
		defer c.Close(context.Background())
		count := 0
		for ctx.Err() == nil && (*n == 0 || count < *n) {
			recs, err := c.Poll(ctx)
			if ctx.Err() != nil {
				break
			}
			check(err)
			for _, r := range recs {
				fmt.Printf("%s-%d@%d %s %s\n", r.Topic, r.Partition, r.Offset, r.Key, r.Value)
				count++
			}
		}
	case "share-consume":
		need(*topic)
		s, err := cl.NewShareConsumer(ctx, client.ShareConsumerConfig{Group: *group, Topic: *topic, FromEarliest: *fromEarliest})
		check(err)
		defer s.Close(context.Background())
		count := 0
		for ctx.Err() == nil && (*n == 0 || count < *n) {
			recs, err := s.Poll(ctx)
			if ctx.Err() != nil {
				break
			}
			check(err)
			for _, r := range recs {
				fmt.Printf("%s-%d@%d delivery=%d %s\n", r.Topic, r.Partition, r.Offset, r.DeliveryCount, r.Value)
				s.Ack(r, protocol.AckAccept)
				count++
			}
		}
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		check(s.Commit(ctx2))
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: sluicectl create-topic|topics|produce|consume|share-consume [flags]")
	os.Exit(2)
}

func need(topic string) {
	if topic == "" {
		fmt.Fprintln(os.Stderr, "sluicectl: -topic is required")
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "sluicectl:", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
