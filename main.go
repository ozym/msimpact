package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
	"github.com/ozym/impact"
	"github.com/ozym/mseed"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	var Q *sqs.Queue

	// runtime settings
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "make noise")
	var dryrun bool
	flag.BoolVar(&dryrun, "dry-run", false, "don't actually send the messages")
	var replay bool
	flag.BoolVar(&replay, "replay", false, "send current time rather than recorded time")

	// streaming channel information
	var config string
	flag.StringVar(&config, "config", "impact.json", "provide a streams config file")

	// amazon queue details
	var region string
	flag.StringVar(&region, "region", "", "provide AWS region")
	var queue string
	flag.StringVar(&queue, "queue", "", "send messages to the SQS queue")
	var key string
	flag.StringVar(&key, "key", "", "AWS access key id, overrides env and credentials file (default profile)")
	var secret string
	flag.StringVar(&secret, "secret", "", "AWS secret key id, overrides env and credentials file (default profile)")

	// noisy channel detection
	var probation time.Duration
	flag.DurationVar(&probation, "probation", 10.0*time.Minute, "noise probation window")
	var level int
	flag.IntVar(&level, "level", 2, "noise threshold level")

	flag.Parse()
	if region == "" {
		region = os.Getenv("AWS_IMPACT_REGION")
		if region == "" {
			log.Fatalf("unable to find region in environment or command line [AWS_IMPACT_REGION]")
		}
	}

	if queue == "" {
		queue = os.Getenv("AWS_IMPACT_QUEUE")
		if queue == "" {
			log.Fatalf("unable to find queue in environment or command line [AWS_IMPACT_QUEUE]")
		}
	}

	// configure amazon ...
	if !dryrun {
		R := aws.GetRegion(region)
		// fall through to env then credentials file
		A, err := aws.GetAuth(key, secret, "", time.Now().Add(30*time.Minute))
		if err != nil {
			log.Fatal(err)
		}

		S := sqs.New(A, R)
		Q, err = S.GetQueue(queue)
		if err != nil {
			log.Fatal(err)
		}
	}

	// load stream configuration
	state := impact.LoadStreams(config)

	// initial stream setup
	for s := range state {
		_, err := state[s].Init(s, probation, (int32)(level))
		if err != nil {
			log.Fatal(err)
		}
	}

	// make space for miniseed blocks
	msr := mseed.NewMSRecord()
	defer mseed.FreeMSRecord(msr)

	// fixup stream code for messaging
	replace := strings.NewReplacer("_", ".")

	// output channel
	result := make(chan impact.Message)
	go func() {
		for m := range result {
			mm, err := json.Marshal(m)
			if err != nil {
				log.Panic(err)
			}
			if verbose {
				fmt.Println(string(mm))
			}
			if !dryrun {
				_, err := Q.SendMessage(string(mm))
				if err != nil {
					log.Panic(err)
				}
			}
		}
	}()

	missing := make(map[string]string)

	blk := make([]byte, 512)
	for i := range flag.Args() {
		if verbose {
			fmt.Printf("processing miniseed file: \"%s\"\n", flag.Args()[i])
		}

		file, err := os.Open(flag.Args()[i])
		if err != nil {
			log.Fatal(err)
		}

		in := bufio.NewReader(file)
		for {
			n, err := in.Read(blk)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if n == 0 {
				break
			}

			// decode mseed block
			msr.Unpack(blk, n, 1, 0)

			// what to send
			source := strings.TrimRight(msr.Network()+"."+msr.Station(), "\u0000")

			// block lookup key
			srcname := msr.SrcName(0)
			// have we rejected this before?
			if _, ok := missing[srcname]; ok {
				continue
			}
			stream, ok := state[srcname]
			if ok == false {
				log.Printf("unable to find stream config! %s\n", srcname)
				missing[srcname] = srcname
				continue
			}

			// recover amplitude samples
			samples, err := msr.DataSamples()
			if err != nil {
				log.Printf("data sample problem! %s\n", err)
				continue
			}

			// process each block into a message
			message, err := stream.ProcessSamples(replace.Replace(source), srcname, msr.Starttime(), samples)
			if err != nil {
				log.Printf("data processing problem! %s\n", err)
				continue
			}

			// should we send a message .. but only on a change in MMI (no heartbeats)
			if stream.Flush(0, message.MMI) {
				if replay {
					message.Time = time.Now().Truncate(time.Second)
				}
				result <- message
			}

		}

	}
}
