package rxn

import (
	"fmt"
	"log"
	"net"
	"os"

	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxnsvr"
)

// Run provides a CLI for the provided job configuration and handles either a
// "start" or "config" command. Config prints the job config to stdout. Start
// runs the job on ":8080".
func Run(config *jobs.Job) {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <command>", os.Args[0])
	}

	synth, err := config.Synthesize()
	if err != nil {
		log.Fatalf("invalid job configuration: %v", err)
	}

	switch os.Args[1] {
	case "start":
		listener, err := net.Listen("tcp", ":8080")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		svr := rxnsvr.New(synth.Handler, rxnsvr.WithListener(listener))
		if err := svr.Start(); err != nil {
			log.Fatalf("server stopped with error: %v", err)
		}
	case "config":
		fmt.Printf("%s", synth.Config.Marshal())
	default:
		log.Fatalf("Unknown command: %s", os.Args[1])
	}
}
