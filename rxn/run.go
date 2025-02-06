package rxn

import (
	"log"
	"os"

	"reduction.dev/reduction-go/jobs"
)

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
		Start(synth, WithAddress(":8080"))
	case "config":
		log.Printf("%s", config.Marshal())
	default:
		log.Fatalf("Unknown command: %s", os.Args[1])
	}
}
