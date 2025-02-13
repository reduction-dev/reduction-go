package rxn

import (
	"fmt"
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
		Start(synth.Handler, WithAddress(":8080"))
	case "config":
		fmt.Printf("%s", synth.Config.Marshal())
	default:
		log.Fatalf("Unknown command: %s", os.Args[1])
	}
}
