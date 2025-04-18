package topology

import "reduction.dev/reduction-go/internal"

type internalJob struct {
	job *Job
}

// InternalAccess is used for cross-package communication and shouldn't be considered
// part of the public API.
func InternalAccess(job *Job) internalJob {
	return internalJob{job: job}
}

func (j internalJob) RegisterSource(source internal.Source) {
	j.job.registerSource(source)
}

func (j internalJob) RegisterSink(sink internal.SinkSynthesizer) {
	j.job.registerSink(sink)
}
