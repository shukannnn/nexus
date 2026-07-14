package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var jobEnqueuedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nexus_jobs_enqueued_total",
	Help: "number of jobs enqueued",
}, []string{"job_type"})

var jobsCompletedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nexus_jobs_completed_total",
	Help: "number of jobs completed successfully",
}, []string{"job_type"})

var jobsFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nexus_jobs_failed_total",
	Help: "number of jobs failed",
}, []string{"job_type"})

var jobsRetriedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nexus_jobs_retried_total",
	Help: "number of jobs retried",
}, []string{"job_type"})

var codeExecutionVerdictsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nexus_code_execution_verdicts_total",
	Help: "number of code submission with their verdicts",
}, []string{"language", "verdict"})

var jobsDepthCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "nexus_queue_depth",
	Help: "number of current jobs",
}, []string{"status"})

var jobsDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "nexus_job_duration_seconds",
	Help: "histogram to see the job durations",
}, []string{"job_type"})

func RecordJobEnqueued(jobType string) {
	jobEnqueuedTotal.WithLabelValues(jobType).Inc()
}

func RecordJobCompleted(jobType string) {
	jobsCompletedTotal.WithLabelValues(jobType).Inc()
}

func RecordJobFailed(jobType string) {
	jobsFailedTotal.WithLabelValues(jobType).Inc()
}

func RecordJobRetried(jobType string) {
	jobsRetriedTotal.WithLabelValues(jobType).Inc()
}

func RecordCodeExecutionVerdict(language string, verdict string) {
	codeExecutionVerdictsTotal.WithLabelValues(language, verdict).Inc()
}

func RecordJobsDepthCount(status string, value float64) {
	jobsDepthCount.WithLabelValues(status).Set(value)
}

func RecordJobDuration(jobType string, seconds float64) {
	jobsDurationSeconds.WithLabelValues(jobType).Observe(seconds)
}