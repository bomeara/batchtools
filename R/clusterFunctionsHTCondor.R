#' @title ClusterFunctions for HTCondor Systems
#'
#' @description
#' Cluster functions for HTCondor (\url{https://research.cs.wisc.edu/htcondor/}).
#'
#' Job files are created based on the brew template \code{template}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{condor_qsub} command. Jobs are killed using the \code{condor_rm} command and the
#' list of running jobs is retrieved using \code{condor_q}. The user must have
#' the appropriate privileges to submit, delete and list jobs on the cluster
#' (this is usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @note
#' Array jobs are currently not supported.
#'
#' @template template
#' @inheritParams makeClusterFunctions
#' @template nodename
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsHTCondor = function(template = "htcondor", nodename = "localhost", scheduler.latency = 1, fs.latency = 65) { # nocov start
  assertString(nodename)
  template = findTemplateFile(template)
  if (testScalarNA(template))
    stopf("Argument 'template' (=\"%s\") must point to a readable template file", template)
  template = cfReadBrewTemplate(template)
  quote = if (isLocalHost(nodename)) identity else shQuote

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("condor_qsub", shQuote(outfile), nodename = nodename)

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("condor_qsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_flatten(res$output, " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, args) {
    assertRegistry(reg, writeable = FALSE)
    res = runOSCommand("condor_status", args, nodename = nodename)
    if (res$exit.code > 0L)
      OSError("Listing of jobs failed", res)
    stri_extract_first_regex(tail(res$output, -2L), "\\d+")
  }

  listJobsQueued = function(reg) {
    listJobs(reg, c("-u $USER", "-s p"))
  }

  listJobsRunning = function(reg) {
    listJobs(reg, c("-u $USER", "-s rs"))
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "condor_rm", batch.id, nodename = nodename)
  }

  makeClusterFunctions(name = "htcondor", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, store.job.collection = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
