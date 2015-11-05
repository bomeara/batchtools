context("killJobs")

test_that("killJobs", {
  skip_on_os("windows")
  skip_on_cran()

  reg = makeTempRegistry(FALSE)
  reg$cluster.functions = makeClusterFunctionsMulticore(ncpus = 1L, max.load = 99, max.jobs = 99)
  ids = batchMap(Sys.sleep, time = 60, reg = reg)
  submitJobs(1, reg = reg)
  expect_equal(findOnSystem(1, reg = reg), findJobs(reg = reg))

  batch.id = reg$status[1, batch.id]
  res = killJobs(1, reg = reg)
  expect_equal(res$job.id, 1L)
  expect_equal(res$batch.id, batch.id)
  expect_true(res$killed)
})
