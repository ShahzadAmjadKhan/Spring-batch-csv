package io.github.lscsv.batch;

import java.time.LocalDateTime;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LscsvBatchService {

    private final JobExplorer explorer;
    private final JobLauncher launcher;
    private final Job lscsvJob;
    private final Job lsxlsJob;

    public LscsvBatchService(JobExplorer explorer,
            @Qualifier("lscsvJobLauncher") JobLauncher launcher,
            @Qualifier("lscsvJob") Job lscsvJob, @Qualifier("lsxlsJob") Job lsxlsJob) {
        this.explorer = explorer;
        this.launcher = launcher;
        this.lscsvJob = lscsvJob;
        this.lsxlsJob = lsxlsJob;
    }

    public JobInfo lanchLscsv(String file) {
        try {
            JobParameters parameters = new JobParametersBuilder()
                    .addJobParameter("file", file, String.class)
                    .addLocalDateTime("date", LocalDateTime.now())
                    .toJobParameters();
            JobExecution execution;
            if (file.endsWith(".csv")) {
                execution = launcher.run(lscsvJob, parameters);
            } else {
                execution = launcher.run(lsxlsJob, parameters);
            }
            log.info("lscsv job execution status: {}, {}, {}", file, execution.getId(), execution.getExitStatus());
            return new JobInfo(execution);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException e) {
            log.warn("lscsv job execution error: {}, {} ", file, e.getMessage(), e);
            throw new IllegalStateException("lscsv job execution failed with error: " + e.getMessage());
        }
    }

    public JobInfo findLscsvJob(Long executionId) {
        JobExecution execution = explorer.getJobExecution(executionId);
        if (execution != null) {
            log.debug("lscsv job execution status: {}, {}, {}", execution.getJobParameters().getString("file"),
                    execution.getId(), execution.getExitStatus());
            return new JobInfo(execution);
        } else {
            log.debug("lscsv job execution not found: {}", executionId);
        }
        return null;
    }

    public static record LscsvInfo(String exp_imp, String Year, String month, String ym, String Country, String Custom,
            String hs2, String hs4, String hs6, String hs9, String Q1, String Q2, String Value) {
    }

    public static record JobInfo(Long id, String status, LocalDateTime startTime, LocalDateTime endTime) {
        public JobInfo(JobExecution execution) {
            this(execution.getId(), execution.getStatus().name(), execution.getStartTime(), execution.getEndTime());
        }
    }
}
