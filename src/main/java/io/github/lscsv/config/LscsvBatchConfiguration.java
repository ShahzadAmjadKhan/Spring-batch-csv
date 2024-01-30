package io.github.lscsv.config;

import java.time.LocalDateTime;
import java.util.concurrent.Future;

import org.apache.poi.ss.usermodel.Row;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.lscsv.batch.LscsvBatchApiClient;
import io.github.lscsv.batch.LscsvBatchService.LscsvInfo;
import io.github.lscsv.config.poi.StreamingXlsxItemReader;
import io.github.lscsv.config.poi.StreamingXlsxItemReader.RowMapper;

@EnableAsync
@Configuration
public class LscsvBatchConfiguration {

    private static final String HEADERS = "exp_imp,Year,month,ym,Country,Custom,hs2,hs4,hs6,hs9,Q1,Q2,Value";
    private static final String TOKENS = "${lscsv.skip.tokens:" + HEADERS + "}";

    @Bean("lscsvJobLauncher")
    public JobLauncher lscsvJobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor("lscsv-job"));
        return launcher;
    }

    @Bean("lscsvJobTaskExecutor")
    public TaskExecutor lscsvJobTaskExecutor(@Value("${lscsv.thread.pool.size:10}") int threadPoolSize) {
        VirtualThreadTaskExecutor executor = new VirtualThreadTaskExecutor("lscsv");
        // ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // executor.setThreadNamePrefix("lscsv");
        // executor.setCorePoolSize(threadPoolSize);
        // executor.setMaxPoolSize(threadPoolSize);
        // executor.setAllowCoreThreadTimeOut(true);
        return executor;
    }

    @Bean("lscsvJob")
    public Job lscsvJob(JobRepository jobRepository, @Qualifier("lscsvJobStep") Step lscsvJobStep) {
        return new JobBuilder("lscsvJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(lscsvJobStep)
                .end()
                .build();
    }

    @Bean("lscsvJobStep")
    public Step lscsvJobStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            @Value("${lscsv.chunk.size:100}") int chunkSize,
            @Qualifier("lscsvJobReader") FlatFileItemReader<LscsvInfo> lscsvJobReader,
            @Qualifier("lscsvJobProcessor") AsyncItemProcessor<LscsvInfo, LscsvInfo> lscsvJobProcessor,
            @Qualifier("lscsvJobWriter") AsyncItemWriter<LscsvInfo> lscsvJobWriter) {
        return new StepBuilder("lscsvJobStep", jobRepository)
                .<LscsvInfo, Future<LscsvInfo>>chunk(chunkSize, transactionManager)
                .reader(lscsvJobReader)
                .processor(lscsvJobProcessor)
                .writer(lscsvJobWriter)
                .build();
    }

    @StepScope
    @Bean("lscsvJobReader")
    public FlatFileItemReader<LscsvInfo> lscsvJobReader(@Value("#{jobParameters['file']}") String file,
            @Value("${lscsv.skip.lines:1}") int linesToSkip,
            @Value(TOKENS) String[] tokens) {
        return new FlatFileItemReaderBuilder<LscsvInfo>()
                .name(file)
                .resource(new FileSystemResource(file))
                .linesToSkip(linesToSkip)
                .lineMapper(lineMapper())
                .build();
    }

    private LineMapper<LscsvInfo> lineMapper() {
        return (line, lineNumber) -> parse(line.split(","));
    }

    @Bean("lscsvJobProcessor")
    public AsyncItemProcessor<LscsvInfo, LscsvInfo> lscsvJobProcessor(LscsvBatchApiClient client,
            @Qualifier("lscsvJobTaskExecutor") TaskExecutor lscsvJobTaskExecutor) {
        AsyncItemProcessor<LscsvInfo, LscsvInfo> processor = new AsyncItemProcessor<>();
        processor.setTaskExecutor(lscsvJobTaskExecutor);
        processor.setDelegate(info -> client.processLscsv(info));
        // processor.setDelegate(info -> info);
        return processor;
    }

    @Bean("lscsvJobWriter")
    public AsyncItemWriter<LscsvInfo> lscsvJobWriter(
            @Qualifier("lscsvJobFileWriter") FlatFileItemWriter<LscsvInfo> lscsvJobFileWriter) {
        AsyncItemWriter<LscsvInfo> writer = new AsyncItemWriter<>();
        writer.setDelegate(lscsvJobFileWriter);
        return writer;
    }

    @StepScope
    @Bean("lscsvJobFileWriter")
    public FlatFileItemWriter<LscsvInfo> lscsvJobFileWriter(@Value("#{jobParameters['file']}") String file,
            @Value(TOKENS) String tokens) {
        FlatFileItemWriter<LscsvInfo> writer = new FlatFileItemWriter<>();
        writer.setLineAggregator(info -> format(info));
        writer.setResource(new FileSystemResource(file + "-" + System.currentTimeMillis()));
        writer.setHeaderCallback(w -> w.write(tokens + ",at"));
        return writer;
    }

    public String format(LscsvInfo info) {
        return info.exp_imp() + "," + info.Year() + "," + info.month() + "," + info.ym()
                + "," + info.Country() + "," + info.Custom() + "," + info.hs2() + "," + info.hs4() + "," + info.hs6()
                + "," + info.hs9() + "," + info.Q1() + "," + info.Q2() + "," + info.Value() + "," + LocalDateTime.now();
    }

    public LscsvInfo parse(String[] values) {
        int i = 0;
        return new LscsvInfo(values[i++], values[i++], values[i++], values[i++], values[i++], values[i++], values[i++],
                values[i++], values[i++], values[i++], values[i++], values[i++], values[i++]);
    }

    public LscsvInfo parse(Row row) {
        int i = 0;
        return new LscsvInfo(row.getCell(i++).getStringCellValue(), row.getCell(i++).getStringCellValue(),
                row.getCell(i++).getStringCellValue(), row.getCell(i++).getStringCellValue(),
                row.getCell(i++).getStringCellValue(), row.getCell(i++).getStringCellValue(),
                row.getCell(i++).getStringCellValue(), row.getCell(i++).getStringCellValue(),
                row.getCell(i++).getStringCellValue(), row.getCell(i++).getStringCellValue(),
                row.getCell(i++).getStringCellValue(), row.getCell(i++).getStringCellValue(),
                row.getCell(i++).getStringCellValue());
    }

    @Bean("lsxlsJob")
    public Job lsxlsJob(JobRepository jobRepository, @Qualifier("lsxlsJobStep") Step lsxlsJobStep) {
        return new JobBuilder("lsxlsJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(lsxlsJobStep)
                .end()
                .build();
    }

    @Bean("lsxlsJobStep")
    public Step lsxlsJobStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            @Value("${lscsv.chunk.size:100}") int chunkSize,
            @Qualifier("lsxlsJobReader") StreamingXlsxItemReader<LscsvInfo> lsxlsJobReader,
            @Qualifier("lscsvJobProcessor") AsyncItemProcessor<LscsvInfo, LscsvInfo> lscsvJobProcessor,
            @Qualifier("lscsvJobWriter") AsyncItemWriter<LscsvInfo> lscsvJobWriter) {
        return new StepBuilder("lscsvJobStep", jobRepository)
                .<LscsvInfo, Future<LscsvInfo>>chunk(chunkSize, transactionManager)
                .reader(lsxlsJobReader)
                .processor(lscsvJobProcessor)
                .writer(lscsvJobWriter)
                .build();
    }

    @StepScope
    @Bean("lsxlsJobReader")
    public StreamingXlsxItemReader<LscsvInfo> lsxlsJobReader(@Value("#{jobParameters['file']}") String file,
            @Value("${lscsv.skip.lines:1}") int linesToSkip,
            @Value(TOKENS) String[] tokens) {
        StreamingXlsxItemReader<LscsvInfo> reader = new StreamingXlsxItemReader<>();
        reader.setName(file);
        reader.setResource(new FileSystemResource(file));
        reader.setLinesToSkip(linesToSkip);
        reader.setRowMapper(rowMapper());
        return reader;
    }

    private RowMapper<LscsvInfo> rowMapper() {
        return row -> parse(row);
    }
}
