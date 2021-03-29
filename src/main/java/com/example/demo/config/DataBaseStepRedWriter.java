package com.example.demo.config;

import com.example.demo.entity.Customer;
import com.example.demo.mapper.CustomerMapping;
import com.example.demo.writer.SavingItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@EnableAsync
@EnableScheduling
public class DataBaseStepRedWriter {

    @Autowired
    DataSource dataSource;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    List<Flow> lisFlows;

    //@Autowired
    //TaskExecutor threadPoolTaskExecutor;

    private int poolSize;


   // @Bean
    // @Scope(value="step", proxyMode = ScopedProxyMode.INTERFACES)
    public JdbcPagingItemReader<Customer> jdbcPagingItemReader() {
        JdbcPagingItemReader<Customer> pagingItemReader = new JdbcPagingItemReader<>();

        pagingItemReader.setDataSource(dataSource);
        pagingItemReader.setFetchSize(10);
        pagingItemReader.setRowMapper(new CustomerMapping());

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, firstName, lastName, birthdate");
        queryProvider.setFromClause("from customer");

        Map<String, Order> sortKeys = new HashMap<>(1);

        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        pagingItemReader.setQueryProvider(queryProvider);

        return pagingItemReader;

    }


    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{"listJobs"});
        return listener;
    }

    //@Bean
   // @Scope(value="step", proxyMode = ScopedProxyMode.INTERFACES)
    public Step getFlowList() {
        poolSize = 10;
        return stepBuilderFactory.get("getListJobs")
                .<Customer, Flow>chunk(poolSize)
                .reader(jdbcPagingItemReader())
                .processor(processor())
                .writer(items -> new SavingItemWriter())
                .listener(promotionListener())
                .build();
    }

   // @Bean
    //@Scope(value="step", proxyMode = ScopedProxyMode.INTERFACES)
    ItemProcessor<Customer, Flow> processor() {
        return item -> {
            FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("job-" + item.getFirstName() + "-" + item.getId());
            flowBuilder.start(
                    stepBuilderFactory.get("step-" + item.getFirstName() + "-" + item.getId())
                            .tasklet((stepContribution, chunkContext) -> {
                                System.out.println("Executing: step-" + item.getFirstName() + "-" + item.getId());
                                return RepeatStatus.FINISHED;
                            }).build()
            ).end();
            return flowBuilder.build();

        };
    }


    public TaskExecutor threadPoolTaskExecutor() {

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(poolSize - 2);
        executor.setCorePoolSize(8);
        executor.setQueueCapacity(poolSize);

        return executor;
    }

    @Bean
    @StepScope
    //@Scope(value="step", proxyMode = ScopedProxyMode.INTERFACES)
    List<Flow> lisFlows(@Value("#{jobExecutionContext['listJobs']}") List<Flow> flows) {
        return flows;

    }


    public JobLauncher asyncJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();

        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(threadPoolTaskExecutor());
        return jobLauncher;
    }

    public JobLauncher jobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();

        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return jobLauncher;
    }



    Job mainJob() {
        return jobBuilderFactory.get("mainJobFactory")
               // .incrementer(new RunIdIncrementer())
                .start(getFlowList())
                .split(new SimpleAsyncTaskExecutor())
                .add(lisFlows.toArray(new Flow[poolSize]))
                .end()
                .build();

    }


    @Scheduled(cron = "*/20 * * * * *")
    public void executeJob() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("JobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();
        jobLauncher().run(mainJob(), params);
    }
}
