package com.springbatch.demo.config;

import com.springbatch.demo.step.JobCompletionListener;
import com.springbatch.demo.step.Processor;
import com.springbatch.demo.step.Reader;
import com.springbatch.demo.step.Writer;
import com.springbatch.demo.step2.SampleStep2Reader;
import com.springbatch.demo.step2.SampleStep2Writer;
import com.springbatch.demo.step3.SampleStep3Reader;
import com.springbatch.demo.step3.SampleStep3Writer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@SuppressWarnings("unchecked")
@Configuration
public class BatchConfig {
    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public BatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job processJob(JobRepository jobRepository, Step sampleStep1, Step sampleStep2, Step sampleStep3){
        return this.jobBuilderFactory.get("sampleStep1")
                //JobRepository用于定期存储StepExecution和ExecutionContext(仅在事务提交之前)
                .repository(jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener())
                .start(sampleStep1)
                .next(sampleStep2)
                .next(sampleStep3)
                .build();
    }

    @Bean
    public Step sampleStep1(PlatformTransactionManager transactionManager){
        SimpleStepBuilder stepBuilder = this.stepBuilderFactory.get("sampleStep1")
                //在整个加工过程当中启动和提交事务
                .transactionManager(transactionManager)
                //每个事务处理10个条目，每次调用read方法时，计数器会增加，当增加至10个条目时，条目集合将传递给ItemWriter，并且将事务提交。
                //TODO 为什么在reader后就会提交事务？难道不应该是read和write都做完了再提交事务吗？
                //TODO 为什么当所有的条目全都处理完毕后，reader才会将条目集合交给writer?难道不应该是每次read了1条条目就应当交给writer处理吗？
                .<String, String> chunk(10)
                //提供需要加工的Items
                .reader(new Reader()).processor(new Processor())
                //对reader提供的Items进行加工
                .writer(new Writer());

        return stepBuilder.build();
    }

    @Bean
    public Step sampleStep2(PlatformTransactionManager transactionManager){
        SimpleStepBuilder stepBuilder = this.stepBuilderFactory.get("sampleStep1")
                .<String, String>chunk(10)
                .reader(new SampleStep2Reader())
                .writer(new SampleStep2Writer());

        //本步骤只能执行一次，试图再次运行将导致StartLimitExceededException异常抛出  默认值为Integer.MAX_VALUE
        stepBuilder.startLimit(1);
        return stepBuilder.build();
    }

    @Bean
    public Step sampleStep3(PlatformTransactionManager transactionManager){
        SimpleStepBuilder stepBuilder = this.stepBuilderFactory.get("sampleStep3")
                .<String, String>chunk(10)
                .reader(new SampleStep3Reader())
                .writer(new SampleStep3Writer());

        //设置已完成的step能否再次执行。  在同一个Job中，有可能希望同一个step被执行多次，此时就需要用到allowStartIfComplete(true)了。
        stepBuilder.allowStartIfComplete(true);
        return stepBuilder.build();
    }



    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

}
