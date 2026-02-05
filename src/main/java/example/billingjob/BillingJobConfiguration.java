package example.billingjob;

import java.io.File;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.support.JdbcTransactionManager;

@Configuration
public class BillingJobConfiguration {

	//FIRST VERSION OF JOB
//    @Bean
//    public Job job(JobRepository jobRepository) {
//        return new BillingJob(jobRepository);
//    }
	
	@Bean
	public Job job(JobRepository jobRepository, Step step1, Step step2, Step step3) {
	    return new JobBuilder("BillingJob", jobRepository)
//	    		.validator(parametersValidator())
	            .start(step1)
	            .next(step2)
	            .next(step3)
	            .build();
	}
    
    @Bean
    public Step step1(JobRepository jobRepository, JdbcTransactionManager transactionManager) {
        return new StepBuilder("filePreparation", jobRepository)
                .tasklet(new FilePreparationTasklet(), transactionManager)
                .build();
    }
    
    @Bean
    public Step step2(
       JobRepository jobRepository, JdbcTransactionManager transactionManager,
       ItemReader<BillingData> billingDataFileReader, ItemWriter<BillingData> billingDataTableWriter) {
        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(100, transactionManager)
                .reader(billingDataFileReader)
                .writer(billingDataTableWriter)
                .build();
    }
    
    @Bean
    public Step step3(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                               ItemReader<BillingData> billingDataTableReader,
                               ItemProcessor<BillingData, ReportingData> billingDataProcessor,
                               ItemWriter<ReportingData> billingDataFileWriter) {
        return new StepBuilder("reportGeneration", jobRepository)
                .<BillingData, ReportingData>chunk(100, transactionManager)
                .reader(billingDataTableReader)
                .processor(billingDataProcessor)
                .writer(billingDataFileWriter)
                .build();
    }
    
    @Bean
    public JobParametersValidator parametersValidator() {
        JobParametersValidator j = new JobParametersValidator() {
            
            @Override
            public void validate(JobParameters jparams) throws JobParametersInvalidException {
                Map<String, JobParameter<?>> params = jparams.getParameters();
                if (params==null) {
                	throw new JobParametersInvalidException("parameters empty");
                }
                if
//                (null == params.get("PRODUCT_NAME").getValue() || null == params.get("PRODUCT_ID").getValue()
//                        || null == params.get("PRODUCT_TYPE").getValue())
                (
                		params.get("input.file")    == null            	
                		|| params.get("input.file").getValue()    == null            	
                ) 
                {
                    throw new JobParametersInvalidException("input.file parameter not given");
                }
                if
//              (null == params.get("PRODUCT_NAME").getValue() || null == params.get("PRODUCT_ID").getValue()
//                      || null == params.get("PRODUCT_TYPE").getValue())
	              (
	              	
	              	params.get("input.file").getValue().toString().trim().length()==0
	              ) 
	              {
	                  throw new JobParametersInvalidException("input.file parameter not given");
	              }
                
                if (!new File(params.get("input.file").getValue().toString()).exists())
                {
                    throw new JobParametersInvalidException("File within input.file parameter not exists");
                }
            }
        };
        return j;       
    }
    
//    @Bean
//    public FlatFileItemReader<BillingData> billingDataFileReader() {
//        return new FlatFileItemReaderBuilder<BillingData>()
//                .name("billingDataFileReader")
//                .resource(new FileSystemResource("staging/billing-2023-01.csv"))
//                .delimited()
//                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
//                .targetType(BillingData.class)
//                .build();
//    }
    
    @Bean
    @StepScope
    public FlatFileItemReader<BillingData> billingDataFileReader(@Value("#{jobParameters['input.file']}") String inputFile) {
       return new FlatFileItemReaderBuilder<BillingData>()
             .name("billingDataFileReader")
             .resource(new FileSystemResource(inputFile))
             .delimited()
             .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
             .targetType(BillingData.class)
             .build();
    }

    
    @Bean
    public JdbcBatchItemWriter<BillingData> billingDataTableWriter(DataSource dataSource) {
        String sql = "insert into BILLING_DATA values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(sql)
                .beanMapped()
                .build();
    }
    
//    @Bean
//    public JdbcCursorItemReader<BillingData> billingDataTableReader(DataSource dataSource) {
//        String sql = "select * from BILLING_DATA";
//        return new JdbcCursorItemReaderBuilder<BillingData>()
//                .name("billingDataTableReader")
//                .dataSource(dataSource)
//                .sql(sql)
//                .rowMapper(new DataClassRowMapper<>(BillingData.class))
//                .build();
//    }
    
    @Bean
    @StepScope
    public JdbcCursorItemReader<BillingData> billingDataTableReader(
       DataSource dataSource,
       @Value("#{jobParameters['data.year']}") Integer year,
       @Value("#{jobParameters['data.month']}") Integer month) {
        String sql = String.format("select * from BILLING_DATA where DATA_YEAR = %d and DATA_MONTH = %d", year, month);
        return new JdbcCursorItemReaderBuilder<BillingData>()
                .name("billingDataTableReader")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                .build();
    }
    
    @Bean
    public BillingDataProcessor billingDataProcessor() {
        return new BillingDataProcessor();
    }
//    @Bean
//    public FlatFileItemWriter<ReportingData> billingDataFileWriter() {
//            return new FlatFileItemWriterBuilder<ReportingData>()
//                .resource(new FileSystemResource("staging/billing-report-2023-01.csv"))
//                .name("billingDataFileWriter")
//                .delimited()
//                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
//                .build();
//    }
    @Bean
    @StepScope
    public FlatFileItemWriter<ReportingData> billingDataFileWriter(
       @Value("#{jobParameters['output.file']}") String outputFile) {
        return new FlatFileItemWriterBuilder<ReportingData>()
                .resource(new FileSystemResource(outputFile))
                .name("billingDataFileWriter")
                .delimited()
                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
                .build();
    }
    
    
}