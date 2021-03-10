package it.luca.lgd;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
//public class DRLGDApplication implements CommandLineRunner {
public class DRLGDApplication {

    public static void main(String[] args) {
        SpringApplication.run(DRLGDApplication.class, args);
    }

    /*
    @Override
    public void run(String... args) throws Exception {

        DataSource dataSource = DataSourceBuilder
                .create()
                .build();

        log.info("Created dataSource");
        Jdbi jdbi = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .installPlugin(new PostgresPlugin());

        RequestRecord requestRecord = jdbi.withExtension(RequestJDBIDao.class, dao -> dao.findById(1));
        log.info("ts_insert: {}", requestRecord.getTsInsert());
    }

     */
}
