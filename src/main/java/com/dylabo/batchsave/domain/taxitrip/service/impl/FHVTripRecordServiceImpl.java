package com.dylabo.batchsave.domain.taxitrip.service.impl;

import com.dylabo.batchsave.common.file.LocalInputFile;
import com.dylabo.batchsave.domain.taxitrip.entity.FHVTripRecord;
import com.dylabo.batchsave.domain.taxitrip.repository.FHVTripRecordRepository;
import com.dylabo.batchsave.domain.taxitrip.service.FHVTripRecordService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class FHVTripRecordServiceImpl implements FHVTripRecordService {

    private static final int THREAD_COUNT = 6; // 병렬 처리할 스레드 수
    private static final int BATCH_SIZE = 5000; // 배치 크기

    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true); // 데몬 스레드 설정
        return thread;
    });

    private final FHVTripRecordRepository fhvTripRecordRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final TransactionTemplate transactionTemplate;

    @Override
    public String uploadParquetFile(MultipartFile file, int rowIndex) {
        File tempFile = null;
        try {
            // 1. 업로드된 파일을 임시 디렉토리에 저장
            File tempDir = new File("D:/files/temp");
            if (!tempDir.exists()) {
                tempDir.mkdirs(); // 디렉토리가 없으면 생성
            }
            tempFile = File.createTempFile("parquet_upload_", ".parquet", tempDir);
            file.transferTo(tempFile);
            log.info("임시 파일 생성됨: {}", tempFile.getAbsolutePath());

            // 2. 멀티스레드로 PARQUpip install pyarrowET 파일 처리
            if (rowIndex == 0) {
                processParquetFile(tempFile);
            } else {
                processParquetFile(tempFile, rowIndex);
            }

            return "Parquet 파일 처리가 완료되었습니다!";
        } catch (IOException ioe) {
            return "파일 업로드 또는 처리 중 오류 발생: " + ioe.getMessage();
        } finally {
            // 3. 파일 삭제
            if (tempFile != null) {
                try {
                    Files.delete(tempFile.toPath());
                    log.info("임시 파일 삭제");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Transactional
    public void processParquetFile(File file) {
        String filePath = file.getAbsolutePath();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("parquet.read.page.size", "1000");
        conf.set("parquet.read.support.class", GroupReadSupport.class.getName());

        Path parquetFilePath = new Path(filePath);
        AtomicInteger batchCounter = new AtomicInteger(0);

        // 배치 시작 시간
        LocalDateTime startTime = LocalDateTime.now();
        log.info("배치 시작 : {}", startTime);

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            // 파일 메타데이터 가져오기
            MessageType schema = reader.getFileMetaData().getSchema();
            List<BlockMetaData> blocks = reader.getRowGroups();

            log.info("총 블록 사이즈 : {}", blocks.size());

            for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
                // RowGroup 가져오기
                PageReadStore pageReadStore = reader.readNextRowGroup();

                // 더이상 RowGroup 없으면 종료
                if (pageReadStore == null) {
                    break;
                }

                long rowCount = pageReadStore.getRowCount();

                log.info("현재 블록 : {}", blockIndex);
                log.info("현재 row 수 : {}", rowCount);

                // reader 설정
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(
                        pageReadStore,
                        new GroupRecordConverter(schema)
                );

                // 엔티티 초기화
                List<FHVTripRecord> batch = new ArrayList<>(BATCH_SIZE);
                Group record;

                // 배치 시작
                while ((record = recordReader.read()) != null) {
                    batch.add(mapToEntity((SimpleGroup) record));

                    if (batch.size() >= BATCH_SIZE) {
                        // 배치 처리 작업을 병렬로 실행
                        List<FHVTripRecord> batchToProcess = new ArrayList<>(batch);
                        executorService.submit(() -> {
                            transactionTemplate.execute(status -> {
                                processBatch(batchToProcess, batchCounter.getAndIncrement());
                                return null;
                            });
                        });
                        batch.clear();
                    }
                }

                // 남은 데이터 처리
                if (!batch.isEmpty()) {
                    executorService.submit(() -> {
                        transactionTemplate.execute(status -> {
                            processBatch(batch, batchCounter.getAndIncrement());
                            return null;
                        });
                    });
                }
            }
        } catch(Exception e) {
            log.error("Parquet 파일 처리 중 오류 발생: " + e.getMessage());
        } finally {
            shutdownExecutor();

            // 배치 종료 시간
            LocalDateTime endTime = LocalDateTime.now();
            log.info("배치 종료 : {}", endTime);

            // 소요 시간 계산
            long elapsedSeconds = Duration.between(startTime, endTime).getSeconds();
            log.info("소요 시간 (초) : {} 초", elapsedSeconds);
        }

    }

    @Transactional
    public void processParquetFile(File file, int rowIndex) {
        String filePath = file.getAbsolutePath();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("parquet.read.page.size", "1000");
        conf.set("parquet.read.support.class", GroupReadSupport.class.getName());

        Path parquetFilePath = new Path(filePath);
        AtomicInteger batchCounter = new AtomicInteger(0);

        // 배치 시작 시간
        LocalDateTime startTime = LocalDateTime.now();
        log.info("배치 시작 : {}", startTime);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFilePath)
                .withConf(conf)
                .build()
        ) {
            List<FHVTripRecord> batch = new ArrayList<>(BATCH_SIZE);
            Group record;

            while ((record = reader.read()) != null) {
                if (reader.getCurrentRowIndex() < rowIndex) continue;

                batch.add(mapToEntity((SimpleGroup) record));
                if (batch.size() >= BATCH_SIZE) {
                    // 배치 처리 작업을 병렬로 실행
                    List<FHVTripRecord> batchToProcess = new ArrayList<>(batch);
                    executorService.submit(() -> {
                        transactionTemplate.execute(status -> {
                            processBatch(batchToProcess, batchCounter.getAndIncrement());
                            return null;
                        });
                    });
                    batch.clear();
                }
            }

            // 남은 데이터 처리
            if (!batch.isEmpty()) {
                List<FHVTripRecord> batchToProcess = new ArrayList<>(batch);
                executorService.submit(() -> {
                    transactionTemplate.execute(status -> {
                        processBatch(batchToProcess, batchCounter.getAndIncrement());
                        return null;
                    });
                });
            }
        } catch(Exception e) {
            log.error("Parquet 파일 처리 중 오류 발생: " + e.getMessage());
        } finally {
            shutdownExecutor();

            // 배치 종료 시간
            LocalDateTime endTime = LocalDateTime.now();
            log.info("배치 종료 : {}", endTime);

            // 소요 시간 계산
            long elapsedSeconds = Duration.between(startTime, endTime).getSeconds();
            log.info("소요 시간 (초) : {} 초", elapsedSeconds);
        }

    }

    private void processBatch(List<FHVTripRecord> batch, int batchNumber) {
        log.info("배치 {} 처리 시작 (크기: {})", batchNumber, batch.size());

        try {
            saveRecords(batch);
            batch.clear();
        } catch(Exception e) {
            log.error("배치 {} 저장 중 오류 발생", batchNumber);
            e.printStackTrace();
            throw e;
        }

        log.info("배치 {} 처리 완료", batchNumber);
    }

    private void saveRecords(List<FHVTripRecord> fhvTripRecords) {
//        fhvTripRecordRepository.saveAllAndFlush(fhvTripRecords);

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            entityManager.getTransaction().begin();
            for (int i = 0; i < fhvTripRecords.size(); i++) {
                entityManager.persist(fhvTripRecords.get(i));
                if (i % 100 == 0) {
                    entityManager.flush();
                    entityManager.clear();
                }
            }
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw e;
        } finally {
            entityManager.close();
        }
    }

    private void shutdownExecutor() {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                log.warn("모든 배치 작업이 60초 내에 완료되지 않았습니다. 강제 종료를 시도합니다.");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Executor 종료 중 오류 발생: {}", e.getMessage(), e);
            executorService.shutdownNow();
        }
    }

    private FHVTripRecord mapToEntity(SimpleGroup record) {
        return FHVTripRecord.builder()
                .hvfhsLicenseNum(getString(record, "hvfhs_license_num"))
                .dispatchingBaseNum(getString(record, "dispatching_base_num"))
                .originatingBaseNum(getString(record, "originating_base_num"))
                .requestDatetime(toLocalDateTime(getLong(record, "request_datetime")))
                .onSceneDatetime(toLocalDateTime(getLong(record, "on_scene_datetime")))
                .pickupDatetime(toLocalDateTime(getLong(record, "pickup_datetime")))
                .dropoffDatetime(toLocalDateTime(getLong(record, "dropoff_datetime")))
                .puLocationID(getLong(record, "PULocationID"))
                .doLocationID(getLong(record, "DOLocationID"))
                .tripMiles(getDouble(record, "trip_miles"))
                .tripTime(getLong(record, "trip_time"))
                .basePassengerFare(getDouble(record, "base_passenger_fare"))
                .tolls(getDouble(record, "tolls"))
                .bcf(getDouble(record, "bcf"))
                .salesTax(getDouble(record, "sales_tax"))
                .congestionSurcharge(getDouble(record, "congestion_surcharge"))
                .airportFee(getDouble(record, "airport_fee"))
                .tips(getDouble(record, "tips"))
                .driverPay(getDouble(record, "driver_pay"))
                .sharedRequestFlag(getString(record, "shared_request_flag"))
                .sharedMatchFlag(getString(record, "shared_match_flag"))
                .accessARideFlag(getString(record, "access_a_ride_flag"))
                .wavRequestFlag(getString(record, "wav_request_flag"))
                .wavMatchFlag(getString(record, "wav_match_flag"))
                .build();
    }

    private String getString(SimpleGroup record, String column) {
        try {
            return record.getString(column, 0);
        } catch (Exception e) {
            return null;
        }
    }

    private Long getLong(SimpleGroup record, String column) {
        try {
            return record.getLong(column, 0);
        } catch (Exception e) {
            return null;
        }
    }

    private Double getDouble(SimpleGroup record, String column) {
        try {
            return record.getDouble(column, 0);
        } catch (Exception e) {
            return null;
        }
    }

    private LocalDateTime toLocalDateTime(Long epochMillis) {
        if (epochMillis == null) {
            return null;
        }
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
    }

}
