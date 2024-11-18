package com.dylabo.batchsave.domain.taxitrip.service;

import org.springframework.web.multipart.MultipartFile;

public interface FHVTripRecordService {

    String uploadParquetFile(MultipartFile file, int rowIndex);

}
