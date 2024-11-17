package com.dylabo.batchsave.domain.taxitrip.controller;

import com.dylabo.batchsave.domain.taxitrip.service.FHVTripRecordService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequiredArgsConstructor
@RequestMapping("/fhv-trip-record")
public class FHVTripRecordController {

    private final FHVTripRecordService fhvTripRecordService;

    @PostMapping("/upload-parquet")
    public String uploadParquetFile(@RequestParam("file") MultipartFile file) {
        return fhvTripRecordService.uploadParquetFile(file);
    }

}
