package com.dylabo.batchsave.domain.taxitrip.repository;

import com.dylabo.batchsave.domain.taxitrip.entity.FHVTripRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FHVTripRecordRepository extends JpaRepository<FHVTripRecord, Long> {
}
