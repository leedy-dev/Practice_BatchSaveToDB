package com.dylabo.batchsave.domain.taxitrip.service.dto;

import lombok.Getter;

@Getter
public class FHVTripRecordDto {

    private String vendorId;

    private String pickupDatetime;

    private String dropoffDatetime;

    private Integer passengerCount;

    private Double tripDistance;

    private Double pickupLongitude;

    private Double pickupLatitude;

    private Double dropoffLongitude;

    private Double dropoffLatitude;

    private Double fareAmount;

    private Double tipAmount;

    private Double totalAmount;

    @Getter
    public static class Request extends FHVTripRecordDto {

    }

    @Getter
    public static class Response extends FHVTripRecordDto {
        private Long id;
    }

}
