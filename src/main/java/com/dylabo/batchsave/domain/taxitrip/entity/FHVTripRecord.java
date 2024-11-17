package com.dylabo.batchsave.domain.taxitrip.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.*;

import java.time.LocalDateTime;

@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@Entity
public class FHVTripRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String hvfhsLicenseNum;
    private String dispatchingBaseNum;
    private String originatingBaseNum;

    private LocalDateTime requestDatetime;
    private LocalDateTime onSceneDatetime;
    private LocalDateTime pickupDatetime;
    private LocalDateTime dropoffDatetime;

    private Long puLocationID;
    private Long doLocationID;

    private Double tripMiles;
    private Long tripTime;

    private Double basePassengerFare;
    private Double tolls;
    private Double bcf;
    private Double salesTax;
    private Double congestionSurcharge;
    private Double airportFee;
    private Double tips;
    private Double driverPay;

    private String sharedRequestFlag;
    private String sharedMatchFlag;
    private String accessARideFlag;
    private String wavRequestFlag;
    private String wavMatchFlag;

}
