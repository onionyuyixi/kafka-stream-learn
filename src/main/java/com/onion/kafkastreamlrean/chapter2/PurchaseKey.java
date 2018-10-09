package com.onion.kafkastreamlrean.chapter2;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.Date;

@Getter
@Builder
@AllArgsConstructor
@ToString
public class PurchaseKey {

    private String consumerId;
    private Date date;
}
