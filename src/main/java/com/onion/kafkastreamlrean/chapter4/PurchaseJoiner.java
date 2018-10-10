package com.onion.kafkastreamlrean.chapter4;

import com.onion.kafkastreamlrean.model.CorrelatedPurchase;
import com.onion.kafkastreamlrean.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *合并之的接口实现 自定义逻辑 返回想要的合并结果
 */
public class PurchaseJoiner implements ValueJoiner<Purchase,Purchase,CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {

        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();
        
        Date purchaseDate = purchase != null ? purchase.getPurchaseDate() : null;
        Double price = purchase != null ? purchase.getPrice() : 0.0;
        String itemPurchased = purchase != null ? purchase.getItemPurchased() : null;

        Date otherPurchaseDate = otherPurchase != null ? otherPurchase.getPurchaseDate() : null;
        Double otherPrice = otherPurchase != null ? otherPurchase.getPrice() : 0.0;
        String otherItemPurchased = otherPurchase != null ? otherPurchase.getItemPurchased() : null;

        List<String> purchasedItems = new ArrayList<>();

        if(itemPurchased!=null) purchasedItems.add(itemPurchased);
        if(otherItemPurchased!=null) purchasedItems.add(otherItemPurchased);

        String customerId = purchase != null ? purchase.getCustomerId() : null;
        String otherCustomerId = otherPurchase != null ? otherPurchase.getCustomerId() : null;

        builder.withCustomerId(customerId!=null?customerId:otherCustomerId)
                .withFirstPurchaseDate(purchaseDate)
                .withSecondPurchaseDate(otherPurchaseDate)
                .withTotalAmount(price+otherPrice);





        return builder.build();


    }
}
