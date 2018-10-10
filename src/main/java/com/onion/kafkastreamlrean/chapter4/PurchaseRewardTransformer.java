package com.onion.kafkastreamlrean.chapter4;

import com.onion.kafkastreamlrean.model.Purchase;
import com.onion.kafkastreamlrean.model.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

/**
 * 将Purchase  转成 RewardAccumulator
 */
public class PurchaseRewardTransformer implements ValueTransformer<Purchase,RewardAccumulator> {

    private KeyValueStore<String,Integer> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public PurchaseRewardTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store name  not  null");
        this.storeName = storeName;
    }

    //初始化  通过stateName  获取 state的存储数据 stateStore
    @Override
    public void init(ProcessorContext processorContext) {
        this.context=processorContext;
        stateStore = (KeyValueStore<String, Integer>) this.context.getStateStore(storeName);
    }

    //转变数据形式
    @Override
    public RewardAccumulator transform(Purchase purchase) {
        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(purchase).build();
        //从state store中获取数据 再修改 然后再放回state store
        Integer accumulatedSoFar = stateStore.get(rewardAccumulator.getCustomerId());
        if(accumulatedSoFar!=null){
            rewardAccumulator.addRewardPoints(accumulatedSoFar);//修改总额
        }
        stateStore.put(rewardAccumulator.getCustomerId(),rewardAccumulator.getTotalRewardPoints());
        return rewardAccumulator;
    }

    @Override
    public RewardAccumulator punctuate(long l) {
        return null;
    }

    @Override
    public void close() {

    }
}
