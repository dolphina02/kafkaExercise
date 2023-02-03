package com.example.kafkaExercise.service;

import com.example.kafkaExercise.vo.EffectOrNot;
import com.example.kafkaExercise.vo.PurchaseLog;
import com.example.kafkaExercise.vo.PurchaseLogOneProduct;
import com.example.kafkaExercise.vo.WatchingAdLog;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AdEvaluationService {
    // 광고 데이터 중복 Join될 필요없다. --> Table
    // 광고 이력이 먼저 들어옵니다.
    // 구매 이력은 상품별로 들어오지 않습니다. (복수개의 상품 존재) --> contain
    // 광고에 머문시간이 10초 이상되어야만 join 대상
    // 특정가격이상의 상품은 join 대상에서 제외 (100만원)
    // 광고이력 : KTable(AdLog), 구매이력 : KTable(PurchaseLogOneProduct)
    // filtering, 형 변환,
    // EffectOrNot --> Json 형태로 Topic : AdEvaluationComplete

    Producer myprdc;
    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        //object의 형태별로 Serializer, Deserializer 를 설정합니다.
        JsonSerializer<EffectOrNot> effectSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLogOneProduct> purchaseLogOneProductSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectOrNot> effectDeserializer = new JsonDeserializer<>(EffectOrNot.class);
        JsonDeserializer<PurchaseLog> purchaseLogDeserializer = new JsonDeserializer<>(PurchaseLog.class);
        JsonDeserializer<WatchingAdLog> watchingAdLogJsonDeserializer = new JsonDeserializer<>(WatchingAdLog.class);
        JsonDeserializer<PurchaseLogOneProduct> purchaseLogOneProductDeserializer = new JsonDeserializer<>(PurchaseLogOneProduct.class);

        Serde<EffectOrNot> effectOrNotSerde = Serdes.serdeFrom(effectSerializer, effectDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogSerializer, purchaseLogDeserializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogSerializer, watchingAdLogJsonDeserializer);
        Serde<PurchaseLogOneProduct> purchaseLogOneProductSerdeSerde = Serdes.serdeFrom(purchaseLogOneProductSerializer, purchaseLogOneProductDeserializer);

        // adLog topic 을 consuming 하여 KTable 로 받습니다.
        KTable<String, WatchingAdLog> adTable = sb.stream("adLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .selectKey((k,v) -> v.getUserId() + "_" + v.getProductId()) // key를 userId+prodId로 생성해줍니다.
                .filter((k,v)-> Integer.parseInt(v.getWatchingTime()) > 10) // 광고 시청시간이 10초 이상인 데이터만 Table에 담습니다.
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore") // Key-Value Store로 만들어줍니다.
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                        );

        // purchaseLog topic 을 consuming 하여 KStream 으로 받습니다.
        KStream<String, PurchaseLog> purchaseLogKStream = sb.stream("purchaseLog", Consumed.with(Serdes.String(), purchaseLogSerde));

        // 해당 KStream의 매 Row(Msg)마다 하기의 내용을 수행합니다.
        purchaseLogKStream.foreach((k,v) -> {

            // value 의 product 개수만큼 반복하여 신규 VO에 값을 Binding 합니다.
            for (Map<String, String> prodInfo:v.getProductInfo())   {
                // price 100000 미만인 경우만 하기의 내용을 수행합니다. 위 purchaseLogKStream에서 filter조건을 주고 받아도 무관합니다.
                if (Integer.parseInt(prodInfo.get("price")) < 1000000) {
                    PurchaseLogOneProduct tempVo = new PurchaseLogOneProduct();
                    tempVo.setUserId(v.getUserId());
                    tempVo.setProductId(prodInfo.get("productId"));
                    tempVo.setOrderId(v.getOrderId());
                    tempVo.setPrice(prodInfo.get("price"));
                    tempVo.setPurchasedDt(v.getPurchasedDt());

                    // 1개의 product로 나눈 데이터를 purchaseLogOneProduct Topic으로 produce 합니다.
                    myprdc.sendJoinedMsg("purchaseLogOneProduct", tempVo);

                    // 하기의 method는 samplie Data를 생산하여 Topic에 넣습니다. 1개 받으면 여러개를 생성하기 때문에 무한하게 생성됩니다. .
                    // sendNewMsg();
                }
            }
        }
        );

        // product이 1개씩 나누어 producing 된 topic을 읽어 KTable 로 받아옵니다.
        KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable= sb.stream("purchaseLogOneProduct", Consumed.with(Serdes.String(), purchaseLogOneProductSerdeSerde))
                // Stream의 key 지정
                .selectKey((k,v)-> v.getUserId()+ "_" +v.getProductId())
                // key-value Store로 이용이 가능하도록 생성
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("purchaseLogStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseLogOneProductSerdeSerde)
                );

        // value joiner 를 통해 Left, Right 값을 통한 Output 결과값을 bind 하거나 join 조건을 설정할 수 있습니다.
        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectOrNot> tableStreamJoiner = (leftValue, rightValue) -> {
            EffectOrNot returnValue = new EffectOrNot();
            returnValue.setUserId(rightValue.getUserId());
            returnValue.setAdId(leftValue.getAdId());
            returnValue.setOrderId(rightValue.getOrderId());
            Map<String, String> tempProdInfo = new HashMap<>();
            tempProdInfo.put("productId", rightValue.getProductId());
            tempProdInfo.put("price", rightValue.getPrice());
            returnValue.setProductInfo(tempProdInfo);
            System.out.println("Joined!");
            return returnValue;
        };

        // table과 joiner를 입력해줍니다. stream join이 아니기에 window 설정은 불필요합니다.
        adTable.join(purchaseLogOneProductKTable,tableStreamJoiner)
                // join 이 완료된 데이터를 AdEvaluationComplete Topic으로 전달합니다.
                .toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectOrNotSerde));
    }

    public void sendNewMsg() {
        PurchaseLog tempPurchaseLog  = new PurchaseLog();
        WatchingAdLog tempWatchingAdLog = new WatchingAdLog();

        //랜덤한 ID를 생성하기 위해 아래의 함수를 사용합니다.
        // random Numbers for concatenation with attrs
        Random rd = new Random();
        int rdUidNumber = rd.nextInt(9999);
        int rdOrderNumber = rd.nextInt(9999);
        int rdProdIdNumber = rd.nextInt(9999);
        int rdPriceIdNumber = rd.nextInt(90000)+10000;
        int prodCnt = rd.nextInt(9)+1;
        int watchingTime = rd.nextInt(55)+5;

        // bind value for purchaseLog
        tempPurchaseLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempPurchaseLog.setPurchasedDt("20230101070000");
        tempPurchaseLog.setOrderId("od-" + String.format("%05d", rdOrderNumber));
        ArrayList<Map<String, String>> tempProdInfo = new ArrayList<>();
        Map<String, String> tempProd = new HashMap<>();
        for (int i=0; i<prodCnt; i++ ){
            tempProd.put("productId", "pg-" + String.format("%05d", rdProdIdNumber));
            tempProd.put("price", String.format("%05d", rdPriceIdNumber));
            tempProdInfo.add(tempProd);
        }
        tempPurchaseLog.setProductInfo(tempProdInfo);

        // bind value for watchingAdLog
        tempWatchingAdLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempWatchingAdLog.setProductId("pg-" + String.format("%05d", rdProdIdNumber));
        tempWatchingAdLog.setAdId("ad-" + String.format("%05d",rdUidNumber) );
        tempWatchingAdLog.setAdType("banner");
        tempWatchingAdLog.setWatchingTime(String.valueOf(watchingTime));
        tempWatchingAdLog.setWatchingDt("20230201070000");

        // produce msg
        myprdc.sendMsgForPurchaseLog("purchaseLog", tempPurchaseLog);
        myprdc.sendMsgForWatchingAdLog("adLog", tempWatchingAdLog);
    }
}
