package com.example.kafkaExercise.vo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EffectOrNot {

    String adId;  // ad-101
    String userId; //
    String orderId;
    Map<String, String> productInfo;
}
