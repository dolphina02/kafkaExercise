package com.example.kafkaExercise.service;

import org.springframework.stereotype.Service;

@Service
public class KTableService {

//    @Autowired
//    public void buildPipeline(StreamsBuilder sb) {
//
//        KTable<String, String> leftTable = sb.stream("leftTopic", Consumed.with(Serdes.String(),Serdes.String())).toTable();
//        KTable<String, String> rightTable = sb.stream("rightTopic", Consumed.with(Serdes.String(),Serdes.String())).toTable();
//        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
//            return "[StringJoiner]" + leftValue + "-" + rightValue;
//        };
//
//        KTable<String, String> joinedTable = leftTable.join(rightTable, stringJoiner);
//        joinedTable.toStream().to("joinedMsg");
//    }
}
