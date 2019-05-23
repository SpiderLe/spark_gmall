package com.atguigu.gmall1128.publisher.service.impl;

import com.king.yl.constant.GmallConstant;
import com.atguigu.gmall1128.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
//        String query = "{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"logDate\": \"2019-05-21\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";
         SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).build();
        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
              total = searchResult.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauHoursMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合操作
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Map dauHourMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //从bucket中获取 分时数据
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                dauHourMap.put( bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new TermQueryBuilder("createDate", date));

        searchSourceBuilder.query(boolQueryBuilder);

        // 聚合
        SumBuilder sumBuilder = AggregationBuilders.sum("sumAmount").field("totalAmount");

        searchSourceBuilder.aggregation(sumBuilder);


        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        Double sumtotalAmount = 0D;
        try {
            SearchResult searchResult = jestClient.execute(search);

            sumtotalAmount = searchResult.getAggregations().getSumAggregation("sumAmount").getSum();

            System.out.println(sumtotalAmount);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return sumtotalAmount;
    }

    @Override
    public Map getOrderHoursMap(String date) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate", date));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合  把聚合操作嵌入分组操作中
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");

        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);

        termsBuilder.subAggregation(sumBuilder);

        searchSourceBuilder.aggregation(termsBuilder);

        Search build = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        Map<String, Double> orderAmountHourMap = new HashMap<>();

        try {
            SearchResult searchResult = jestClient.execute(build);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {

                Double sum_totalAmount = bucket.getSumAggregation("sum_totalAmount").getSum();
                orderAmountHourMap.put(bucket.getKey(), sum_totalAmount);

                System.out.println("bucket.getKey______________________________________"+ bucket.getKey());

            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return orderAmountHourMap;
    }
}
