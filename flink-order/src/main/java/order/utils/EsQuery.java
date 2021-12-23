package order.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import order.domain.Es;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 获取es中的维度数据
 */
public class EsQuery {
    public static Es getEs(String tenantCode, String skuListSkuCode, String env) {

        PropertyUtils.init(env);
        // 创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(PropertyUtils.getStrValue("es.url"))));

        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("product_*");

        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("tenantCode", tenantCode))
                .must(QueryBuilders.termQuery("skuList.skuCode", skuListSkuCode)));
        request.source(sourceBuilder);
        Es es = new Es();
        es.setTENANT_CODE(tenantCode);
        es.setSKU_CODE(skuListSkuCode);
        //对查询的数据进行非空判断,对查询到的数据进行解析
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            String sourceAsString = hits.getAt(0).getSourceAsString();
            JSONObject jsonObject = JSONObject.parseObject(sourceAsString);
            if (jsonObject != null && !jsonObject.isEmpty()) {
                if (jsonObject.get("skuList").toString().length() > 0) {
                    String skuList = jsonObject.get("skuList").toString();
                    boolean skuName1 = JSON.parseArray(skuList).getJSONObject(0).containsKey("skuName");
                    if (skuName1) {
                        String skuName = JSON.parseArray(skuList).getJSONObject(0).get("skuName").toString();
                        es.setSKU_NAME(skuName);
                    }
                }
                if (jsonObject.get("categories").toString().length() > 0) {
                    Set<String> set = new HashSet<String>();
                    String categories = jsonObject.get("categories").toString();
                    JSONArray array = JSON.parseArray(categories);
                    for (int i = 0; i < array.size(); i++){
                        boolean skuName1 = array.getJSONObject(i).containsKey("categoryPath");
                        if (skuName1) {
                            String[] categoryPaths = JSON.parseArray(categories).getJSONObject(0).get("categoryPath").toString().split(">");
                            set.add(categoryPaths[0]);
                        }
                    }
                    String str = set.toString();
                    es.setCATEGORY_CODE(str.substring(1, str.length() - 1));
                }
                if (jsonObject.get("brand").toString().length() > 0) {
                    String brand = jsonObject.get("brand").toString();
                    boolean skuName1 = JSONObject.parseObject(brand).containsKey("brandCode");
                    if (skuName1) {
                        String brandCode = JSONObject.parseObject(brand).get("brandCode").toString();
                        es.setBRAND_CODE(brandCode);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
                return es;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return es;
    }
}