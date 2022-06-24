package com.yiqun233.psyduck.component.elasticSearch;

import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * /**
 *
 * @author Qun Q Yi
 * @date 2022/6/24 15:50
 * @description
 */
@Log4j2
public class ElasticsearchUtil {

    @Value("${spring.elasticsearch.username}")
    private String username;
    @Value("${spring.elasticsearch.password}")
    private String password;
    @Value("${spring.elasticsearch.cluster-host}")
    private String clusterHost;
    @Value("${spring.elasticsearch.cluster-port}")
    private Integer clusterPort;

    private static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        // 默认缓存限制为100MB，此处修改为30MB。
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(30 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    public RestHighLevelClient getEsClient() {
        //阿里云
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(this.username, this.password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(this.clusterHost, this.clusterPort, "http"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        //内网
//        RestClientBuilder builder = RestClient.builder(new HttpHost(this.clusterHost, this.clusterPort, "http"));
        return new RestHighLevelClient(builder);
    }

    /**
     * 判断索引是否存在
     *
     * @param index 索引名
     */
    public boolean existsIndex(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = getEsClient().indices().exists(request, COMMON_OPTIONS);
        log.info("existsIndex: " + exists);
        return exists;
    }

    /**
     * 创建索引
     *
     * @param index 索引名
     */
    public void createIndex(String index) throws IOException {
        if (!existsIndex(index)) {
            CreateIndexRequest request = new CreateIndexRequest(index);
            CreateIndexResponse createIndexResponse = getEsClient().indices().create(request, COMMON_OPTIONS);
            log.info("createIndex: " + JSON.toJSONString(createIndexResponse));
        }
    }

    /**
     * 删除索引
     *
     * @param index 索引名
     */
    public void deleteIndex(String index) throws IOException {
        if (existsIndex(index)) {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse acknowledgedResponse = getEsClient().indices().delete(request, COMMON_OPTIONS);
            log.info("deleteIndex: " + JSON.toJSONString(acknowledgedResponse));
        }
    }

    /**
     * 添加数据
     *
     * @param data  数据内容
     * @param index 索引名
     * @param id    数据id
     */
    public void addData(Object data, String index, String id) {
        try {
            RestHighLevelClient highClient = getEsClient();
            IndexRequest request = new IndexRequest(index, "_doc").id(id).source(JSON.toJSONString(data), XContentType.JSON);
            IndexResponse response = highClient.index(request, COMMON_OPTIONS);
            highClient.close();
            log.info("索引:{},数据添加,返回码:{},id:{}", index, response.status().getStatus(), id);
        } catch (IOException e) {
            log.error("添加数据失败,index:{},id:{}", index, id);
        }
    }

    /**
     * 添加数据
     *
     * @param list  数据内容
     * @param index 索引名
     */
    public void addDataBatch(List<?> list, String index) {
//        RestHighLevelClient highClient = getEsClient();
//        BulkRequest request = new BulkRequest();
//        for (Object object : list) {
//            request.add(new IndexRequest(index, "_doc").id(IDBuilder.createID()).source(JSON.toJSONString(object), XContentType.JSON));
//        }
//        highClient.bulkAsync(request, RequestOptions.DEFAULT, listener);
//        log.info("开始异步初始化添加数据");
    }

    /**
     * 添加数据
     *
     * @param list  数据内容
     * @param index 索引名
     */
    public void addHomeSearchDataBatch(List<HomeSearchData> list, String index) throws InterruptedException {
        BulkProcessor bulkProcessor = this.bulkProcessor();

        for (HomeSearchData data : list) {
            bulkProcessor.add(new IndexRequest(index, "_doc").id(data.getId()).source(JSON.toJSONString(data), XContentType.JSON));
        }
        bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
        log.info("开始异步初始化添加数据");
    }

    //    @Bean(name = "bulkProcessor") // 可以封装为一个bean，非常方便其余地方来进行 写入 操作
    public BulkProcessor bulkProcessor(){

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> this.getEsClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        // todo do something
                        int i = request.numberOfActions();
                        log.error("ES 同步数量{}",i);
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        // todo do something
                        Iterator<BulkItemResponse> iterator = response.iterator();
                        while (iterator.hasNext()){
                            System.out.println(JSON.toJSONString(iterator.next()));
                        }
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        // todo do something
                        log.error("写入ES 重新消费");
                    }
                }).setBulkActions(1000) //  达到刷新的条数
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB)) // 达到 刷新的大小
                .setFlushInterval(TimeValue.timeValueSeconds(5)) // 固定刷新的时间频率
                .setConcurrentRequests(1) //并发线程数
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // 重试补偿策略
                .build();

    }

//    ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
//        @Override
//        public void onResponse(BulkResponse bulkResponse) {
//            log.info("异步添加数据成功");
//            log.info(bulkResponse.buildFailureMessage());
//        }
//
//        @Override
//        public void onFailure(Exception e) {
//            log.error("异步添加数据失败");
//        }
//    };

    /**
     * 查询数据
     *
     * @param index               索引
     * @param clazz               返回类型
     * @param searchSourceBuilder 查询条件
     */
    public List<?> searchData(String index, Class<?> clazz, SearchSourceBuilder searchSourceBuilder) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        RestHighLevelClient highClient = getEsClient();
        SearchResponse searchResponse = highClient.search(searchRequest, RequestOptions.DEFAULT);
        highClient.close();
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();
        log.info("查询条数: " + totalHits);
        SearchHit[] searchHits = hits.getHits();
        List<?> list = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            list.add(JSON.parseObject(JSON.toJSONString(hit.getSourceAsMap()), (Type) clazz));
        }
        return list;
    }

    public List<HomeSearchData> highLightSearchData(String index, String queryData, String queryType, int current, int pageSize) throws IOException {

        List<HomeSearchData> list = new ArrayList<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder bq = QueryBuilders.boolQuery();
        bq.must(QueryBuilders.multiMatchQuery(queryData, "name", "documentContent"));
        if (!"0".equals(queryType)) {
            bq.must(QueryBuilders.matchQuery("type", queryType));
        }
        searchSourceBuilder.query(bq);
        int from = (current - 1) * pageSize;
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.highlighter(getHighlightBuilder());

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        RestHighLevelClient highClient = getEsClient();
        SearchResponse searchResponse = highClient.search(searchRequest, RequestOptions.DEFAULT);
        highClient.close();
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();
        log.info("查询条数: " + totalHits);
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Map<String, HighlightField> highlightFieldsMap = hit.getHighlightFields();
            getHighLightMap(sourceAsMap, highlightFieldsMap, queryData);
            list.add(JSON.parseObject(JSON.toJSONString(sourceAsMap), HomeSearchData.class));
        }
        return list;
    }

    private HighlightBuilder getHighlightBuilder() {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='#E26400'>");
        highlightBuilder.postTags("</font>");
        /**
         * highlighterType可选:unified,plain和fvh
         * unified : 使用Lucene的统一highlighter。
         * 这个突出显示器将文本分成句子，并使用BM25算法对单个句子进行评分，
         * 就好像它们是语料库中的文档一样。它还支持准确的短语和多项（模糊，前缀，正则表达式）突出显示
         *
         *plain highlighter最适合在单一领域突出简单的查询匹配。
         * 为了准确反映查询逻辑，它会创建一个微小的内存索引，
         * 并通过Lucene的查询执行计划程序重新运行原始查询条件，
         * 以访问当前文档的低级匹配信息。对于需要突出显示的每个字段和每个文档都会
         * 重复此操作。如果要在复杂查询的大量文档中突出显示很多字段，
         * 我们建议使用unified highlighter postings或term_vector字段
         *
         *fvh highlighter使用Lucene的Fast Vector highlighter。此突出显示器可用于映射中term_vector设置为的
         * 字段with_positions_offsets。Fast Vector highlighter
         */
        highlightBuilder.highlighterType("unified");
        /**
         * 这只高亮字段,我这里设置为要查询的字段一致
         */
        highlightBuilder.field("documentContent").field("name");
        //如果要多个字段高亮,这项要为false
        highlightBuilder.requireFieldMatch(false);
        /**
         * fragmentSize  设置要显示出来的fragment文本判断的长度，默认是100
         * numOfFragments 代表要显示几处高亮(可能会搜出多段高亮片段)。默认是5
         * noMatchSize  即使字段中没有关键字命中，也可以返回一段文字，该参数表示从开始多少个字符被返回
         */
        highlightBuilder.fragmentSize(60).numOfFragments(3).noMatchSize();
        return highlightBuilder;
    }

    /**
     * 高亮结果返回：这里要做字段的部分高亮，比如 “我是中国人” 如果只匹配上“是中”，那么就只高亮这两个字其他不亮
     * 正常查询和高亮查询分开返回的，也就是高亮部分数据不会影响正常数据
     * 这里要用高亮数据覆盖正常返回数据，这样返回前端就是匹配的都是高亮显示了
     **/
    private void getHighLightMap(Map<String, Object> map, Map<String, HighlightField> highlightFields, String queryData) {
        HighlightField documentContent = highlightFields.get("documentContent");
        boolean flag = false;
        if (documentContent != null) {
            map.put("documentContent", documentContent.fragments()[0].string());
            flag = true;
        }
//        HighlightField name = highlightFields.get("name");
//        if (name != null) {
//            map.put("name", name.fragments()[0].string());
//            flag = true;
//        }
    }
}
