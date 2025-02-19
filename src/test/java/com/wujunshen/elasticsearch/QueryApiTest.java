package com.wujunshen.elasticsearch;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.GetResponse;
import com.wujunshen.ApplicationTests;
import com.wujunshen.config.ElasticSearchConfigProperties;
import com.wujunshen.entity.product.Sku;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * @author frank woo(吴峻申) <br>
 * email:<a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/19 10:55<br>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(30)
class QueryApiTest extends ApplicationTests {
    @Resource
    private ElasticSearchConfigProperties elasticSearchConfigProperties;

    @Resource
    private DocumentApi documentApi;

    @Resource
    private QueryApi queryApi;

    @Resource
    private IndexApi indexApi;

    private Sku sku;

    private List<Sku> skuList;

    private String indexName;

    @BeforeAll
    void setUp() throws IOException, InterruptedException {
        indexName = elasticSearchConfigProperties.getIndex();

        sku = Sku.builder().id(1L).skuName("City bike").skuPrice(123).build();

        skuList = bulkWriteProducts();

        TypeMapping typeMapping = new TypeMapping.Builder()
                .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                .properties("skuName", skuName -> skuName.text(textProperty -> textProperty.fielddata(true)))
                .properties("skuPrice", skuPrice -> skuPrice.integer(intProperty -> intProperty.index(true)))
                .build();

        indexApi.createIndexWithMapping(indexName, typeMapping);

        documentApi.addDocument(indexName, String.valueOf(sku.getId()), sku);

        documentApi.batchAddDocument(indexName, skuList);

        indexApi.refresh(indexName);
    }

    @AfterAll
    void tearDown() throws IOException {
        sku = null;
        skuList = null;

        indexApi.deleteIndex(indexName);

        indexName = null;
    }

    /**
     * 聚合操作
     */
    @Order(65)
    @Test
    void aggsByHistogram() throws IOException {
        List<HistogramBucket> buckets =
                queryApi.aggsByHistogram(indexName, "bike", "skuName", "skuPrice", "price-histogram", 50.0);

        for (HistogramBucket bucket : buckets) {
            log.info("There are " + bucket.docCount() + " bikes under " + bucket.key());
        }

        assertThat(buckets, notNullValue());
    }

    /**
     * 指定id检索数据
     */
    @Order(70)
    @Test
    void searchById() throws IOException {
        GetResponse<Sku> response = queryApi.searchById(indexName, String.valueOf(sku.getId()), Sku.class);

        if (response != null) {
            Sku source = response.source();
            log.info("sku: {}", source);
        }

        assertThat(response, notNullValue());
    }

    private List<Sku> bulkWriteProducts() {
        List<Sku> result = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            String type = "City bike " + i;
            int price = (int) (Math.random() * 3 * 100);
            result.add(Sku.builder().id((long) i).skuName(type).skuPrice(price).build());
        }
        return result;
    }
}
