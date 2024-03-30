package com.wujunshen.elasticsearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
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
 * @date 2022/8/19 10:17<br>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(20)
class DocumentApiTest extends ApplicationTests {
    @Resource
    private ElasticSearchConfigProperties elasticSearchConfigProperties;

    @Resource
    private IndexApi indexApi;

    @Resource
    private DocumentApi documentApi;

    private Sku sku;

    private Sku updateSku;

    private List<Sku> skuList;

    private String indexName;

    @BeforeAll
    void setUp() throws IOException {
        indexName = elasticSearchConfigProperties.getIndex();

        sku = Sku.builder().id(1L).skuName("City bike").skuPrice(123).build();

        updateSku = sku;
        updateSku.setSkuName("updated bike");
        updateSku.setSkuPrice(199);

        skuList = bulkWriteSkus();

        TypeMapping typeMapping = new TypeMapping.Builder()
                .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                .properties("skuName", skuName -> skuName.text(textProperty -> textProperty.fielddata(true)))
                .properties("skuPrice", skuPrice -> skuPrice.integer(intProperty -> intProperty.index(true)))
                .build();

        indexApi.createIndexWithMapping(indexName, typeMapping);

        indexApi.refresh(indexName);
    }

    @AfterAll
    void tearDown() throws IOException {
        sku = null;
        updateSku = null;
        skuList = null;

        indexApi.deleteIndex(indexName);

        indexName = null;
    }

    /**
     * 单个文档写入
     */
    @Order(30)
    @Test
    void addDocument() throws IOException {
        IndexResponse indexResponse = documentApi.addDocument(indexName, sku);

        String index = indexResponse.index();
        log.info("Indexed: {}", index);

        assertThat(index, equalTo(indexName));

        indexResponse = documentApi.addDocument(indexName, String.valueOf(sku.getId()), sku);

        index = indexResponse.index();
        log.info("Indexed: {}", index);

        assertThat(index, equalTo(indexName));
    }

    /**
     * 单个文档更新
     */
    @Order(35)
    @Test
    void updateDocument() throws IOException {
        IndexResponse indexResponse = documentApi.addDocument(indexName, updateSku);

        UpdateResponse<Sku> updateResponse =
                documentApi.updateDocument(indexName, updateSku, indexResponse.id(), Sku.class);

        String index = updateResponse.index();
        log.info("Indexed: {}", index);

        assertThat(index, equalTo(indexName));
    }

    /**
     * 查询文档信息
     */
    @Order(40)
    @Test
    void getDocument() throws IOException {
        IndexResponse indexResponse = documentApi.addDocument(indexName, sku);

        GetResponse<Sku> getResponse = documentApi.getDocument(indexName, indexResponse.id(), Sku.class);

        String index = getResponse.index();
        log.info("Indexed: {}", index);

        assertThat(index, equalTo(indexName));
    }

    /**
     * 删除文档信息
     */
    @Order(45)
    @Test
    void deleteDocument() throws IOException {
        IndexResponse indexResponse = documentApi.addDocument(indexName, sku);

        DeleteResponse deleteResponse = documentApi.deleteDocument(indexName, indexResponse.id());

        String index = deleteResponse.index();
        log.info("Indexed: {}", index);

        assertThat(index, equalTo(indexName));
    }

    /**
     * 批量文档写入
     */
    @Order(50)
    @Test
    void batchAddDocument() throws IOException {
        boolean result = documentApi.batchAddDocument(indexName, skuList);

        log.info("batch insert operation: {}", result);

        assertThat(result, is(true));
    }

    /**
     * 删除所有文档
     */
    @Order(55)
    @Test
    void deleteAllDocument() throws IOException {
        documentApi.batchAddDocument(indexName, skuList);
        indexApi.refresh(indexName);

        boolean result = documentApi.deleteAllDocument(indexName, Sku.class);

        log.info("batch insert operation: {}", result);

        assertThat(result, is(true));
    }

    /**
     * 批量文档删除
     */
    @Order(57)
    @Test
    void batchDeleteDocument() throws IOException {
        documentApi.batchAddDocument(indexName, skuList);
        indexApi.refresh(indexName);

        List<String> ids = skuList.stream().map(e -> String.valueOf(e.getId())).toList();

        boolean result = documentApi.batchDeleteDocument(indexName, ids);

        log.info("batch insert operation: {}", result);

        assertThat(result, is(true));
    }

    private List<Sku> bulkWriteSkus() {
        List<Sku> result = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            String type = "City bike " + i;
            int price = (int) (Math.random() * 3 * 100);
            result.add(Sku.builder().id((long) i).skuName(type).skuPrice(price).build());
        }
        return result;
    }
}
