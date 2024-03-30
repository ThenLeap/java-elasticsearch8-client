package com.wujunshen.elasticsearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import co.elastic.clients.elasticsearch.cat.nodes.NodesRecord;
import com.wujunshen.ApplicationTests;
import com.wujunshen.config.ElasticSearchConfigProperties;
import java.io.IOException;
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
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/28 20:29<br>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(0)
class NodeApiTest extends ApplicationTests {
    @Resource
    private ElasticSearchConfigProperties elasticSearchConfigProperties;

    @Resource
    private NodeApi nodeApi;

    @BeforeAll
    void setUp() {}

    @AfterAll
    void tearDown() {}

    /**
     * 获取所有节点信息
     */
    @Order(0)
    @Test
    void getAllNodes() throws IOException {
        List<NodesRecord> nodesRecords = nodeApi.getAllNodes();

        for (NodesRecord nodesRecord : nodesRecords) {
            log.info("\nnodeName:{}\nip:{}", nodesRecord.name(), nodesRecord.ip());
        }

        assertThat(nodesRecords, notNullValue());
        assertThat(nodesRecords, hasSize(equalTo(1)));
    }
}
