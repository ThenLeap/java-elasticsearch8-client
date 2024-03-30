package com.wujunshen.elasticsearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.Property.Builder;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wujunshen.ApplicationTests;
import com.wujunshen.config.ElasticSearchConfigProperties;
import com.wujunshen.entity.height.Bottom;
import com.wujunshen.entity.height.Highest;
import com.wujunshen.entity.height.Middle;
import com.wujunshen.entity.height.Upper;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
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
 * @date 2020/2/7 5:33 下午 <br>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(200)
public class HeightQueryTest extends ApplicationTests {
    private final ObjectMapper mapper = new ObjectMapper();

    @Resource
    private ElasticSearchConfigProperties elasticSearchConfigProperties;

    @Resource
    private IndexApi indexApi;

    @Resource
    private DocumentApi documentApi;

    @Resource
    private QueryApi queryApi;

    private String indexName;

    @BeforeAll
    public void init() throws IOException {
        indexName = elasticSearchConfigProperties.getIndex();

        if (indexApi.isExistedIndex(indexName)) {
            indexApi.deleteIndex(indexName);
        }
        Function<Builder, ObjectBuilder<Property>> bottomFn = fn ->
                fn.nested(bottom -> bottom.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties(
                                "stringValue",
                                stringValue -> stringValue.keyword(keyWordProperty -> keyWordProperty.index(true)))
                        .properties("intValue", intValue -> intValue.integer(intProperty -> intProperty.index(true))));

        Function<Builder, ObjectBuilder<Property>> middleFn = fn ->
                fn.nested(middle -> middle.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties("bottom", bottomFn));

        Function<Builder, ObjectBuilder<Property>> upperFn = fn ->
                fn.nested(upper -> upper.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties("middle", middleFn));

        TypeMapping typeMapping = new TypeMapping.Builder()
                .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                .properties("upper", upperFn)
                .build();

        indexApi.createIndexWithMapping(indexName, typeMapping);

        // 创建数据
        Highest highest = Highest.builder()
                .id(1L)
                .upper(Upper.builder()
                        .id(1L)
                        .middle(Middle.builder()
                                .id(1L)
                                .bottom(Bottom.builder()
                                        .id(1L)
                                        .intValue(8)
                                        .stringValue("test")
                                        .build())
                                .build())
                        .build())
                .build();

        documentApi.addDocument(indexName, highest);

        indexApi.refresh(indexName);
    }

    @AfterAll
    public void clear() throws IOException {
        indexApi.deleteIndex(indexName);

        indexName = null;
    }

    /**
     * 嵌套查询, 内嵌文档查询
     */
    @Test
    public void nestedQuery() throws IOException {
        // 准备查询
        Query query = Query.of(q -> q.bool(t -> t.must(List.of(
                Query.of(q1 ->
                        q1.match(t1 -> t1.field("upper.middle.bottom.intValue").query(8))),
                Query.of(q2 -> q2.match(
                        t2 -> t2.field("upper.middle.bottom.stringValue").query("test")))))));

        List<Highest> result = queryApi.nestedQuery(
                indexName, "upper.middle.bottom", query, ChildScoreMode.None, "id", 0, 10, true, Highest.class);

        log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
    }
}
