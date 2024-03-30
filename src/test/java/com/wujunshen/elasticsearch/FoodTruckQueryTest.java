package com.wujunshen.elasticsearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.Property.Builder;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wujunshen.ApplicationTests;
import com.wujunshen.config.ElasticSearchConfigProperties;
import com.wujunshen.entity.foodtruck.FoodTruck;
import com.wujunshen.entity.foodtruck.Location;
import com.wujunshen.entity.foodtruck.Point;
import com.wujunshen.entity.foodtruck.TimeRange;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
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
@Order(300)
public class FoodTruckQueryTest extends ApplicationTests {
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

        Function<Builder, ObjectBuilder<Property>> pointFn = fn ->
                fn.nested(point -> point.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties("lat", lat -> lat.double_(doubleProperty -> doubleProperty.index(true)))
                        .properties("lon", lon -> lon.double_(doubleProperty -> doubleProperty.index(true))));

        Function<Builder, ObjectBuilder<Property>> timeRangeFn = fn -> fn.nested(timeRange -> timeRange
                .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                .properties("from", from -> from.date(dateProperty -> dateProperty.index(true)))
                .properties("to", to -> to.date(dateProperty -> dateProperty.index(true))));

        Function<Builder, ObjectBuilder<Property>> locationFn = fn -> fn.nested(
                location -> location.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties("address", address -> address.text(textProperty -> textProperty.fielddata(true)))
                        .properties("point", pointFn)
                        .properties("timeRange", timeRangeFn));

        TypeMapping typeMapping = new TypeMapping.Builder()
                .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                .properties(
                        "description", description -> description.text(textProperty -> textProperty.fielddata(true)))
                .properties("location", locationFn)
                .build();

        indexApi.createIndexWithMapping(indexName, typeMapping);

        // 创建数据
        FoodTruck foodTruck = FoodTruck.builder()
                .id(1L)
                .description("A very nice truck")
                .location(Location.builder()
                        .id(1L)
                        .address("Cologne City")
                        .point(new Point(1L, 50.9406645, 6.9599115))
                        .timeRange(TimeRange.builder()
                                .id(1L)
                                .from(createTime(8, 30))
                                .to(createTime(12, 30))
                                .build())
                        .build())
                .build();

        documentApi.addDocument(indexName, foodTruck);

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
    public void nestedQueryPoint() throws IOException {
        // 准备查询
        Query query = Query.of(q -> q.bool(t -> t.must(List.of(
                Query.of(q1 -> q1.match(t1 -> t1.field("location.point.lat").query(50.9406645))),
                Query.of(q2 -> q2.range(t2 ->
                        t2.field("location.point.lon").gt(JsonData.of(0.000)).lt(JsonData.of(36.0000))))))));

        List<FoodTruck> result = queryApi.nestedQuery(
                indexName, "location.point", query, ChildScoreMode.None, "id", 0, 10, true, FoodTruck.class);

        log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));

        query = Query.of(q -> q.bool(t -> t.must(List.of(
                Query.of(q1 -> q1.match(t1 -> t1.field("location.address").query("City")))))));

        result = queryApi.nestedQuery(
                indexName, "location", query, ChildScoreMode.None, "id", 0, 10, true, FoodTruck.class);

        log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
    }

    /**
     * 嵌套查询, 内嵌文档查询
     */
    @Test
    public void nestedQueryTimeRange() throws IOException {
        // 准备查询
        Query query = Query.of(q -> q.bool(t -> t.must(Query.of(q2 -> q2.range(t2 -> t2.field("location.timeRange.to")
                .gt(JsonData.of(createTime(12, 0).getTime()))
                .lt(JsonData.of(createTime(13, 0).getTime())))))));

        List<FoodTruck> result = queryApi.nestedQuery(
                indexName, "location.timeRange", query, ChildScoreMode.None, "id", 0, 10, true, FoodTruck.class);

        log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
    }

    @Test
    public void noNestedQuery() throws IOException {
        // 准备查询
        List<FoodTruck> result =
                queryApi.fuzzyQuery(indexName, "truck", "description", "id", 0, 1, true, FoodTruck.class);

        log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));

        result = queryApi.fuzzyQuery(indexName, "fuck", "description", "id", 0, 1, true, FoodTruck.class);

        log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(0));
    }

    private Date createTime(int hour, int minutes) {
        Calendar cal = Calendar.getInstance();

        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minutes);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.DATE, 0);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.YEAR, 0);

        return cal.getTime();
    }
}
