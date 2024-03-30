package com.wujunshen.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cat.nodes.NodesRecord;
import java.io.IOException;
import java.util.List;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/28 20:26<br>
 */
@Slf4j
@Component
public class NodeApi {
    @Resource
    private ElasticsearchClient elasticsearchClient;

    /**
     * 获取所有索引信息
     *
     * @return NodesRecord列表
     * @throws IOException 异常信息
     */
    public List<NodesRecord> getAllNodes() throws IOException {
        List<NodesRecord> nodesRecords = elasticsearchClient.cat().nodes().valueBody();
        log.info("node size is:{}", nodesRecords.size());

        return nodesRecords;
    }
}
