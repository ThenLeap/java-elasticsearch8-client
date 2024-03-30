package com.wujunshen.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @author frank woo(吴峻申) <br>
 * email:<a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/18 12:46<br>
 */
@Slf4j
@Component
public class DocumentApi {
    @Resource
    private ElasticsearchClient elasticsearchClient;

    /**
     * 单个文档写入
     *
     * @param indexName 索引名
     * @param o         文档对象
     * @param <T>       文档对象泛型
     * @return IndexResponse对象
     * @throws IOException 异常信息
     */
    public <T> IndexResponse addDocument(String indexName, T o) throws IOException {
        IndexResponse indexResponse = elasticsearchClient.index(
                indexRequest -> indexRequest.index(indexName).document(o));

        log.info("response: {}", indexResponse);

        return indexResponse;
    }

    /**
     * 单个文档写入
     *
     * @param indexName 索引名
     * @param id        文档id
     * @param o         文档对象
     * @param <T>       文档对象泛型
     * @return IndexResponse对象
     * @throws IOException 异常信息
     */
    public <T> IndexResponse addDocument(String indexName, String id, T o) throws IOException {
        IndexResponse indexResponse = elasticsearchClient.index(
                indexRequest -> indexRequest.index(indexName).id(id).document(o));

        log.info("response: {}", indexResponse);

        return indexResponse;
    }

    /**
     * 更新文档信息
     *
     * @param indexName 索引名
     * @param o         文档对象
     * @param id        要更新的文档对象id
     * @param clazz     要更新的文档对象class
     * @param <T>       文档对象泛型
     * @return UpdateResponse对象
     * @throws IOException 异常信息
     */
    public <T> UpdateResponse<T> updateDocument(String indexName, T o, String id, Class<T> clazz) throws IOException {
        UpdateResponse<T> updateResponse = elasticsearchClient.update(
                updateRequest -> updateRequest.index(indexName).id(id).doc(o), clazz);

        log.info("response: {}", updateResponse);

        return updateResponse;
    }

    /**
     * 查询文档信息
     *
     * @param indexName 索引名
     * @param id        要查询的文档对象id
     * @param clazz     要查询的文档对象class
     * @param <T>       文档对象泛型
     * @return GetResponse对象
     * @throws IOException 异常信息
     */
    public <T> GetResponse<T> getDocument(String indexName, String id, Class<T> clazz) throws IOException {
        GetResponse<T> getResponse = elasticsearchClient.get(
                getRequest -> getRequest.index(indexName).id(id), clazz);

        log.info("document source: {}, response: {}", getResponse.source(), getResponse);

        return getResponse;
    }

    /**
     * 获取索引下所有文档信息
     *
     * @param indexName 索引名
     * @param clazz 要查询的文档对象class
     * @return 文档对象泛型列表
     * @param <T> 文档对象泛型
     * @throws IOException 异常信息
     */
    public <T> List<T> getAllDocument(String indexName, Class<T> clazz) throws IOException {
        SearchResponse<T> searchResponse = elasticsearchClient.search(a -> a.index(indexName), clazz);

        List<Hit<T>> hitList = getHitList(searchResponse);

        return hitList.stream().map(Hit::source).toList();
    }

    /**
     * 获取索引下所有文档id
     *
     * @param indexName 索引名
     * @param clazz 要查询的文档对象class
     * @return 文档对象id列表
     * @param <T> 文档对象泛型
     * @throws IOException 异常信息
     */
    public <T> List<String> getAllDocumentIds(String indexName, Class<T> clazz) throws IOException {
        SearchResponse<T> searchResponse = elasticsearchClient.search(a -> a.index(indexName), clazz);

        List<Hit<T>> hitList = getHitList(searchResponse);

        return hitList.stream().map(Hit::id).toList();
    }

    /**
     * 删除文档信息
     *
     * @param indexName 索引名
     * @param id        要删除的文档对象id
     * @return DeleteResponse对象
     * @throws IOException 异常信息
     */
    public DeleteResponse deleteDocument(String indexName, String id) throws IOException {
        DeleteResponse deleteResponse = elasticsearchClient.delete(
                deleteRequest -> deleteRequest.index(indexName).id(id));

        log.info("response: {}, result:{}", deleteResponse, deleteResponse.result());

        return deleteResponse;
    }

    /**
     * 删除所有文档信息
     *
     * @param indexName 索引名
     * @param clazz 文档对象泛型的class
     * @param <T>       文档对象泛型
     * @return 删除是否成功
     * @throws IOException 异常信息
     */
    public <T> boolean deleteAllDocument(String indexName, Class<T> clazz) throws IOException {
        return batchDeleteDocument(indexName, getAllDocumentIds(indexName, clazz));
    }

    /**
     * 批量插入文档
     *
     * @param indexName 索引名
     * @param list      批量插入的文档对象list
     * @param <T>       文档对象泛型
     * @return 批量插入是否成功
     * @throws IOException 异常信息
     */
    public <T> boolean batchAddDocument(String indexName, List<T> list) throws IOException {
        BulkRequest.Builder br = new BulkRequest.Builder();

        for (T element : list) {
            br.operations(op -> op.index(idx -> idx.index(indexName).document(element)));
        }

        BulkResponse bulkResponse = elasticsearchClient.bulk(br.build());

        if (bulkResponse.errors()) {
            log.error("Bulk had errors");
            for (BulkResponseItem item : bulkResponse.items()) {
                if (item.error() != null) {
                    log.error("{}", item.error().reason());
                }
            }
            return false;
        } else {
            log.info("Bulk write success!");
            return true;
        }
    }

    /**
     * 批量删除文档
     *
     * @param indexName 索引名
     * @param ids      批量删除的文档id的列表
     * @param <T>       文档对象泛型
     * @return 批量删除是否成功
     * @throws IOException 异常信息
     */
    public <T> boolean batchDeleteDocument(String indexName, List<String> ids) throws IOException {
        BulkRequest.Builder br = new BulkRequest.Builder();

        for (String id : ids) {
            br.operations(op -> op.delete(idx -> idx.index(indexName).id(id)));
        }

        BulkResponse bulkResponse = elasticsearchClient.bulk(br.build());

        if (bulkResponse.errors()) {
            log.error("Bulk had errors");
            for (BulkResponseItem item : bulkResponse.items()) {
                if (item.error() != null) {
                    log.error("{}", item.error().reason());
                }
            }
            return false;
        } else {
            log.info("Bulk delete success!");
            return true;
        }
    }

    /**
     * 获取Hit对象列表
     *
     * @param response SearchResponse对象
     * @return 文档对象Hit泛型列表
     * @param <T> 文档对象泛型
     */
    private <T> List<Hit<T>> getHitList(SearchResponse<T> response) {
        log.info("consume times {} mill second", response.took());

        List<Hit<T>> hitList = response.hits().hits();

        if (CollectionUtils.isEmpty(hitList)) {
            return new ArrayList<>();
        }

        return hitList;
    }
}
