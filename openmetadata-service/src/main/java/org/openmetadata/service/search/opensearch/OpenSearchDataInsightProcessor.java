package org.openmetadata.service.search.opensearch;

import static org.openmetadata.service.workflows.searchIndex.ReindexingUtil.ENTITY_TYPE_KEY;
import static org.openmetadata.service.workflows.searchIndex.ReindexingUtil.getUpdatedStats;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.internal.util.ExceptionUtils;
import org.openmetadata.common.utils.CommonUtil;
import org.openmetadata.schema.analytics.ReportData;
import org.openmetadata.schema.system.IndexingError;
import org.openmetadata.schema.system.StepStats;
import org.openmetadata.service.Entity;
import org.openmetadata.service.exception.SearchIndexException;
import org.openmetadata.service.search.indexes.ReportDataIndexes;
import org.openmetadata.service.search.models.IndexMapping;
import org.openmetadata.service.util.JsonUtils;
import org.openmetadata.service.util.ResultList;
import org.openmetadata.service.workflows.interfaces.Processor;
import os.org.opensearch.action.bulk.BulkRequest;
import os.org.opensearch.action.update.UpdateRequest;
import os.org.opensearch.common.xcontent.XContentType;

@Slf4j
public class OpenSearchDataInsightProcessor
    implements Processor<BulkRequest, ResultList<ReportData>> {
  private final StepStats stats = new StepStats();

  public OpenSearchDataInsightProcessor(int total) {
    this.stats.withTotalRecords(total).withSuccessRecords(0).withFailedRecords(0);
  }

  @Override
  public BulkRequest process(ResultList<ReportData> input, Map<String, Object> contextData)
      throws SearchIndexException {
    String entityType = (String) contextData.get(ENTITY_TYPE_KEY);
    if (CommonUtil.nullOrEmpty(entityType)) {
      throw new IllegalArgumentException(
          "[EsDataInsightProcessor] entityType cannot be null or empty.");
    }

    LOG.debug(
        "[EsDataInsightProcessor] Processing a Batch of Size: {}, EntityType: {} ",
        input.getData().size(),
        entityType);
    BulkRequest requests;
    try {
      requests = buildBulkRequests(entityType, input.getData());
      LOG.debug(
          "[EsDataInsightProcessor] Batch Stats :- Submitted : {} Success: {} Failed: {}",
          input.getData().size(),
          input.getData().size(),
          0);
      updateStats(input.getData().size(), 0);
    } catch (Exception e) {
      IndexingError error =
          new IndexingError()
              .withErrorSource(IndexingError.ErrorSource.PROCESSOR)
              .withSubmittedCount(input.getData().size())
              .withFailedCount(input.getData().size())
              .withSuccessCount(0)
              .withMessage(
                  "Data Insights Processor Encountered Failure. Converting requests to Es Request.")
              .withStackTrace(ExceptionUtils.exceptionStackTraceAsString(e));
      LOG.debug("[EsDataInsightsProcessor] Failed. Details: {}", JsonUtils.pojoToJson(error));
      updateStats(0, input.getData().size());
      throw new SearchIndexException(error);
    }
    return requests;
  }

  private BulkRequest buildBulkRequests(String entityType, List<ReportData> entities) {
    BulkRequest bulkRequests = new BulkRequest();
    for (ReportData reportData : entities) {
      UpdateRequest request = getUpdateRequest(entityType, reportData);
      bulkRequests.add(request);
    }
    return bulkRequests;
  }

  private UpdateRequest getUpdateRequest(String entityType, ReportData reportData) {
    IndexMapping indexMapping = Entity.getSearchRepository().getIndexMapping(entityType);
    UpdateRequest updateRequest =
        new UpdateRequest(
            indexMapping.getIndexName(Entity.getSearchRepository().getClusterAlias()),
            reportData.getId().toString());
    updateRequest.doc(
        JsonUtils.pojoToJson(new ReportDataIndexes(reportData).buildSearchIndexDoc()),
        XContentType.JSON);
    updateRequest.docAsUpsert(true);
    return updateRequest;
  }

  @Override
  public void updateStats(int currentSuccess, int currentFailed) {
    getUpdatedStats(stats, currentSuccess, currentFailed);
  }

  @Override
  public StepStats getStats() {
    return stats;
  }
}
