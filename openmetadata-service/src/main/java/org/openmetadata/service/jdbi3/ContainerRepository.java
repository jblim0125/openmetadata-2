package org.openmetadata.service.jdbi3;

import com.google.common.collect.Lists;
import org.jdbi.v3.sqlobject.transaction.Transaction;
import org.openmetadata.schema.EntityInterface;
import org.openmetadata.schema.api.data.CreateTableProfile;
import org.openmetadata.schema.api.feed.ResolveTask;
import org.openmetadata.schema.entity.data.Container;
import org.openmetadata.schema.entity.data.DashboardDataModel;
import org.openmetadata.schema.entity.services.StorageService;
import org.openmetadata.schema.tests.CustomMetric;
import org.openmetadata.schema.type.*;
import org.openmetadata.service.Entity;
import org.openmetadata.service.jdbi3.FeedRepository.TaskWorkflow;
import org.openmetadata.service.jdbi3.FeedRepository.ThreadContext;
import org.openmetadata.service.resources.feeds.MessageParser.EntityLink;
import org.openmetadata.service.resources.storages.ContainerResource;
import org.openmetadata.service.security.mask.PIIMasker;
import org.openmetadata.service.util.EntityUtil;
import org.openmetadata.service.util.FullyQualifiedName;
import org.openmetadata.service.util.JsonUtils;
import org.openmetadata.service.util.ResultList;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.openmetadata.common.utils.CommonUtil.listOrEmpty;
import static org.openmetadata.schema.type.Include.ALL;
import static org.openmetadata.schema.type.Include.NON_DELETED;
import static org.openmetadata.service.Entity.*;

public class ContainerRepository extends EntityRepository<Container> {
  private static final String CONTAINER_UPDATE_FIELDS = "dataModel";
  private static final String CONTAINER_PATCH_FIELDS = "dataModel";

  // JBLIM : for profiling : extension type
  public static final String CONTAINER_TABLE_PROFILER_CONFIG_EXTENSION = "container.tableProfilerConfig";

  public static final String CONTAINER_TABLE_PROFILE_EXTENSION = "container.tableProfile";
  public static final String CONTAINER_TABLE_COLUMN_PROFILE_EXTENSION = "container.columnProfile";
  public static final String CONTAINER_SYSTEM_PROFILE_EXTENSION = "container.systemProfile";

  public static final String CONTAINER_SAMPLE_DATA_EXTENSION = "container.sampleData";
  public static final String CONTAINER_UNSTRUCTURED_SAMPLE_DATA_EXTENSION = "container.unstructured_sampleData";
  // JBLIM : for profiling - custom metrics ( customMetrics.container.table / customMetrics.container.column )
  public static final String CUSTOM_METRICS_EXTENSION = "customMetrics.";
  public static final String CONTAINER_TABLE_EXTENSION = "container.table";
  public static final String CONTAINER_TABLE_COLUMN_EXTENSION = "container.column";
  // JBLIM : for profiling : data fields
  public static final String TABLE_PROFILER_CONFIG = "tableProfilerConfig";
  public static final String DATA_MODEL_FIELD = "dataModel";
  public static final String CUSTOM_METRICS = "customMetrics";


  public ContainerRepository() {
    super(
        ContainerResource.COLLECTION_PATH,
        Entity.CONTAINER,
        Container.class,
        Entity.getCollectionDAO().containerDAO(),
        CONTAINER_PATCH_FIELDS,
        CONTAINER_UPDATE_FIELDS);
    supportsSearch = true;
  }

  @Override
  public void setFields(Container container, EntityUtil.Fields fields) {
    setDefaultFields(container);
    container.setParent(
        fields.contains(FIELD_PARENT) ? getParent(container) : container.getParent());
    if (container.getDataModel() != null) {
      populateDataModelColumnTags(
          fields.contains(FIELD_TAGS),
          container.getFullyQualifiedName(),
          container.getDataModel().getColumns());
    }

    container.setTableProfilerConfig(
            fields.contains(TABLE_PROFILER_CONFIG)
                    ? getTableProfilerConfig(container)
                    : container.getTableProfilerConfig());
    container.setTestSuite(fields.contains("testSuite") ? getTestSuite(container) : container.getTestSuite());
    container.setCustomMetrics(
            fields.contains(CUSTOM_METRICS) ? getCustomMetrics(container, null)
                    : container.getCustomMetrics());
    if ((fields.contains(DATA_MODEL_FIELD)) && (fields.contains(CUSTOM_METRICS))) {
      if( container.getDataModel() != null) {
        for (Column column : container.getDataModel().getColumns()) {
          column.setCustomMetrics(getCustomMetrics(container, column.getName()));
        }
      }
    }
    container.setTableProfilerConfig(
            fields.contains(TABLE_PROFILER_CONFIG)
                    ? getTableProfilerConfig(container)
                    : container.getTableProfilerConfig());


  }

  @Override
  public void clearFields(Container container, EntityUtil.Fields fields) {
    container.setParent(fields.contains(FIELD_PARENT) ? container.getParent() : null);
    container.withDataModel(fields.contains("dataModel") ? container.getDataModel() : null);
    container.setTableProfilerConfig(
            fields.contains(TABLE_PROFILER_CONFIG) ? container.getTableProfilerConfig() : null);
    container.setTestSuite(fields.contains("testSuite") ? container.getTestSuite() : null);
  }

  private void populateDataModelColumnTags(
      boolean setTags, String fqnPrefix, List<Column> columns) {
    populateEntityFieldTags(entityType, columns, fqnPrefix, setTags);
  }

  private void setDefaultFields(Container container) {
    EntityReference parentServiceRef =
        getFromEntityRef(container.getId(), Relationship.CONTAINS, STORAGE_SERVICE, true);
    container.withService(parentServiceRef);
  }

  @Override
  public void setFullyQualifiedName(Container container) {
    container.setParent(
        container.getParent() != null ? container.getParent() : getParent(container));
    if (container.getParent() != null) {
      container.setFullyQualifiedName(
          FullyQualifiedName.add(
              container.getParent().getFullyQualifiedName(), container.getName()));
    } else {
      container.setFullyQualifiedName(
          FullyQualifiedName.add(
              container.getService().getFullyQualifiedName(), container.getName()));
    }
    if (container.getDataModel() != null) {
      setColumnFQN(container.getFullyQualifiedName(), container.getDataModel().getColumns());
    }
  }

  private void setColumnFQN(String parentFQN, List<Column> columns) {
    columns.forEach(
        c -> {
          String columnFqn = FullyQualifiedName.add(parentFQN, c.getName());
          c.setFullyQualifiedName(columnFqn);
          if (c.getChildren() != null) {
            setColumnFQN(columnFqn, c.getChildren());
          }
        });
  }

  @Override
  public void prepare(Container container, boolean update) {
    // the storage service is not fully filled in terms of props - go to the db and get it in full
    // and re-set it
    StorageService storageService =
        Entity.getEntity(container.getService(), "", Include.NON_DELETED);
    container.setService(storageService.getEntityReference());
    container.setServiceType(storageService.getServiceType());

    if (container.getParent() != null) {
      Container parent = Entity.getEntity(container.getParent(), "owner", ALL);
      container.withParent(parent.getEntityReference());
    }
  }

  @Override
  public void storeEntity(Container container, boolean update) {
    EntityReference storageService = container.getService();
    EntityReference parent = container.getParent();
    container.withService(null).withParent(null);

    // Don't store datamodel column tags as JSON but build it on the fly based on relationships
    List<Column> columnWithTags = Lists.newArrayList();
    if (container.getDataModel() != null) {
      columnWithTags.addAll(container.getDataModel().getColumns());
      container.getDataModel().setColumns(ColumnUtil.cloneWithoutTags(columnWithTags));
      container.getDataModel().getColumns().forEach(column -> column.setTags(null));
    }

    store(container, update);

    // Restore the relationships
    container.withService(storageService).withParent(parent);
    if (container.getDataModel() != null) {
      container.getDataModel().setColumns(columnWithTags);
    }
  }

  @Override
  public void restorePatchAttributes(Container original, Container updated) {
    // Patch can't make changes to following fields. Ignore the changes
    super.restorePatchAttributes(original, updated);
    updated.withService(original.getService()).withParent(original.getParent());
  }

  @Override
  public void storeRelationships(Container container) {
    // store each relationship separately in the entity_relationship table
    addServiceRelationship(container, container.getService());

    // parent container if exists
    EntityReference parentReference = container.getParent();
    if (parentReference != null) {
      addRelationship(
          parentReference.getId(), container.getId(), CONTAINER, CONTAINER, Relationship.CONTAINS);
    }
  }

  @Override
  public EntityUpdater getUpdater(Container original, Container updated, Operation operation) {
    return new ContainerUpdater(original, updated, operation);
  }

  @Transaction
  public Container addTableSampleData(UUID containerId, TableData sampleData) {
    Container container = find(containerId, NON_DELETED);

    for (String columnName : sampleData.getColumns()) {
      validateColumn(container, columnName);
    }
    // Make sure each row has number values for all the columns
    for (List<Object> row : sampleData.getRows()) {
      if (row.size() != sampleData.getColumns().size()) {
        throw new IllegalArgumentException(
                String.format(
                        "Number of columns is %d but row has %d sample values",
                        sampleData.getColumns().size(), row.size()));
      }
    }

    daoCollection
      .entityExtensionDAO()
      .insert(containerId, CONTAINER_SAMPLE_DATA_EXTENSION,
              "tableData", JsonUtils.pojoToJson(sampleData));
    setFieldsInternal(container, EntityUtil.Fields.EMPTY_FIELDS);
    return container.withSampleData(sampleData);
  }

  @Transaction
  public Container addUnstructuredSampleData(UUID containerId, String sampleData) {
    Container container = find(containerId, NON_DELETED);

    daoCollection
            .entityExtensionDAO()
            .insert(containerId, CONTAINER_UNSTRUCTURED_SAMPLE_DATA_EXTENSION,
                    "docSample", sampleData);
    setFieldsInternal(container, EntityUtil.Fields.EMPTY_FIELDS);
    try {
      sampleData = sampleData.replaceAll("^\"|\"$", "");
      sampleData = URLDecoder.decode(sampleData, "UTF-8");
    } catch (UnsupportedEncodingException ignored) {
    }
    return container.withSampleData(sampleData);
  }

  public Container getSampleData(UUID containerId, boolean authorizePII) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);
    // for unstructured data
    switch(container.getFileFormats().get(0)) {
      case Doc, Docx, Hwp, Hwpx -> {
        String sampleData =
                daoCollection
                        .entityExtensionDAO()
                        .getExtension(container.getId(), CONTAINER_UNSTRUCTURED_SAMPLE_DATA_EXTENSION);
        try {
          sampleData = sampleData.replaceAll("^\"|\"$", "");
          sampleData = URLDecoder.decode(sampleData, "UTF-8");
        } catch (UnsupportedEncodingException ignored) {
        }
        container.setSampleData(sampleData);
        return container;
      }
    }
    // for structured data
    TableData sampleData =
            JsonUtils.readValue(
                    daoCollection
                            .entityExtensionDAO()
                            .getExtension(container.getId(), CONTAINER_SAMPLE_DATA_EXTENSION),
                    TableData.class);
    container.setSampleData(sampleData);
    setFieldsInternal(container, EntityUtil.Fields.EMPTY_FIELDS);

    // Set the column tags. Will be used to mask the sample data
    if (!authorizePII) {
      populateEntityFieldTags(entityType, container.getDataModel().getColumns(),
              container.getFullyQualifiedName(), true);
      container.setTags(getTags(container));
      return PIIMasker.getSampleData(container);
    }

    return container;
  }

  @Transaction
  public Container deleteSampleData(UUID containerId) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);
    daoCollection.entityExtensionDAO().delete(containerId, CONTAINER_SAMPLE_DATA_EXTENSION);
    daoCollection.entityExtensionDAO().delete(containerId, CONTAINER_UNSTRUCTURED_SAMPLE_DATA_EXTENSION);
    setFieldsInternal(container, EntityUtil.Fields.EMPTY_FIELDS);
    return container;
  }

  public TableProfilerConfig getTableProfilerConfig(Container container) {
    return JsonUtils.readValue(
            daoCollection
                    .entityExtensionDAO()
                    .getExtension(container.getId(), CONTAINER_TABLE_PROFILER_CONFIG_EXTENSION),
            TableProfilerConfig.class);
  }

  public EntityReference getTestSuite(Container container) {
    return getToEntityRef(container.getId(), Relationship.CONTAINS, TEST_SUITE, false);
  }

  @Transaction
  public Container addTableProfilerConfig(UUID containerId, TableProfilerConfig tableProfilerConfig) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);

    // Validate all the columns
    if (tableProfilerConfig.getExcludeColumns() != null) {
      for (String columnName : tableProfilerConfig.getExcludeColumns()) {
        validateColumn(container, columnName);
      }
    }

    if (tableProfilerConfig.getIncludeColumns() != null) {
      for (ColumnProfilerConfig columnProfilerConfig : tableProfilerConfig.getIncludeColumns()) {
        validateColumn(container, columnProfilerConfig.getColumnName());
      }
    }
    if (tableProfilerConfig.getProfileSampleType() != null
            && tableProfilerConfig.getProfileSample() != null) {
      EntityUtil.validateProfileSample(
              tableProfilerConfig.getProfileSampleType().toString(),
              tableProfilerConfig.getProfileSample());
    }

    daoCollection
            .entityExtensionDAO()
            .insert(
                    containerId,
                    CONTAINER_TABLE_PROFILER_CONFIG_EXTENSION,
                    TABLE_PROFILER_CONFIG,
                    JsonUtils.pojoToJson(tableProfilerConfig));
    clearFields(container, EntityUtil.Fields.EMPTY_FIELDS);
    return container.withTableProfilerConfig(tableProfilerConfig);
  }

  @Transaction
  public Container deleteTableProfilerConfig(UUID containerId) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);
    daoCollection.entityExtensionDAO().delete(containerId, CONTAINER_TABLE_PROFILER_CONFIG_EXTENSION);
    clearFieldsInternal(container, EntityUtil.Fields.EMPTY_FIELDS);
    return container;
  }

  private Column getColumnNameForProfiler(
          List<Column> columnList, ColumnProfile columnProfile, String parentName) {
    for (Column col : columnList) {
      String columnName;
      if (parentName != null) {
        columnName = String.format("%s.%s", parentName, col.getName());
      } else {
        columnName = col.getName();
      }
      if (columnName.equals(columnProfile.getName())) {
        return col;
      }
      if (col.getChildren() != null) {
        Column childColumn = getColumnNameForProfiler(col.getChildren(), columnProfile, columnName);
        if (childColumn != null) {
          return childColumn;
        }
      }
    }
    return null;
  }

  public Container addTableProfileData(UUID containerId, CreateTableProfile createTableProfile) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);
    daoCollection
            .profilerDataTimeSeriesDao()
            .insert(
                    container.getFullyQualifiedName(),
                    CONTAINER_TABLE_PROFILE_EXTENSION,
                    "tableProfile",
                    JsonUtils.pojoToJson(createTableProfile.getTableProfile()));

    for (ColumnProfile columnProfile : createTableProfile.getColumnProfile()) {
      // Validate all the columns
      Column column = getColumnNameForProfiler(container.getDataModel().getColumns(), columnProfile, null);
      if (column == null) {
        throw new IllegalArgumentException("Invalid column name " + columnProfile.getName());
      }
      daoCollection
              .profilerDataTimeSeriesDao()
              .insert(
                      column.getFullyQualifiedName(),
                      CONTAINER_TABLE_COLUMN_PROFILE_EXTENSION,
                      "columnProfile",
                      JsonUtils.pojoToJson(columnProfile));
    }

    List<SystemProfile> systemProfiles = createTableProfile.getSystemProfile();
    if (systemProfiles != null && !systemProfiles.isEmpty()) {
      for (SystemProfile systemProfile : createTableProfile.getSystemProfile()) {
        // system metrics timestamp is the one of the operation. We'll need to
        // update the entry if it already exists in the database
        String storedSystemProfile =
                daoCollection
                        .profilerDataTimeSeriesDao()
                        .getExtensionAtTimestampWithOperation(
                                container.getFullyQualifiedName(),
                                CONTAINER_SYSTEM_PROFILE_EXTENSION,
                                systemProfile.getTimestamp(),
                                systemProfile.getOperation().value());
        daoCollection
                .profilerDataTimeSeriesDao()
                .storeTimeSeriesWithOperation(
                        container.getFullyQualifiedName(),
                        CONTAINER_SYSTEM_PROFILE_EXTENSION,
                        "systemProfile",
                        JsonUtils.pojoToJson(systemProfile),
                        systemProfile.getTimestamp(),
                        systemProfile.getOperation().value(),
                        storedSystemProfile != null);
      }
    }

    setFieldsInternal(container, EntityUtil.Fields.EMPTY_FIELDS);
    return container.withProfile(createTableProfile.getTableProfile());
  }

  public void deleteTableProfile(String fqn, String entityType, Long timestamp) {
    // Validate the request content
    String extension;
    if (entityType.equalsIgnoreCase(CONTAINER)) {
      extension = CONTAINER_TABLE_PROFILE_EXTENSION;
    } else if (entityType.equalsIgnoreCase("column")) {
      extension = CONTAINER_TABLE_COLUMN_PROFILE_EXTENSION;
    }
    else if (entityType.equalsIgnoreCase("system")) {
      extension = CONTAINER_SYSTEM_PROFILE_EXTENSION;
    }
    else {
      throw new IllegalArgumentException("entityType must be table, column or system");
    }
    daoCollection.profilerDataTimeSeriesDao().deleteAtTimestamp(fqn, extension, timestamp);
  }

  public ResultList<TableProfile> getTableProfiles(String fqn, Long startTs, Long endTs) {
    List<TableProfile> tableProfiles;
    tableProfiles =
            JsonUtils.readObjects(
                    daoCollection
                            .profilerDataTimeSeriesDao()
                            .listBetweenTimestampsByOrder(
                                    fqn, CONTAINER_TABLE_PROFILE_EXTENSION, startTs, endTs, EntityTimeSeriesDAO.OrderBy.DESC),
                    TableProfile.class);
    return new ResultList<>(
            tableProfiles, startTs.toString(), endTs.toString(), tableProfiles.size());
  }

  public ResultList<ColumnProfile> getColumnProfiles(
          String fqn, Long startTs, Long endTs, boolean authorizePII) {
    List<ColumnProfile> columnProfiles;
    columnProfiles =
            JsonUtils.readObjects(
                    daoCollection
                            .profilerDataTimeSeriesDao()
                            .listBetweenTimestampsByOrder(
                                    fqn,
                                    CONTAINER_TABLE_COLUMN_PROFILE_EXTENSION,
                                    startTs,
                                    endTs,
                                    EntityTimeSeriesDAO.OrderBy.DESC),
                    ColumnProfile.class);
    ResultList<ColumnProfile> columnProfileResultList =
            new ResultList<>(
                    columnProfiles, startTs.toString(), endTs.toString(), columnProfiles.size());
    if (!authorizePII) {
      // Mask the PII data
      columnProfileResultList.setData(
              PIIMasker.getColumnProfile(fqn, columnProfileResultList.getData()));
    }
    return columnProfileResultList;
  }

  public ResultList<SystemProfile> getSystemProfiles(String fqn, Long startTs, Long endTs) {
    List<SystemProfile> systemProfiles;
    systemProfiles =
            JsonUtils.readObjects(
                    daoCollection
                            .profilerDataTimeSeriesDao()
                            .listBetweenTimestampsByOrder(
                                    fqn,
                                    CONTAINER_SYSTEM_PROFILE_EXTENSION,
                                    startTs,
                                    endTs,
                                    EntityTimeSeriesDAO.OrderBy.DESC),
                    SystemProfile.class);
    return new ResultList<>(
            systemProfiles, startTs.toString(), endTs.toString(), systemProfiles.size());
  }

  private void setColumnProfile(List<Column> columnList) {
    for (Column column : columnList) {
      ColumnProfile columnProfile =
              JsonUtils.readValue(
                      daoCollection
                              .profilerDataTimeSeriesDao()
                              .getLatestExtension(
                                      column.getFullyQualifiedName(), CONTAINER_TABLE_COLUMN_PROFILE_EXTENSION),
                      ColumnProfile.class);
      column.setProfile(columnProfile);
      if (column.getChildren() != null) {
        setColumnProfile(column.getChildren());
      }
    }
  }

  public Container getLatestTableProfile(String fqn, boolean authorizePII) {
    Container container = findByName(fqn, ALL);
    TableProfile tableProfile =
            JsonUtils.readValue(
                    daoCollection
                            .profilerDataTimeSeriesDao()
                            .getLatestExtension(container.getFullyQualifiedName(),
                                    CONTAINER_TABLE_PROFILE_EXTENSION),
                    TableProfile.class);
    container.setProfile(tableProfile);
    setColumnProfile(container.getDataModel().getColumns());

    // Set the column tags. Will be used to hide the data
    if (!authorizePII) {
      populateEntityFieldTags(entityType, container.getDataModel().getColumns(),
              container.getFullyQualifiedName(), true);
      return PIIMasker.getTableProfile(container);
    }

    return container;
  }

  public Container addCustomMetric(UUID containerId, CustomMetric customMetric) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);

    String customMetricName = customMetric.getName();
    String customMetricColumnName = customMetric.getColumnName();
    String extensionType =
            customMetricColumnName != null ? CONTAINER_TABLE_COLUMN_EXTENSION : CONTAINER_TABLE_EXTENSION;
    String extension = CUSTOM_METRICS_EXTENSION + extensionType + "." + customMetricName;

    // Validate the column name exists in the table
    if (customMetricColumnName != null) {
      validateColumn(container, customMetricColumnName);
    }

    CustomMetric storedCustomMetrics = getCustomMetric(container, extension);
    if (storedCustomMetrics != null) {
      storedCustomMetrics.setExpression(customMetric.getExpression());
    }

    daoCollection
            .entityExtensionDAO()
            .insert(container.getId(), extension, "customMetric", JsonUtils.pojoToJson(customMetric));
    // return the newly created/updated custom metric only
    setFieldsInternal(container, new EntityUtil.Fields(Set.of(CUSTOM_METRICS, DATA_MODEL_FIELD)));
    return container;
  }

  public Container deleteCustomMetric(UUID containerId, String columnName, String metricName) {
    // Validate the request content
    Container container = find(containerId, NON_DELETED);
    if (columnName != null) validateColumn(container, columnName);

    // Get unique entity extension and delete data from DB
    String extensionType = columnName != null ?
            CONTAINER_TABLE_COLUMN_EXTENSION : CONTAINER_TABLE_EXTENSION;
    String extension = CUSTOM_METRICS_EXTENSION + extensionType + "." + metricName;
    daoCollection.entityExtensionDAO().delete(containerId, extension);

    // return the newly created/updated custom metric only
    setFieldsInternal(container, new EntityUtil.Fields(Set.of(CUSTOM_METRICS, DATA_MODEL_FIELD)));
    return container;
  }

  private CustomMetric getCustomMetric(Container container, String extension) {
    return JsonUtils.readValue(
            daoCollection.entityExtensionDAO().getExtension(container.getId(), extension),
            CustomMetric.class);
  }

  private List<CustomMetric> getCustomMetrics(Container container, String columnName) {
    String extension = columnName != null ? CONTAINER_TABLE_COLUMN_EXTENSION : CONTAINER_TABLE_EXTENSION;
    extension = CUSTOM_METRICS_EXTENSION + extension;

    List<CollectionDAO.ExtensionRecord> extensionRecords =
            daoCollection.entityExtensionDAO().getExtensions(container.getId(), extension);
    List<CustomMetric> customMetrics = new ArrayList<>();
    for (CollectionDAO.ExtensionRecord extensionRecord : extensionRecords) {
      customMetrics.add(JsonUtils.readValue(extensionRecord.extensionJson(), CustomMetric.class));
    }

    if (columnName != null) {
      // Filter custom metrics by column name
      customMetrics =
              customMetrics.stream()
                      .filter(metric -> metric.getColumnName().equals(columnName))
                      .collect(Collectors.toList());
    }

    return customMetrics;
  }

  @Override
  public void applyTags(Container container) {
    // Add container level tags by adding tag to container relationship
    super.applyTags(container);
    if (container.getDataModel() != null) {
      applyColumnTags(container.getDataModel().getColumns());
    }
  }

  @Override
  public void validateTags(Container container) {
    super.validateTags(container);
    if (container.getDataModel() != null) {
      validateColumnTags(container.getDataModel().getColumns());
    }
  }

  @Override
  public EntityInterface getParentEntity(Container entity, String fields) {
    return Entity.getEntity(entity.getService(), fields, Include.ALL);
  }

  @Override
  public List<TagLabel> getAllTags(EntityInterface entity) {
    List<TagLabel> allTags = new ArrayList<>();
    Container container = (Container) entity;
    EntityUtil.mergeTags(allTags, container.getTags());
    if (container.getDataModel() != null) {
      for (Column column : listOrEmpty(container.getDataModel().getColumns())) {
        EntityUtil.mergeTags(allTags, column.getTags());
      }
    }
    return allTags;
  }

  @Override
  public TaskWorkflow getTaskWorkflow(ThreadContext threadContext) {
    validateTaskThread(threadContext);
    EntityLink entityLink = threadContext.getAbout();
    if (entityLink.getFieldName().equals("dataModel")) {
      TaskType taskType = threadContext.getThread().getTask().getType();
      if (EntityUtil.isDescriptionTask(taskType)) {
        return new DataModelDescriptionTaskWorkflow(threadContext);
      } else if (EntityUtil.isTagTask(taskType)) {
        return new DataModelTagTaskWorkflow(threadContext);
      } else {
        throw new IllegalArgumentException(String.format("Invalid task type %s", taskType));
      }
    }
    return super.getTaskWorkflow(threadContext);
  }

  static class DataModelDescriptionTaskWorkflow extends DescriptionTaskWorkflow {
    private final Column column;

    DataModelDescriptionTaskWorkflow(ThreadContext threadContext) {
      super(threadContext);
      DashboardDataModel dataModel =
          Entity.getEntity(
              DASHBOARD_DATA_MODEL, threadContext.getAboutEntity().getId(), "dataModel", ALL);
      threadContext.setAboutEntity(dataModel);
      column = EntityUtil.findColumn(dataModel.getColumns(), getAbout().getArrayFieldName());
    }

    @Override
    public EntityInterface performTask(String user, ResolveTask resolveTask) {
      column.setDescription(resolveTask.getNewValue());
      return threadContext.getAboutEntity();
    }
  }

  static class DataModelTagTaskWorkflow extends TagTaskWorkflow {
    private final Column column;

    DataModelTagTaskWorkflow(ThreadContext threadContext) {
      super(threadContext);
      DashboardDataModel dataModel =
          Entity.getEntity(
              DASHBOARD_DATA_MODEL, threadContext.getAboutEntity().getId(), "dataModel,tags", ALL);
      threadContext.setAboutEntity(dataModel);
      column = EntityUtil.findColumn(dataModel.getColumns(), getAbout().getArrayFieldName());
    }

    @Override
    public EntityInterface performTask(String user, ResolveTask resolveTask) {
      List<TagLabel> tags = JsonUtils.readObjects(resolveTask.getNewValue(), TagLabel.class);
      column.setTags(tags);
      return threadContext.getAboutEntity();
    }
  }

  /** Handles entity updated from PUT and POST operations */
  public class ContainerUpdater extends ColumnEntityUpdater {
    public ContainerUpdater(Container original, Container updated, Operation operation) {
      super(original, updated, operation);
    }

    @Transaction
    @Override
    public void entitySpecificUpdate() {
      updateDataModel(original, updated);
      recordChange("prefix", original.getPrefix(), updated.getPrefix());
      List<ContainerFileFormat> addedItems = new ArrayList<>();
      List<ContainerFileFormat> deletedItems = new ArrayList<>();
      recordListChange(
          "fileFormats",
          original.getFileFormats(),
          updated.getFileFormats(),
          addedItems,
          deletedItems,
          EntityUtil.containerFileFormatMatch);

      // record the changes for size and numOfObjects change without version update.
      recordChange(
          "numberOfObjects",
          original.getNumberOfObjects(),
          updated.getNumberOfObjects(),
          false,
          EntityUtil.objectMatch,
          false);
      recordChange(
          "size", original.getSize(), updated.getSize(), false, EntityUtil.objectMatch, false);
      recordChange("sourceUrl", original.getSourceUrl(), updated.getSourceUrl());
      recordChange("fullPath", original.getFullPath(), updated.getFullPath());
      recordChange("retentionPeriod", original.getRetentionPeriod(), updated.getRetentionPeriod());
      recordChange("sourceHash", original.getSourceHash(), updated.getSourceHash());
    }

    private void updateDataModel(Container original, Container updated) {
      if (original.getDataModel() == null || updated.getDataModel() == null) {
        recordChange("dataModel", original.getDataModel(), updated.getDataModel(), true);
      }

      if (original.getDataModel() != null && updated.getDataModel() != null) {
        updateColumns(
            "dataModel.columns",
            original.getDataModel().getColumns(),
            updated.getDataModel().getColumns(),
            EntityUtil.columnMatch);
        recordChange(
            "dataModel.partition",
            original.getDataModel().getIsPartitioned(),
            updated.getDataModel().getIsPartitioned());
      }
    }
  }
}
