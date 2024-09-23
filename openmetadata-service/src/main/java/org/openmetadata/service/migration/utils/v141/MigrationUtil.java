package org.openmetadata.service.migration.utils.v141;

import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.entity.classification.Classification;
import org.openmetadata.schema.entity.classification.Tag;
import org.openmetadata.schema.type.EntityRelationship;
import org.openmetadata.schema.type.ProviderType;
import org.openmetadata.service.exception.EntityNotFoundException;
import org.openmetadata.service.jdbi3.CollectionDAO;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class MigrationUtil {

  private MigrationUtil() {
    /* Cannot create object  util class*/
  }

  public static UUID migrateClassification(CollectionDAO collectionDAO) {
    Classification classification = null;
    try {
      classification =
              collectionDAO.classificationDAO().findEntityByName("ovp_category");
    } catch (EntityNotFoundException ignored) {
      LOG.warn("Classification ovp_category not found");
    }
    try {
      if (classification == null) {
        Classification ovp = new Classification()
                .withId(UUID.randomUUID())
                .withName("ovp_category")
                .withDisplayName("ovp_category")
                .withDescription("카테고리")
                .withFullyQualifiedName("ovp_category")
                .withVersion(0.1)
                .withProvider(ProviderType.SYSTEM)
                .withUpdatedAt(System.currentTimeMillis())
                .withMutuallyExclusive(false)
                .withUpdatedBy("system");
        collectionDAO.classificationDAO().insert(ovp, "ovp_category");
        LOG.info("Inserted classification ovp_category");
        return ovp.getId();
      } else {
        LOG.info("Find Classification ovp_category");
        return classification.getId();
      }
    } catch (Exception ex) {
      LOG.warn("Error running the classification migration ", ex);
    }
    return null;
  }
  public static UUID migrationTag(CollectionDAO collectionDAO) {
    Tag tag = null;
    try {
      tag = collectionDAO.tagDAO().findEntityByName("ovp_category.미분류");
    } catch (EntityNotFoundException ignored) {
      LOG.warn("Tag unknow(미분류) not found");
    }
    try {
      if( tag == null ) {
        Tag unknow = new Tag()
                .withId(UUID.randomUUID())
                .withName("미분류")
                .withDisplayName("미분류")
                .withDescription("미분류")
                .withFullyQualifiedName("ovp_category.미분류")
                .withVersion(0.1)
                .withProvider(ProviderType.SYSTEM)
                .withUpdatedAt(System.currentTimeMillis())
                .withMutuallyExclusive(false)
                .withUpdatedBy("system");
        collectionDAO.tagDAO().insert(unknow, "ovp_category.미분류");
        LOG.info("Inserted tag unknow(미분류)");
        return unknow.getId();
      } else {
        LOG.info("Find Tag unknow(미분류)");
        return tag.getId();
      }
    } catch (Exception ex) {
      LOG.warn("Error running the tag migration ", ex);
    }
    return null;
  }
  public static void addRelationship(CollectionDAO collectionDAO, UUID classificationId, UUID tagId) {
    try {
      List<CollectionDAO.EntityRelationshipRecord> res = collectionDAO.relationshipDAO().findFrom(tagId, "tag", 0, "classification");
      if(!res.isEmpty()) {
        LOG.info("Relationship between classification and tag already exists");
        return;
      }
    } catch (EntityNotFoundException ignored) {
      LOG.warn("Relationship between classification and tag not found");
    }
    try {
      collectionDAO.relationshipDAO().insert(classificationId, tagId, "classification", "tag", 0);
      LOG.info("Added relationship between classification and tag");
    } catch (Exception ex) {
      LOG.warn("Error running the relationship migration ", ex);
    }
  }
}
