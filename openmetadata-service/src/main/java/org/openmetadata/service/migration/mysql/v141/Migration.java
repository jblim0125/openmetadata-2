package org.openmetadata.service.migration.mysql.v141;

import lombok.SneakyThrows;
import org.openmetadata.service.migration.api.MigrationProcessImpl;
import org.openmetadata.service.migration.utils.MigrationFile;

import java.util.UUID;

import static org.openmetadata.service.migration.utils.v141.MigrationUtil.*;

public class Migration extends MigrationProcessImpl {

  public Migration(MigrationFile migrationFile) {
    super(migrationFile);
  }

  @Override
  @SneakyThrows
  public void runDataMigration() {
    // Migrate classification
    UUID classificationID = migrateClassification(collectionDAO);
    if( classificationID == null) {
      throw new RuntimeException("Failed to migrate classification");
    }
    // Migrate tag
    UUID tagID = migrationTag(collectionDAO);
    if( tagID == null) {
      throw new RuntimeException("Failed to migrate tag");
    }
    // Migrate relationship
    addRelationship(collectionDAO, classificationID, tagID);
  }
}
