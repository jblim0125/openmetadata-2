package org.openmetadata.service.migration.mysql.v141;

import lombok.SneakyThrows;
import org.openmetadata.service.migration.api.MigrationProcessImpl;
import org.openmetadata.service.migration.utils.MigrationFile;

import static org.openmetadata.service.migration.utils.v141.MigrationUtil.*;

public class Migration extends MigrationProcessImpl {

  public Migration(MigrationFile migrationFile) {
    super(migrationFile);
  }

  @Override
  @SneakyThrows
  public void runDataMigration() {
    // Migrate classification
    migrateClassification(collectionDAO);
    // Migrate tag
    migrationTag(collectionDAO);
  }
}
