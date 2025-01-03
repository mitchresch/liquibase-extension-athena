package liquibase.ext.athena.configuration;

import liquibase.configuration.ConfigurationDefinition;
import liquibase.configuration.AutoloadedConfigurations;

public class AthenaConfiguration implements AutoloadedConfigurations {

    public static final ConfigurationDefinition<String> LIQUIBASE_S3_TABLE_LOCATION;

    static {
        ConfigurationDefinition.Builder builder = new ConfigurationDefinition.Builder("liquibase.athena");

        LIQUIBASE_S3_TABLE_LOCATION = builder.define("liquibaseS3TableLocation", String.class)
            .setDescription("S3 Location where Liquibase Iceberg Tables will be stored")
            .build();
    }

    public static String getS3TableLocation() {
        return LIQUIBASE_S3_TABLE_LOCATION.getCurrentValue();
    }
}
