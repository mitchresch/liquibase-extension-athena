package liquibase.ext.athena.configuration;

import liquibase.configuration.ConfigurationDefinition;
import liquibase.configuration.AutoloadedConfigurations;

public class AthenaConfiguration implements AutoloadedConfigurations {

    public static final ConfigurationDefinition<String> LIQUIBASE_TABLE_S3_LOCATION;

    static {
        ConfigurationDefinition.Builder builder = new ConfigurationDefinition.Builder("liquibase.athena");

        LIQUIBASE_TABLE_S3_LOCATION = builder.define("liquibaseTableS3Location", String.class)
            .setDescription("S3 Location where Liquibase Iceberg Tables will be stored")
            .build();
    }

    public static String getS3TableLocation() {
        return LIQUIBASE_TABLE_S3_LOCATION.getCurrentValue();
    }
}
