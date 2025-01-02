package liquibase.ext.athena.configuration;

import liquibase.configuration.ConfigurationDefinition;

public class AthenaConfiguration {
    public static final String LIQUIBASE_ATHENA_NAMESPACE = "liquibase.athena";
    public static final ConfigurationDefinition<String> LIQUIBASE_TABLE_S3_LOCATION;

    static {
        ConfigurationDefinition.Builder builder = new ConfigurationDefinition.Builder(LIQUIBASE_ATHENA_NAMESPACE);

        LIQUIBASE_TABLE_S3_LOCATION = builder.define("liquibaseTableS3Location", String.class)
            .setDescription("S3 Location where Liquibase Iceberg Tables will be stored")
            .build();
    }
}
