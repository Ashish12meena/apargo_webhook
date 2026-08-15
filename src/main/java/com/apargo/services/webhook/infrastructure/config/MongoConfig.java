package com.apargo.services.webhook.infrastructure.config;

import com.apargo.services.webhook.infrastructure.persistence.WebhookEventDocument;
import com.mongodb.WriteConcern;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoActionOperation;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteConcernResolver;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Mongo wiring, and the one durability guarantee this service exists to provide.
 *
 * <p>Mongo's default {@code {w: 1}} acknowledges as soon as the primary holds the write in memory,
 * before the journal flush. A primary crash in that window loses data already acknowledged to Meta —
 * and Meta offers no event log, no replay API and no dead-letter queue, so it is gone for good.
 * {@code majority} survives a failover, {@code j:true} survives a crash, and the cost is a few
 * milliseconds against a five second budget.
 */
@Configuration(proxyBeanMethods = false)
public class MongoConfig {

    /** {@code {w: "majority", j: true}}. Referenced by the adapter and asserted in tests. */
    public static final WriteConcern DURABLE_WRITE_CONCERN = WriteConcern.MAJORITY.withJournal(true);

    /**
     * Applies the durable write concern to every write against {@code webhook_events}, so it holds
     * even for a write path added later that forgets to ask for it.
     */
    @Bean
    public MongoTemplate mongoTemplate(MongoDatabaseFactory databaseFactory, MongoConverter converter) {
        MongoTemplate template = new MongoTemplate(databaseFactory, converter);
        template.setWriteConcernResolver(durableWriteConcernResolver());
        return template;
    }

    /**
     * Package-visible so the guarantee can be asserted directly in a unit test. Losing this is a
     * one-line regression that no functional test would catch.
     */
    static WriteConcernResolver durableWriteConcernResolver() {
        return action -> {
            if (WebhookEventDocument.COLLECTION.equals(action.getCollectionName())
                    && isWrite(action.getMongoActionOperation())) {
                return DURABLE_WRITE_CONCERN;
            }
            return action.getDefaultWriteConcern();
        };
    }

    /**
     * Drops the {@code _class} discriminator so stored documents match the documented schema exactly.
     * Every collection here maps to a single concrete type, so nothing needs the type hint.
     */
    @Bean
    public MappingMongoConverter mappingMongoConverter(
            MongoDatabaseFactory databaseFactory,
            MongoMappingContext mappingContext,
            MongoCustomConversions conversions) {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(databaseFactory);
        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);
        converter.setCustomConversions(conversions);
        converter.setCodecRegistryProvider(databaseFactory);
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        converter.afterPropertiesSet();
        return converter;
    }

    private static boolean isWrite(MongoActionOperation operation) {
        return operation == MongoActionOperation.INSERT
                || operation == MongoActionOperation.INSERT_LIST
                || operation == MongoActionOperation.SAVE
                || operation == MongoActionOperation.UPDATE
                || operation == MongoActionOperation.BULK;
    }
}
