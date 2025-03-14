package org.qubership.cloud.mongoevolution;

import org.qubership.cloud.mongoevolution.java.annotation.DBManagerEntity;
import lombok.Data;
import org.springframework.data.mongodb.core.MongoTemplate;

@Data
public class SpringDBManagerEntity extends DBManagerEntity {
    private MongoTemplate mongoTemplate;
}
