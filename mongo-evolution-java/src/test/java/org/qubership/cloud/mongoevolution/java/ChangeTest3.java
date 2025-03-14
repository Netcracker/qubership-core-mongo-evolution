package org.qubership.cloud.mongoevolution.java;

import org.qubership.cloud.mongoevolution.java.annotation.ChangeLog;
import org.qubership.cloud.mongoevolution.java.annotation.ChangeSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChangeLog(version = 3)
public class ChangeTest3 {

    @ChangeSet(order = 2)
    public void someChange() {
        log.debug("@ChangeSet Empty args");
    }
}
