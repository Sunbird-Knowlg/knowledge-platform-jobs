package org.sunbird.incredible.processor;

import java.util.Map;
import org.slf4j.MDC;

public class BaseLogger {

    /**
     * sets requestId in Sl4j MDC
     *
     * @param trace
     */
    public void setReqId(Map<String, Object> trace) {
        MDC.clear();
        MDC.put(JsonKey.REQUEST_MESSAGE_ID, (String) trace.get(JsonKey.REQUEST_MESSAGE_ID));
    }

    public String getReqId() {
        return MDC.get(JsonKey.REQUEST_MESSAGE_ID);
    }
}
