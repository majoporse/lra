package io.narayana.lra.connectors.common.callbacks;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.narayana.lra.LRAConstants;

/**
 * The kinds of callbacks a participant can declare.
 */
public enum CallbackType {

    COMPENSATE(LRAConstants.COMPENSATE),
    COMPLETE(LRAConstants.COMPLETE),
    STATUS(LRAConstants.STATUS),
    FORGET(LRAConstants.FORGET),
    AFTER(LRAConstants.AFTER);

    private final String rel;

    CallbackType(String rel) {
        this.rel = rel;
    }

    /**
     * @return the LRA protocol path segment of this callback kind ({@code "complete"},
     *         {@code "compensate"}, ...)
     */
    public String rel() {
        return rel;
    }

    /**
     * Read a persisted operation. Kafka persisted the after-LRA callback as {@code AFTER_LRA};
     * the neutral name is {@code AFTER}.
     */
    @JsonCreator
    public static CallbackType from(String value) {
        if (value == null) {
            return null;
        }

        return "AFTER_LRA".equals(value) ? AFTER : valueOf(value);
    }
}
