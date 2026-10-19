package io.narayana.lra.callbacks;

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
}
