package io.narayana.lra.coordinator.domain.model.actions;

public class ActionResult {
    private final ActionStatus status;
    private final String body;
    private final LRAAction updatedStatusAction;

    public ActionResult(ActionStatus status, String body, LRAAction updatedStatusAction) {
        this.status = status;
        this.body = body;
        this.updatedStatusAction = updatedStatusAction;
    }

    public ActionResult(ActionStatus status, String body) {
        this(status, body, null);
    }

    public ActionResult(ActionStatus status) {
        this(status, null, null);
    }

    public ActionStatus getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    public LRAAction getUpdatedStatusAction() {
        return updatedStatusAction;
    }
}