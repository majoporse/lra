package io.narayana.lra.client;

import java.util.NoSuchElementException;
import java.util.UUID;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

public class ParticipantIdProvider {
    private static String participantId;
    private static final Config CONFIG = ConfigProvider.getConfig();
    public static final String LRA_INSTANCE_ID_KEY = "lra.client.participantId";

    private static void Init() {
        try {
            participantId = CONFIG.getValue(LRA_INSTANCE_ID_KEY, String.class);
        } catch (NoSuchElementException e) {
            participantId = UUID.randomUUID().toString();
        }
    }

    public static String getInctanceId() {
        if (participantId == null) {
            Init();
        }
        return participantId;
    }

    public static String getParticipantId(Class<?> resourceClass) {
        return getInctanceId() + ": " + resourceClass.getName();
    }
}
