/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra;

import io.narayana.lra.logging.LRALogger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.ConfigProvider;

public final class LRAConstants {
    // the scheme of the LRA protocol URI, e.g. lra://{lraId}
    public static final String LRA_SCHEME = "lra";
    // the pathname part of the URI of the coordinator
    // warning: the WildFly subsystem uses lra-coordinator/lra-coordinator
    public static final String COORDINATOR_PATH_NAME = "lra-coordinator";
    public static final String RECOVERY_COORDINATOR_PATH_NAME = "recovery";
    public static final String NESTED_COORDINATOR_PATH_NAME = "nested";

    public static final String COMPLETE = "complete";
    public static final String COMPENSATE = "compensate";
    public static final String STATUS = "status";
    public static final String LEAVE = "leave";
    public static final String AFTER = "after";
    public static final String FORGET = "forget";

    public static final String STATUS_PARAM_NAME = "Status";
    public static final String CLIENT_ID_PARAM_NAME = "ClientID";
    public static final String TIMELIMIT_PARAM_NAME = "TimeLimit";
    public static final String PARENT_LRA_PARAM_NAME = "ParentLRA";
    public static final String QUERY_PAIR_SEPARATOR = "&"; // separator to isolate each "key=value" pair of a URI query component
    public static final String QUERY_FIELD_SEPARATOR = "="; // separator to pick out the key and value of each pair
    public static final String RECOVERY_PARAM = "recoveryCount";
    public static final String HTTP_METHOD_NAME = "method"; // the name of the HTTP method used to invoke participants

    public static final String API_VERSION_1_0 = "1.0";
    public static final String API_VERSION_1_1 = "1.1";
    public static final String API_VERSION_1_2 = "1.2";
    public static final String API_VERSION_1_3 = "1.3";
    public static final String API_VERSION_2_0 = "2.0";

    /*
     * Supported Narayana LRA API versions.
     * Any version not mentioned in the list of the supported versions is unsupported
     * and will fail on incoming HTTP call.
     * The valid version format is specified in API.adoc document.
     * When a new version with new features is added consider adding API tests under 'test/basic'.
     *
     * Version history:
     * 1.0 - initial version
     * 1.1 - participant data support
     * 1.2 - participant link header support
     * 1.3 - nested LRA endpoints extracted into NestedCoordinator sub-resource;
     * nested status returns 200/410 per @Status;
     * nested forget uses HTTP DELETE per @Forget and returns 200/410
     * 2.0 - close/cancel return 202 for transitional states (Closing/Cancelling)
     * and 503 for transient failures (lock contention/store failure);
     * nested complete/compensate return 200/202/409 per @Complete/@Compensate
     * (pre-2.0 always returns 200 for backward compatibility);
     * opt-in only, planned as default for a future release
     */
    public static final String[] NARAYANA_LRA_API_SUPPORTED_VERSIONS = new String[] {
            API_VERSION_1_0,
            API_VERSION_1_1,
            API_VERSION_1_2,
            API_VERSION_1_3,
            API_VERSION_2_0
    };

    /**
     * The Narayana API version for LRA coordinator supported for the release.
     * Any higher version is considered as unimplemented and unknown.
     */
    public static final String CURRENT_API_VERSION_STRING = API_VERSION_1_3;

    public static final String NARAYANA_LRA_API_VERSION_HEADER_NAME = "Narayana-LRA-API-version";

    public static final String NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME = "Narayana-LRA-Participant-Data";
    public static final String NARAYANA_LRA_PARTICIPANT_LINK_HEADER_NAME = "Narayana-LRA-Participant-Link";

    /**
     * Number of seconds to wait for requests to participant.
     * The timeout is hardcoded as the protocol expects retry in case of failure and timeout.
     */
    public static final long PARTICIPANT_TIMEOUT = 2;
    public static final String ALLOW_PARTICIPANT_DATA = "lra.participant.data";

    public static final String HTTP_CLIENT_PROVIDERS = "lra.http-client.providers";
    public static final String BEARER_TOKEN_PROPERTY = "lra.jwt.token";

    public static final String SERVICE_TOKEN_LOCATION = "lra.security.service-token.location";
    public static final String SERVICE_TOKEN_REFRESH_SECONDS = "lra.security.service-token.refresh-seconds";

    /**
     * Number of milliseconds for the coordinator to hold a lock for enlisting participants. Defaults to 500.
     */
    public static final String ENLIST_PARTICIPANT_LOCK_TIMEOUT = "lra.participant.lock.timeout";

    /**
     * Numbers of times a client participant tries to enlist with the coordinator before giving up. Defaults to 3.
     */
    public static final String ENLIST_PARTICIPANT_CLIENT_MAX_RETRY = "lra.participant.client.max.retry";

    /**
     * Custom base URI if the client needs to override default base URI info taken from the Jakarta REST filters.
     */
    public static final String NARAYANA_LRA_BASE_URI_PROPERTY_NAME = "narayana.lra.base-uri";

    /**
     * When set to {@code true} the server side LRA processing is handled by the CDI interceptor
     * ({@code @LRA.Transaction} beans) instead of the legacy JAX-RS {@code ServerLRAFilter}.
     * Defaults to {@code false}, ie the legacy filter is used.
     */
    public static final String SERVER_FILTER_INTERCEPTOR_PROPERTY_NAME = "lra.server.filter.interceptor";

    /**
     * @return whether the CDI interceptor (instead of the legacy JAX-RS filter) owns the server
     *         side LRA processing: {@value #SERVER_FILTER_INTERCEPTOR_PROPERTY_NAME}, defaulting to
     *         {@code false}
     */
    public static boolean isInterceptorMode() {
        try {
            return ConfigProvider.getConfig()
                    .getOptionalValue(SERVER_FILTER_INTERCEPTOR_PROPERTY_NAME, Boolean.class)
                    .orElse(false);
        } catch (Exception e) {
            // no MicroProfile Config provider is installed; system properties are the lowest
            // common denominator
            return Boolean.parseBoolean(System.getProperty(SERVER_FILTER_INTERCEPTOR_PROPERTY_NAME, "false"));
        }
    }

    /**
     * @return the base URI used to build participant callback URIs (the address the LRA
     *         coordinator can use to reach this application), or empty when not set
     */
    public static Optional<String> getBaseUri() {
        try {
            return ConfigProvider.getConfig()
                    .getOptionalValue(NARAYANA_LRA_BASE_URI_PROPERTY_NAME, String.class);
        } catch (Exception e) {
            // no MicroProfile Config provider is installed; system properties are the lowest
            // common denominator
            return Optional.ofNullable(System.getProperty(NARAYANA_LRA_BASE_URI_PROPERTY_NAME));
        }
    }

    private static final Pattern UID_REGEXP_EXTRACT_MATCHER = Pattern.compile(".*/([^/?]+).*");

    private LRAConstants() {
        // utility class
    }

    /**
     * Extract the uid part from an LRA id.
     *
     * @param lraId LRA id to extract the uid from
     * @return uid of LRA
     */
    public static String getLRAUid(String lraId) {
        return lraId == null ? null : UID_REGEXP_EXTRACT_MATCHER.matcher(lraId).replaceFirst("$1");
    }

    /**
     * Extract the uid part from an LRA id.
     *
     * @param lraId LRA id to extract the uid from
     * @return uid of LRA
     */
    public static String getLRAUid(URI lraId) {
        if (lraId == null)
            return null;

        // the LRA protocol form (lra://{uid}) carries the uid in the authority
        if (LRA_SCHEME.equals(lraId.getScheme())) {
            String authority = lraId.getAuthority();
            if (authority != null && !authority.isEmpty()) {
                return authority;
            }
        }
        return null;
    }

    /**
     * Form the LRA protocol URI ({@code lra://{uid}}) for the given LRA uid.
     *
     * @param lraId the uid of the LRA
     * @return LRA id in the LRA protocol form
     */
    public static URI toLRAId(UUID lraId) {
        return lraId == null ? null : URI.create(LRA_SCHEME + "://" + lraId);
    }

    /**
     * Extract the uid (as a {@link UUID}) from an LRA id.
     *
     * @param lraId the LRA id to extract the uid from
     * @return the uid of the LRA, or null if the LRA id does not carry one
     */
    public static UUID toUuid(URI lraId) {
        String uid = getLRAUid(lraId);
        return uid == null ? null : UUID.fromString(uid);
    }

    /*
     * Extract the coordinator URL from the provided LRA id.
     *
     * @implNote Narayana LRA id is defined as an URL, e.g. {@code
     * http://localhost:8080/deployment/lra-coordinator/0_ffff0a28054b_9133_5f855916_a7}.
     * The LRA coordinator is available at {@code http://localhost:8080/deployment/lra-coordinator}
     * and the {@code 0_ffff0a28054b_9133_5f855916_a7} is the LRA uid.
     *
     * @param lraId LRA id to extract the LRA coordinator URI from
     *
     * @return LRA Coordinator URL
     *
     * @throws IllegalStateException if the LRA coordinator URL, extracted from the LRA id, is not assignable to URI
     */
    public static URI getLRACoordinatorUrl(URI lraId) {
        if (lraId == null)
            return null;
        String lraIdPath = lraId.getPath();
        int coordinatorIndex = lraIdPath.lastIndexOf(COORDINATOR_PATH_NAME);
        if (coordinatorIndex < 0) {
            String errMsg = LRALogger.i18nLogger.warn_invalid_uri(lraId.toASCIIString(), "getLRACoordinatorUrl");
            throw new IllegalStateException(errMsg);
        }
        String lraCoordinatorPath = lraIdPath.substring(0, coordinatorIndex)
                + COORDINATOR_PATH_NAME;
        try {
            return new URI(lraId.getScheme(), lraId.getUserInfo(), lraId.getHost(), lraId.getPort(), lraCoordinatorPath,
                    null, null);
        } catch (URISyntaxException use) {
            String errMsg = LRALogger.i18nLogger.warn_invalid_uri(lraId.toASCIIString(), "getLRACoordinatorUrl");
            throw new IllegalStateException(errMsg);
        }
    }
}
