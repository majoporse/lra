/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The one lookup table of callback deliveries, indexed by the polymorphic address class.
 * <p>
 * The coordinator is constructed manually, so the registry is mutable: the HTTP adapter
 * registers the HTTP deliveries and the Kafka adapter registers the Kafka delivery. A duplicate
 * address type is rejected at registration time.
 */
@ApplicationScoped
public class DeliveryRegistry {

    @Inject
    Instance<CallbackDelivery> deliveries;

    private final Map<Class<? extends CallbackAddress>, CallbackDelivery<?>> byAddressType = new ConcurrentHashMap<>();

    public DeliveryRegistry() {
    }

    public DeliveryRegistry(Collection<CallbackDelivery<?>> deliveries) {
        deliveries.forEach(this::register);
    }

    @PostConstruct
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void index() {
        for (CallbackDelivery<?> delivery : (Iterable<CallbackDelivery<?>>) (Iterable) deliveries) {
            register(delivery);
        }
    }

    public synchronized void register(CallbackDelivery<?> delivery) {
        Class<? extends CallbackAddress> addressType = delivery.addressType();
        CallbackDelivery<?> previous = byAddressType.putIfAbsent(addressType, delivery);

        if (previous != null) {
            throw new IllegalStateException("Duplicate callback delivery for address type '"
                    + addressType.getSimpleName() + "': " + previous.getClass().getName() + " and "
                    + delivery.getClass().getName());
        }
    }

    /**
     * @param address the callback address
     * @return the enabled delivery for that address subtype
     * @throws NoCallbackDeliveryException when no delivery is registered or it is disabled
     */
    @SuppressWarnings("unchecked")
    public <A extends CallbackAddress> CallbackDelivery<A> forAddress(A address) {
        CallbackDelivery<?> delivery = byAddressType.get(address.getClass());

        if (delivery == null) {
            throw new NoCallbackDeliveryException(address.getClass(), byAddressType.keySet(), false);
        }

        if (!delivery.isEnabled()) {
            throw new NoCallbackDeliveryException(address.getClass(), byAddressType.keySet(), true);
        }

        return (CallbackDelivery<A>) delivery;
    }

    public Set<Class<? extends CallbackAddress>> addressTypes() {
        return byAddressType.keySet();
    }
}
