package org.apache.rocketmq.tieredstore.provider;

import javax.annotation.Nonnull;

public class ClientInstance<T> {
    @Nonnull
    private final T client;
    private final int index;

    public ClientInstance(@Nonnull T client, int index) {
        this.client = client;
        this.index = index;
    }

    @Nonnull
    public T getClient() {
        return client;
    }

    public int getIndex() {
        return index;
    }
}
