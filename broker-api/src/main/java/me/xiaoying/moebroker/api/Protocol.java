package me.xiaoying.moebroker.api;

public interface Protocol {
    void oneway(Object object);

    default <T> T invokeSync(Object object) {
        return this.invokeSync(object, 3000);
    }

    <T> T invokeSync(Object object, long timeoutMillis);
}