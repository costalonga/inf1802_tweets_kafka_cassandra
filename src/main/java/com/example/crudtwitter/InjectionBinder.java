package com.example.crudtwitter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

public class InjectionBinder extends AbstractBinder {
    @Override
    protected void configure() {
        // @TODO Altere para a sua classe que implementa LifecycleManager
        bind(ConsumerLifecycleManager.class)
                .to(LifecycleManager.class)
                .in(Singleton.class);
    }
}
