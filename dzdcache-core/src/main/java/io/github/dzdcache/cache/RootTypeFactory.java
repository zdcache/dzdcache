package io.github.dzdcache.cache;

public interface RootTypeFactory {
    RootType<?> create(TypeDescriptor descriptor);
}
