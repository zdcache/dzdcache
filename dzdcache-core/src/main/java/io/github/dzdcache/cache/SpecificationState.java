package io.github.dzdcache.cache;

public record SpecificationState(SpecificationStatus status, TypeDescriptor change, String reason) { }
