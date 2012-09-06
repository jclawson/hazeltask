package com.hazeltask.config;

public class Validator {
    public static void validate(BundlerConfig<?> config) {
        if(!config.getBatchKeyAdapter().isConsistent()
            && config.isPreventDuplicates()) {
            throw new IllegalStateException("BundlerConfig cannot have prevent duplicates and have an inconsistent BatchKeyAdapter");
        }
    }
}