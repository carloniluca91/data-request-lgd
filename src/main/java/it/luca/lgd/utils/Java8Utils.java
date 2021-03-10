package it.luca.lgd.utils;

import java.util.Optional;
import java.util.function.Function;

public class Java8Utils {

    public static <T, R> R orElse(T input, Function<T, R> trFunction, R elseValue) {

        return Optional.ofNullable(input)
                .map(trFunction)
                .orElse(elseValue);
    }

    public static <T, R> R orNull(T input, Function<T, R> trFunction) {

        return orElse(input, trFunction, null);
    }
}
