package it.luca.lgd.utils;

import java.util.Optional;
import java.util.function.Function;

public class Java8Utils {

    /**
     * Applies function to input object if not null, or returns else value
     * @param input: input object
     * @param trFunction: function to be applied to input object
     * @param elseValue: value to be returned if input object is null
     * @param <T>: input object type
     * @param <R>: return object type
     * @return input object trasnformed by function if not null, else value otherwise
     */

    public static <T, R> R orElse(T input, Function<T, R> trFunction, R elseValue) {

        return Optional.ofNullable(input)
                .map(trFunction)
                .orElse(elseValue);
    }

    /**
     * Applies function to input object if not null
     * @param input: input object
     * @param trFunction: function to be applied to input object
     * @param <T>: input object type
     * @param <R>: return object type
     * @return input object trasnformed by function if not null, else null
     */

    public static <T, R> R orNull(T input, Function<T, R> trFunction) {

        return orElse(input, trFunction, null);
    }
}
