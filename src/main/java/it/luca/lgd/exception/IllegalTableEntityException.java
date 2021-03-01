package it.luca.lgd.exception;

import javax.persistence.Table;

public class IllegalTableEntityException extends RuntimeException {

    public IllegalTableEntityException(Class<?> clazz, String undefinedAnnotationElement) {

        super(String.format("Class %s is annotated with %s, but element '%s' is not defined",
                clazz.getName(), Table.class.getName(), undefinedAnnotationElement));
    }
}
