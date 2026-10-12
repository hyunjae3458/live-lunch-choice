package org.example.livelunchchoice.common.error;

import org.hibernate.exception.ConstraintViolationException;

import java.util.Locale;

public final class UniqueConstraintViolation {
    private UniqueConstraintViolation() {
    }

    public static boolean matches(Throwable error, String expectedName) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConstraintViolationException violation
                    && "23505".equals(violation.getSQLState())
                    && violation.getConstraintName() != null) {
                String name = violation.getConstraintName().toLowerCase(Locale.ROOT);
                name = name.substring(name.lastIndexOf('.') + 1);
                // H2의 생성된 인덱스 이름과 PostgreSQL의 제약조건 이름을 구분한다.
                if (name.equals(expectedName) || name.startsWith(expectedName + "_index_")) {
                    return true;
                }
            }
        }
        return false;
    }
}
