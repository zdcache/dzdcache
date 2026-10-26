package io.github.dzdcache.spring;

import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheTypeTest {
    public record Student(long id, String name) { }
    public record Teacher(long id, String name) { }

    private static SearchSpecification<Student, Long> students() {
        return SearchSpecification.builder(IdentityDimension.of(Student.class, "id", Student::id)).prefix(Student::name).build();
    }

    @Test void aTypeIsNamedAfterItsFullyQualifiedClass() {
        assertThat(CacheType.of(Student.class, 1, Long.class, students()).name()).isEqualTo("io.github.dzdcache.spring.CacheTypeTest.Student");
    }

    @Test void theNameStaysTheSameForEveryInstanceThatDeclaresTheClass() {
        assertThat(CacheType.of(Student.class, 2, Long.class, students()).name()).isEqualTo(CacheType.of(Student.class, 1, Long.class, students()).name());
    }

    @Test void aSpecificationForAnotherClassIsRefused() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Student> teacher = (Class) Teacher.class;
        assertThatThrownBy(() -> CacheType.of(teacher, 1, Long.class, students()))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(Student.class.getName()).hasMessageContaining(Teacher.class.getName());
    }
}
