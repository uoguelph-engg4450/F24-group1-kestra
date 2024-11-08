package io.kestra.core.validations;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.plugin.core.condition.AdvancedExecutionsCondition;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class PreconditionFilterValidationTest {
    @Inject
    private ModelValidator modelValidator;

    @ParameterizedTest
    @EnumSource(value = AdvancedExecutionsCondition.Type.class, names = {"EQUAL_TO", "NOT_EQUAL_TO", "IS_NULL", "IS_NOT_NULL", "IS_TRUE", "IS_FALSE", "STARTS_WITH", "ENDS_WITH", "REGEX", "CONTAINS"})
    void shouldValidateConditionWithAValue(AdvancedExecutionsCondition.Type type) {
        var condition = AdvancedExecutionsCondition.Filter.builder()
            .field(AdvancedExecutionsCondition.Field.FLOW_ID)
            .type(type)
            .value("myFlow")
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(condition);
        assertThat(valid.isEmpty(), is(true));
    }

    @ParameterizedTest
    @EnumSource(value = AdvancedExecutionsCondition.Type.class, names = {"EQUAL_TO", "NOT_EQUAL_TO", "IS_NULL", "IS_NOT_NULL", "IS_TRUE", "IS_FALSE", "STARTS_WITH", "ENDS_WITH", "REGEX", "CONTAINS"})
    void shouldNotValidateConditionWithValues(AdvancedExecutionsCondition.Type type) {
        var condition = AdvancedExecutionsCondition.Filter.builder()
            .field(AdvancedExecutionsCondition.Field.FLOW_ID)
            .type(type)
            .values(List.of("myFlow1", "myFlow2"))
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(condition);
        assertThat(valid.isEmpty(), is(false));
        assertThat(valid.get().getConstraintViolations(), hasSize(1));
        assertThat(valid.get().getMessage(), is(": `value` cannot be null for type " + type.name() + "\n"));
    }

    @ParameterizedTest
    @EnumSource(value = AdvancedExecutionsCondition.Type.class, names = {"EQUAL_TO", "NOT_EQUAL_TO", "IS_NULL", "IS_NOT_NULL", "IS_TRUE", "IS_FALSE", "STARTS_WITH", "ENDS_WITH", "REGEX", "CONTAINS"})
    void shouldNotValidateConditionWithAValueAndValues(AdvancedExecutionsCondition.Type type) {
        var condition = AdvancedExecutionsCondition.Filter.builder()
            .field(AdvancedExecutionsCondition.Field.FLOW_ID)
            .type(type)
            .value("myFlow")
            .values(List.of("myFlow1", "myFlow2"))
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(condition);
        assertThat(valid.isEmpty(), is(false));
        assertThat(valid.get().getConstraintViolations(), hasSize(1));
        assertThat(valid.get().getMessage(), is(": `values` must be null for type " + type.name() + "\n"));
    }

    @ParameterizedTest
    @EnumSource(value = AdvancedExecutionsCondition.Type.class, names = {"IN", "NOT_IN"})
    void shouldValidateConditionWithValues(AdvancedExecutionsCondition.Type type) {
        var condition = AdvancedExecutionsCondition.Filter.builder()
            .field(AdvancedExecutionsCondition.Field.FLOW_ID)
            .type(type)
            .values(List.of("myFlow1", "myFlow2"))
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(condition);
        assertThat(valid.isEmpty(), is(true));
    }

    @ParameterizedTest
    @EnumSource(value = AdvancedExecutionsCondition.Type.class, names = {"IN", "NOT_IN"})
    void shouldNotValidateConditionWithAValue(AdvancedExecutionsCondition.Type type) {
        var condition = AdvancedExecutionsCondition.Filter.builder()
            .field(AdvancedExecutionsCondition.Field.FLOW_ID)
            .type(type)
            .value("myFlow1")
            .build();

        Optional<ConstraintViolationException> valid = modelValidator.isValid(condition);
        assertThat(valid.isEmpty(), is(false));
        assertThat(valid.get().getConstraintViolations(), hasSize(1));
        assertThat(valid.get().getMessage(), is(": `value` must be null for type " + type.name() + "\n"));
    }
}