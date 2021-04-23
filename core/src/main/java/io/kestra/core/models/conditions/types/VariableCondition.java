package io.kestra.core.models.conditions.types;

import io.kestra.core.exceptions.InternalException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.Condition;
import io.kestra.core.models.conditions.ConditionContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Condition based on variables expression",
    description = "If the result is an empty string, a string containing only space or `false`, the condition will be false"
)
@Plugin(
    examples = {
        @Example(
            title = "A condition that will return false for a missing variable",
            full = true,
            code = {
                "- conditions:",
                "    - type: io.kestra.core.models.conditions.types.VariableCondition",
                "      expression: {{ and unknown }}",
            }
        )
    }
)
public class VariableCondition extends Condition {
    @NotNull
    public String expression;

    @Override
    public boolean test(ConditionContext conditionContext) throws InternalException {
        String render = conditionContext.getRunContext().render(expression);
        return !(render.isBlank() || render.isEmpty() || render.trim().equals("false"));
    }
}
