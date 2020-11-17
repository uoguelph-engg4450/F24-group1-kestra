package org.kestra.core.runners.handlebars.helpers;

import com.github.jknack.handlebars.HandlebarsException;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;
import lombok.SneakyThrows;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.runners.VariableRenderer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FirstDefinedEvalHelper implements Helper<String> {
    private final VariableRenderer variableRenderer;

    public FirstDefinedEvalHelper(VariableRenderer variableRenderer) {
        this.variableRenderer = variableRenderer;
    }

    @SneakyThrows
    @Override
    public CharSequence apply(final String value, final Options options) {
        String result = null;

        List<String> params = Stream
            .concat(
                Stream.of(value),
                Arrays.stream(options.params).map(o -> (String) o)
            )
            .collect(Collectors.toList());


        int i = 0;
        while (result == null && i < params.size()) {
            try {
                String param = params.get(i++);
                String finalTemplate = variableRenderer.recursiveRender(param, options.context);

                result = variableRenderer.recursiveRender("{{" + finalTemplate + "}}", options.context);
            } catch (IllegalVariableEvaluationException | IllegalStateException | HandlebarsException ignored) {
            }
        }

        if (result == null) {
            throw new IllegalStateException("Unable to find any defined eval on '" + params + "'");
        }

        return result;
    }
}

