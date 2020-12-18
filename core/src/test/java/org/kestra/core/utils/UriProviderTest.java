package org.kestra.core.utils;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.flows.Flow;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@MicronautTest
class UriProviderTest {
    @Inject
    UriProvider uriProvider;

    @Test
    void root() {
        assertThat(uriProvider.rootUrl().toString(), containsString("mysuperhost.com/subpath/"));
    }

    @Test
    void flowUrl() {
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());

        assertThat(uriProvider.executionUrl(execution).toString(), containsString("mysuperhost.com/subpath/ui"));
        assertThat(uriProvider.flowUrl(execution).toString(), containsString(flow.getNamespace() + "/" + flow.getId()));

        assertThat(uriProvider.executionUrl(execution).toString(), containsString("mysuperhost.com/subpath/ui"));
        assertThat(uriProvider.flowUrl(flow).toString(), containsString(flow.getNamespace() + "/" + flow.getId()));
    }

    @Test
    void executionUrl() {
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());

        assertThat(uriProvider.executionUrl(execution).toString(), containsString("mysuperhost.com/subpath/ui"));
        assertThat(uriProvider.executionUrl(execution).toString(), containsString(flow.getNamespace() + "/" + flow.getId() + "/" + execution.getId()));
    }
}