package io.kestra.core.InfluxDBPluginTest;

import io.kestra.plugin.core.InfluxDB.InfluxDBPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class InfluxDBPluginTest {
    private InfluxDBPlugin influxDBPlugin;

    @BeforeEach
    void setUp() {
        influxDBPlugin = new InfluxDBPlugin();
    }

    @AfterEach
    void tearDown() {
        influxDBPlugin = null;
    }

    @Test
    void testPluginInitialization() {
        assertThat("InfluxDBPlugin should be initialized.", influxDBPlugin != null, is(true));
    }

    @ParameterizedTest
    @MethodSource("provideTestArguments")
    void testParameterizedBehavior(String url, String database, boolean expectedResult) {
        boolean result = influxDBPlugin.connect(url, database, "testUser", "testPassword");
        assertThat("InfluxDBPlugin parameterized test should pass.", result, is(expectedResult));
    }

    @ParameterizedTest
    @MethodSource("provideDataInsertionArguments")
    void testParameterizedDataInsertion(String measurement, String fields, boolean expectedResult) {
        boolean result = influxDBPlugin.insertData(measurement, fields);
        assertThat("InfluxDBPlugin data insertion test should pass.", result, is(expectedResult));
    }

    private static Stream<Arguments> provideTestArguments() {
        return Stream.of(
            Arguments.of("testUrl", "testDatabase", true),    // Valid parameters
            Arguments.of(null, "testDatabase", false),        // Null URL
            Arguments.of("testUrl", null, false),             // Null database
            Arguments.of("", "testDatabase", false),          // Empty URL
            Arguments.of("testUrl", "", false)                // Empty database
        );
    }

    private static Stream<Arguments> provideDataInsertionArguments() {
        return Stream.of(
            Arguments.of("testMeasurement", "field1=value1", true),   // Valid parameters
            Arguments.of(null, "field1=value1", false),               // Null measurement
            Arguments.of("testMeasurement", null, false),             // Null fields
            Arguments.of("", "field1=value1", false),                 // Empty measurement
            Arguments.of("testMeasurement", "", false)                // Empty fields
        );
    }
}
