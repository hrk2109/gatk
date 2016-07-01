package org.broadinstitute.hellbender.cmdline.GATKPlugin;
// NOTE: this class needs to live in the plugin package since it contains a test
// plugin that must live there.

import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineParser;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.QCProgramGroup;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Test basic command line parser plugin functionality. Because fully testing the plugin
 * functionality requires implementations of multiple classes and subclasses, the unit tests for
 * GATKReadFilterPlugin ReadFilterPluginUnitTest have much more extensive much more coverage
 * of the plugin functionality since they use a real (ReadFilter) plugin class hierarchy.
 */
public class CommandLineParserPluginUnitTest {

    public static class TestPluginBase {
    }

    public static class TestPlugin extends TestPluginBase {
    }

    public static class TestPluginDescriptor extends GATKCommandLinePluginDescriptor<TestPluginBase> {

        @Argument(fullName="pluginName", optional = true)
        Set<String> pluginNames = new HashSet<>();

        // Map of plugin names to the corresponding instance
        public Map<String, TestPluginBase> pluginInstances = new HashMap<>();

        public TestPluginDescriptor() {}

        @Override
        public Class<?> getPluginClass() {
            return TestPluginBase.class;
        }

        @Override
        public String getPackageName() {
            return "org.broadinstitute.hellbender.cmdline.GATKPlugin";
        }

        @Override
        public Predicate<Class<?>> getClassFilter() {
            return c -> {
                // don't use the TestPlugin base class
                return !c.getName().equals(this.getPluginClass().getName());
            };
        }

        @Override
        public Object addInstance(Class<?> pluggableClass) throws IllegalAccessException, InstantiationException {
            final TestPluginBase plugin = (TestPluginBase) pluggableClass.newInstance();
            // use getSimpleName; the classes are all in the same package
            pluginInstances.put(pluggableClass.getSimpleName(), plugin);
            return plugin;
        }

        @Override
        public boolean isDependentArgumentAllowed(Class<?> targetPluginClass) {
            return true;
        }

        @Override
        public void validateArguments() {
            // remove the un-specified plugin instances
            Map<String, TestPluginBase> requestedPlugins = new HashMap<>();
            pluginNames.forEach(s -> {
                TestPluginBase trf = pluginInstances.get(s);
                if (null == trf) {
                    throw new UserException.CommandLineException("Unrecognized test plugin name: " + s);
                }
                else {
                    requestedPlugins.put(s, trf);
                }
            });
            pluginInstances = requestedPlugins;

            // now validate that each plugin specified is valid (has a corresponding instance)
            Assert.assertEquals(pluginNames.size(), pluginInstances.size());
        }

        @Override
        public void getInstances(Consumer<Collection<TestPluginBase>> consumer) {
            consumer.accept(pluginInstances.values());
        }
    }

    @CommandLineProgramProperties(
            summary = "Plugin Test",
            oneLineSummary = "Plugin test",
            programGroup = QCProgramGroup.class
    )
    public class PlugInTest {
    }

    @DataProvider(name="pluginTests")
    public Object[][] pluginTests() {
        return new Object[][]{
                {new String[0], 0},
                {new String[]{"--pluginName", TestPlugin.class.getSimpleName()}, 1}
        };
    }

    @Test(dataProvider = "pluginTests")
    public void testPlugin(final String[] args, final int expectedInstanceCount){

        PlugInTest plugInTest = new PlugInTest();
        final CommandLineParser clp = new CommandLineParser(
                plugInTest,
                Collections.singletonList(TestPluginDescriptor.class));

        Assert.assertTrue(clp.parseArguments(System.err, args));

        TestPluginDescriptor pid =
                (TestPluginDescriptor) clp.getPluginDescriptor(CommandLineParserPluginUnitTest.TestPluginDescriptor.class);
        Assert.assertNotNull(pid);

        List<TestPluginBase> pluginBases = new ArrayList<>();
        pid.getInstances(list -> pluginBases.addAll(list));

        Assert.assertEquals(pluginBases.size(), expectedInstanceCount);
    }

}
