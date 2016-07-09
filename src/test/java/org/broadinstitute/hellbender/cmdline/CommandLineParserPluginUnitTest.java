package org.broadinstitute.hellbender.cmdline;

import org.broadinstitute.hellbender.cmdline.GATKPlugin.GATKCommandLinePluginDescriptor;
import org.broadinstitute.hellbender.cmdline.programgroups.QCProgramGroup;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Predicate;

/**
 * Test basic command line parser plugin functionality. Fully testing the plugin functionality requires
 * implementations of multiple classes and subclasses. The unit tests for GATKReadFilterPlugin
 * ReadFilterPluginUnitTest have much more extensive coverage of the plugin functionality since they
 * use a real (ReadFilter) plugin class hierarchy.
 */
public class CommandLineParserPluginUnitTest {

    public static class TestPluginBase {
    }

    public static class TestPlugin extends TestPluginBase {
        final static String argumentName = "argumentForTestPlugin";
        @Argument(fullName = argumentName, optional=true)
        Integer argumentForTestPlugin;
    }

    public static class TestPluginDescriptor extends GATKCommandLinePluginDescriptor<TestPluginBase> {

        final String pluginNamesArgName = "pluginName";

        @Argument(fullName=pluginNamesArgName, optional = true)
        Set<String> pluginNames = new HashSet<>();

        // Map of plugin names to the corresponding instance
        public Map<String, TestPluginBase> pluginInstances = new HashMap<>();

        public TestPluginDescriptor() {}

        @Override
        public Class<?> getPluginClass() {
            return TestPluginBase.class;
        }

        @Override
        public List<String> getPackageNames() {
            return Collections.singletonList("org.broadinstitute.hellbender.cmdline");
        }

        @Override
        public Predicate<Class<?>> getClassFilter() {
            return c -> {
                // don't use the TestPlugin base class
                return !c.getName().equals(this.getPluginClass().getName());
            };
        }

        @Override
        public Object getInstance(Class<?> pluggableClass) throws IllegalAccessException, InstantiationException {
            final TestPluginBase plugin = (TestPluginBase) pluggableClass.newInstance();
            pluginInstances.put(pluggableClass.getSimpleName(), plugin);
            return plugin;
        }

        @Override
        public Set<String> getAllowedValuesForDescriptorArgument(String longArgName) {
            if (longArgName.equals(pluginNamesArgName) ){
                return pluginInstances.keySet();
            }
            throw new IllegalArgumentException("Allowed values request for unrecognized string argument: " + longArgName);

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
        public List<TestPluginBase> getAllInstances() {
            List<TestPluginBase> pluginList = new ArrayList<>();
            pluginList.addAll(pluginInstances.values());
            return pluginList;
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
                Collections.singletonList(new TestPluginDescriptor()));

        Assert.assertTrue(clp.parseArguments(System.err, args));

        TestPluginDescriptor pid =
                (TestPluginDescriptor) clp.getPluginDescriptor(CommandLineParserPluginUnitTest.TestPluginDescriptor.class);
        Assert.assertNotNull(pid);

        List<TestPluginBase> pluginBases = pid.getAllInstances();

        Assert.assertEquals(pluginBases.size(), expectedInstanceCount);
    }

    @Test
    public void testPluginUsage() {
        PlugInTest plugInTest = new PlugInTest();
        final CommandLineParser clp = new CommandLineParser(
                plugInTest,
                Collections.singletonList(new TestPluginDescriptor()));
        final String out = BaseTest.captureStderr(() -> clp.usage(System.err, true)); // with common args

        TestPluginDescriptor pid =
                (TestPluginDescriptor) clp.getPluginDescriptor(CommandLineParserPluginUnitTest.TestPluginDescriptor.class);
        Assert.assertNotNull(pid);

        // Make sure TestPlugin.argumentName is listed as conditional
        final int condIndex = out.indexOf("Conditional Arguments:");
        Assert.assertTrue(condIndex > 0);
        final int argIndex = out.indexOf(TestPlugin.argumentName);
        Assert.assertTrue(argIndex > condIndex);
    }

}
