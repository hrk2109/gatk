package org.broadinstitute.hellbender.engine.filters;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.TextCigarCodec;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineParser;
import org.broadinstitute.hellbender.cmdline.GATKPlugin.GATKReadFilterPluginDescriptor;
import org.broadinstitute.hellbender.cmdline.argumentcollections.ReadInputArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.RequiredReadInputArgumentCollection;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;


public class ReadFilterPluginUnitTest {

    class TestArgCollection {

        @ArgumentCollection
        ReadInputArgumentCollection readInputs = new RequiredReadInputArgumentCollection();

        public TestArgCollection(){};
    };

    @DataProvider(name="filtersWithArguments")
    public Object[][] filtersWithArguments(){
        return new Object[][]{
                { LibraryReadFilter.class.getSimpleName(), "--library", "fakeLibrary" },
                { MappingQualityReadFilter.class.getSimpleName(), "--mappingQuality", "25" },
                { PlatformReadFilter.class.getSimpleName(), "--PLFilterName", "fakePlatform" },
                { PlatformUnitReadFilter.class.getSimpleName(), "--blackListedLanes", "fakeUnit" },
                { ReadGroupReadFilter.class.getSimpleName(), "--readGroup", "fakeGroup" },
                { ReadGroupBlackListReadFilter.class.getSimpleName(), "--blackList", "tg:sub"},
                { ReadNameReadFilter.class.getSimpleName(), "--readName", "fakeRead" },
                { ReadLengthReadFilter.class.getSimpleName(), "--maxReadLength", "10" },
                { SampleReadFilter.class.getSimpleName(), "--sample", "fakeSample" }
        };
    }

    // fail if a filter that requires arguments is specified but arguments are not set on the command line
    @Test(dataProvider = "filtersWithArguments", expectedExceptions = UserException.MissingArgument.class)
    public void testDependentFilterArguments(
            final String filter,
            final String argName,   //unused
            final String argValue)  //unused
    {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        String[] args = {  // no args, just enable filters
                "-I", "fakeInput",
                "--readFilter", filter
        };

        clp.parseArguments(System.out, args);
    }

    // fail if a filter's arguments are passed but the filter itself is not enabled
    @Test(dataProvider = "filtersWithArguments", expectedExceptions = UserException.CommandLineException.class)
    public void testDanglingFilterArguments(
            final String filter,
            final String argName,
            final String argValue)
    {
        TestArgCollection tArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(tArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));

        String[] args = { argName, argValue }; // no read filter set

        // no need to instantiate the filters  - dependsOn errors are caught by the command line parser
        clp.parseArguments(System.out, args);
    }

    @DataProvider(name="filtersWithGoodArguments")
    public Object[][] filtersWithGoodArguments(){
        return new Object[][]{
                { LibraryReadFilter.class.getSimpleName(),
                        (Consumer<SetupTest>) this::setupLibraryTest, "--library", "Foo" },
                { MappingQualityReadFilter.class.getSimpleName(),
                        (Consumer<SetupTest>) this::setupMappingQualityTest, "--mappingQuality", "255" },
                { ReadGroupReadFilter.class.getSimpleName(),
                        (Consumer<SetupTest>) this::setupReadGroupTest, "--read_group_to_keep", "fred" },
                { ReadNameReadFilter.class.getSimpleName(),
                        (Consumer<SetupTest>) this::setupReadNameTest, "--readName", "fred" },
                { SampleReadFilter.class.getSimpleName(),
                        (Consumer<SetupTest>) this::setupSampleTest, "--sample", "fred" }
        };
    }

    @Test(dataProvider = "filtersWithGoodArguments")
    public void testFiltersWithFilterArguments(
            final String filter,
            final Consumer<SetupTest> setup,
            final String argName,
            final String argValue)
    {
        TestArgCollection tArgs = new TestArgCollection();

        final SAMFileHeader header = createHeaderWithReadGroups();
        final GATKRead read = simpleGoodRead(header);

        CommandLineParser clp = new CommandLineParser(tArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));

        String[] args = {
                "-I", "fakeInput",
                "--readFilter", filter,
                argName, argValue
        };
        clp.parseArguments(System.out, args);
        ReadFilter rf = instantiateFilter(clp, tArgs.readInputs, header);

        // to ensure that the filter is actually working, verify that the test record
        // we're using fails the filter test *before* we set it up to pass the filter
        Assert.assertFalse(rf.test(read));

        // setup the header and read for this test
        setup.accept(new SetupTest(header, read, argValue));

        Assert.assertTrue(rf.test(read));
    }

    @Test
    public void testNoFiltersSpecified() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[]{"-I", "fakeInput"});

        // get the command line read filters
        GATKReadFilterPluginDescriptor readFilterPlugin =
                (GATKReadFilterPluginDescriptor)
                        clp.getPluginDescriptor(GATKReadFilterPluginDescriptor.class);
        List<ReadFilter> readFilters = new ArrayList<>();
        readFilterPlugin.getInstances(orderedList -> orderedList.forEach(f -> readFilters.add(f)));
        Assert.assertEquals(readFilters.size(), 0);
    }

    @Test
    public void testDisableAllFilters() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[] {
                "-I", "fakeInput",
                "--RF", ReadFilterLibrary.MAPPED.getClass().getSimpleName(),
                "--disableAllReadFilters"});
        Assert.assertTrue(testArgs.readInputs.disableAllReadFilters);
    }

    @Test(expectedExceptions = UserException.CommandLineException.class)
    public void testNonExistentFilter() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[] {
                "-I", "fakeInput",
                "--RF", "fakeFilter"});
    }

    @Test(expectedExceptions = UserException.CommandLineException.class)
    public void testEnableDisableConflict() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[] {
                "-I", "fakeInput",
                "--RF", "GoodCigar",
                "--disableReadFilter", "GoodCigar"});
    }

    @Test
    public void testDisableOneFilter() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[] {
                "-I", "fakeInput",
                "--RF", ReadFilterLibrary.MAPPED.getClass().getSimpleName(),
                "--RF", ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS.getClass().getSimpleName(),
                "-disableReadFilter", ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName()});

        // get the command line read filters
        GATKReadFilterPluginDescriptor readFilterPlugin =
                (GATKReadFilterPluginDescriptor)
                        clp.getPluginDescriptor(GATKReadFilterPluginDescriptor.class);
        List<ReadFilter> readFilters = new ArrayList<>();
        readFilterPlugin.getInstances(orderedList -> orderedList.forEach(f -> readFilters.add(f)));

        Assert.assertEquals(readFilters.size(), 2);
        Assert.assertEquals(readFilters.get(0).getClass().getSimpleName(),
                ReadFilterLibrary.MAPPED.getClass().getSimpleName());
        Assert.assertEquals(readFilters.get(1).getClass().getSimpleName(),
                ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS.getClass().getSimpleName());

        Assert.assertEquals(readFilterPlugin.disableFilters.size(), 1);
        Assert.assertTrue(readFilterPlugin.disableFilters.contains(
                ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName()));
        Assert.assertTrue(readFilterPlugin.isDisabledFilter(
                ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName()));
    }

    @Test
    public void testDisableMultipleFilters() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[] {
                "-I", "fakeInput",
                "--RF", ReadFilterLibrary.MAPPED.getClass().getSimpleName(),
                "-disableReadFilter", ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName(),
                "-disableReadFilter", ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS.getClass().getSimpleName()});

        // get the command line read filters
        GATKReadFilterPluginDescriptor readFilterPlugin =
                (GATKReadFilterPluginDescriptor)
                        clp.getPluginDescriptor(GATKReadFilterPluginDescriptor.class);
        List<ReadFilter> readFilters = new ArrayList<>();
        readFilterPlugin.getInstances(orderedList -> orderedList.forEach(f -> readFilters.add(f)));

        Assert.assertEquals(readFilters.size(), 1);
        Assert.assertEquals(readFilters.get(0).getClass().getSimpleName(),
                ReadFilterLibrary.MAPPED.getClass().getSimpleName());

        Assert.assertEquals(readFilterPlugin.disableFilters.size(), 2);
        Assert.assertTrue(readFilterPlugin.disableFilters.contains(
                ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName()));
        Assert.assertTrue(readFilterPlugin.disableFilters.contains(
                ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS.getClass().getSimpleName()));
    }

    @Test
    public void testEnableMultipleFilters() {
        TestArgCollection tArgs = new TestArgCollection();

        final SAMFileHeader header = createHeaderWithReadGroups();
        final GATKRead read = simpleGoodRead(header);

        CommandLineParser clp = new CommandLineParser(tArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        String[] args = {
                "-I", "fakeInput",
                "--readFilter", ReadLengthReadFilter.class.getSimpleName(),
                "--minReadLength", "10",
                "--maxReadLength", "20",
                "--readFilter", ReadNameReadFilter.class.getSimpleName(),
                "--readName", "fred"
        };
        clp.parseArguments(System.out, args);
        ReadFilter rf = instantiateFilter(clp, tArgs.readInputs, header);

        Assert.assertFalse(rf.test(read));
        read.setName("fred");
        read.setBases(new byte[15]);
        Assert.assertTrue(rf.test(read));
    }

    @Test
    public void testPreserveSpecifiedOrder() {
        TestArgCollection testArgs = new TestArgCollection();
        CommandLineParser clp = new CommandLineParser(testArgs,
              Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        clp.parseArguments(System.out, new String[] {
                "-I", "fakeInput",
                "-readFilter", ReadFilterLibrary.MAPPED.getClass().getSimpleName(),
                "-readFilter", ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS.getClass().getSimpleName(),
                "-readFilter", ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName()});

        GATKReadFilterPluginDescriptor readFilterPlugin =
                (GATKReadFilterPluginDescriptor)
                        clp.getPluginDescriptor(GATKReadFilterPluginDescriptor.class);

        // now add in any additional filters enabled on the command line
        List<ReadFilter> orderedFilters = new ArrayList<>();
        readFilterPlugin.getInstances(orderedList -> orderedList.forEach(f -> orderedFilters.add(f)));

        // defaults first, then command line, in order
        Assert.assertEquals(orderedFilters.size(), 3);
        Assert.assertEquals(orderedFilters.get(0).getClass().getSimpleName(),
                ReadFilterLibrary.MAPPED.getClass().getSimpleName());
        Assert.assertEquals(orderedFilters.get(1).getClass().getSimpleName(),
                ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS.getClass().getSimpleName());
        Assert.assertEquals(orderedFilters.get(2).getClass().getSimpleName(),
                ReadFilterLibrary.GOOD_CIGAR.getClass().getSimpleName());
    }

    @Test
    public void testReadLengthFilter() {
        TestArgCollection tArgs = new TestArgCollection();

        final SAMFileHeader header = createHeaderWithReadGroups();
        final GATKRead read = simpleGoodRead(header);

        CommandLineParser clp = new CommandLineParser(tArgs,
                Collections.singletonList(GATKReadFilterPluginDescriptor.class));
        String[] args = {
                "-I", "fakeInput",
                "--readFilter", ReadLengthReadFilter.class.getSimpleName(),
                "--minReadLength", "10",
                "--maxReadLength", "20"
        };
        clp.parseArguments(System.out, args);
        ReadFilter rf = instantiateFilter(clp, tArgs.readInputs, header);

        read.setBases(new byte[5]);
        Assert.assertFalse(rf.test(read));
        read.setBases(new byte[25]);
        Assert.assertFalse(rf.test(read));
        read.setBases(new byte[15]);
        Assert.assertTrue(rf.test(read));
    }

    private ReadFilter instantiateFilter(
            final CommandLineParser clp,
            final ReadInputArgumentCollection readInput,
            final SAMFileHeader header)
    {
        GATKReadFilterPluginDescriptor readFilterPlugin =
                (GATKReadFilterPluginDescriptor)
                        clp.getPluginDescriptor(GATKReadFilterPluginDescriptor.class);
        ReadFilter rf = readFilterPlugin.getMergedReadFilters(
                new ArrayList<>(),
                header,
                (ReadFilter f) -> f,
                (ReadFilter f1, ReadFilter f2) -> f1.and(f2),
                ReadFilterLibrary.ALLOW_ALL_READS);

        return rf;
    }

    private static final int CHR_COUNT = 2;
    private static final int CHR_START = 1;
    private static final int CHR_SIZE = 1000;
    private static final int GROUP_COUNT = 5;

    private SAMFileHeader createHeaderWithReadGroups() {
        return ArtificialReadUtils.createArtificialSamHeaderWithGroups(CHR_COUNT, CHR_START, CHR_SIZE, GROUP_COUNT);
    }

    /**
     * Creates a read record.
     *
     * @param header header for the new record
     * @param cigar the new record CIGAR.
     * @param group the new record group index that must be in the range \
     *              [0,{@link #GROUP_COUNT})
     * @param reference the reference sequence index (0-based)
     * @param start the start position of the read alignment in the reference
     *              (1-based)
     * @return never <code>null</code>
     */
    private GATKRead createRead(final SAMFileHeader header, final Cigar cigar, final int group,
                                final int reference, final int start ) {
        final GATKRead record = ArtificialReadUtils.createArtificialRead(header, cigar);
        record.setPosition(header.getSequence(reference).getSequenceName(), start);
        record.setReadGroup(header.getReadGroups().get(group).getReadGroupId());
        return record;
    }

    private GATKRead simpleGoodRead( final SAMFileHeader header ) {
        final String cigarString = "101M";
        final Cigar cigar = TextCigarCodec.decode(cigarString);
        GATKRead read = createRead(header, cigar, 1, 0, 10);
        read.setMappingQuality(50);
        return read;
    }

    // object for holding test setup params to use as a type for test lambdas
    public class SetupTest {
        public SAMFileHeader hdr;
        public GATKRead read;
        public String argValue;

        public SetupTest(SAMFileHeader hdr, GATKRead read, String argValue) {
            this.hdr = hdr;
            this.read = read;
            this.argValue = argValue;
        }
    }

    // setup methods to initialize reads for individual filter tests
    private void setupLibraryTest(SetupTest setup) {
        setup.hdr.getReadGroup(setup.read.getReadGroup()).setLibrary(setup.argValue);
    }

    private void setupMappingQualityTest(SetupTest setup) {
        setup.read.setMappingQuality(Integer.parseInt(setup.argValue));
    }

    private void setupReadGroupTest(SetupTest setup) {
        setup.read.setReadGroup(setup.argValue);
    }

    private void setupReadNameTest(SetupTest setup) {
        setup.read.setName(setup.argValue);
    }

    private void setupSampleTest(SetupTest setup) {
        setup.hdr.getReadGroup(setup.read.getReadGroup()).setSample(setup.argValue);
    }

}
