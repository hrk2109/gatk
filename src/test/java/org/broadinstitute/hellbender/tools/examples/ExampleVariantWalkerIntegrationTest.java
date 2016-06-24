package org.broadinstitute.hellbender.tools.examples;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public final class ExampleVariantWalkerIntegrationTest extends CommandLineProgramTest {

    private static final String TEST_DATA_DIRECTORY = publicTestDir + "org/broadinstitute/hellbender/engine/";
    private static final String TEST_OUTPUT_DIRECTORY = publicTestDir + "org/broadinstitute/hellbender/tools/examples/";

    @Test
    public void testExampleVariantWalker() throws IOException {
        IntegrationTestSpec testSpec = new IntegrationTestSpec(
                " -L 1:100-200" +
                " -R " + hg19MiniReference +
                " -I " + TEST_DATA_DIRECTORY + "reads_data_source_test1.bam" +
                " -V " + TEST_DATA_DIRECTORY + "example_variants.vcf" +
                " -auxiliaryVariants " + TEST_DATA_DIRECTORY + "feature_data_source_test.vcf" +
                " -O %s",
                Arrays.asList(TEST_OUTPUT_DIRECTORY + "expected_ExampleVariantWalkerIntegrationTest_output.txt")
        );
        testSpec.executeTest("testExampleIntervalWalker", this);
    }

    @Test
    public void testExampleVariantWalkerWithTileDB() throws IOException {
        IntegrationTestSpec testSpec = new IntegrationTestSpec(
                " -L 20:100-200" +
                        " -R " + "/Users/louisb/Workspace/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta" +
                        " -I " + "/Users/louisb/Workspace/gatk/src/test/resources/large/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam" +
                        " -V " + "gendb:///Users/louisb/Workspace/gatk/testArrayJsons" +
                        " -O %s",
                Arrays.asList(TEST_OUTPUT_DIRECTORY + "expected_ExampleVariantWalkerIntegrationTest_output.txt")
        );
        testSpec.executeTest("testExampleIntervalWalker", this);
    }
}
