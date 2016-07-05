package org.broadinstitute.hellbender.tools.spark.pipelines;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.GATKPlugin.GATKCommandLinePluginDescriptor;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.PrintStream;
import java.util.List;

@CommandLineProgramProperties(summary = "Counts variants in the input VCF",
        oneLineSummary = "CountVariants on Spark",
        programGroup = SparkProgramGroup.class)
public final class CountVariantsSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    @Argument(doc = "uri for the input file: a local file path",
            shortName = StandardArgumentDefinitions.VARIANT_SHORT_NAME, fullName = StandardArgumentDefinitions.VARIANT_LONG_NAME,
            optional = false)
    public String input;


    @Argument(doc = "uri for the output file: a local file path",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    public String out;

    /**
     * Return the list of GATKCommandLinePluginDescriptor classes to be used for this CLP.
     * GATKSparkTool returns the GATKReadFilterPluginDescriptor, but we don't want that
     * for Variant tools.
     */
    @Override
    protected List<Class<? extends GATKCommandLinePluginDescriptor<?>>> getPluginDescriptors() {
        return null;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        final VariantsSparkSource vss = new VariantsSparkSource(ctx);
        final JavaRDD<VariantContext> variants = vss.getParallelVariantContexts(input, getIntervals());

        final long count = variants.count();
        System.out.println(count);

        if( out != null) {
            try (final PrintStream ps = new PrintStream(BucketUtils.createFile(out, getAuthenticatedGCSOptions()))) {
                ps.print(count);
            }
        }
    }
}
