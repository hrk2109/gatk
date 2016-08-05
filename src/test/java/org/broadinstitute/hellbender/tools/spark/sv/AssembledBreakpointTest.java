package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.TextCigarCodec;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class AssembledBreakpointTest {

    @Test
    public void testGetLeftAlignedLeftBreakpointOnAssembledContig() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1","1", TextCigarCodec.decode("100M100S"), true, new SimpleInterval("1", 100, 200), 60, 1, 100, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1","1", TextCigarCodec.decode("100M100S"), false, new SimpleInterval("1", 500, 600), 60, 101, 200, 0);
        final AssembledBreakpoint assembledBreakpoint = new AssembledBreakpoint("1", region1, region2, "", "", new ArrayList<>());
        Assert.assertEquals(assembledBreakpoint.getLeftAlignedLeftBreakpointOnAssembledContig(), new SimpleInterval("1", 200, 200));
    }

    @Test
    public void testGetLeftAlignedLeftBreakpointOnAssembledContigWithHomology() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1","1", TextCigarCodec.decode("105M100S"), true, new SimpleInterval("1", 100, 205), 60, 1, 105, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1","1", TextCigarCodec.decode("105M100S"), false, new SimpleInterval("1", 500, 605), 60, 95, 200, 0);
        final AssembledBreakpoint assembledBreakpoint = new AssembledBreakpoint("1", region1, region2, "", "ACACA", new ArrayList<>());
        Assert.assertEquals(assembledBreakpoint.getLeftAlignedLeftBreakpointOnAssembledContig(), new SimpleInterval("1", 200, 200));
    }

    @Test
    public void testGetBreakpointAllele() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1","1", TextCigarCodec.decode("105M100S"), true, new SimpleInterval("1", 100, 205), 60, 1, 105, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1","1", TextCigarCodec.decode("100S105M"), false, new SimpleInterval("1", 500, 605), 60, 95, 200, 0);
        final AssembledBreakpoint assembledBreakpoint = new AssembledBreakpoint("1", region1, region2, "", "ACACA", new ArrayList<>());
        final BreakpointAllele breakpointAllele = assembledBreakpoint.getBreakpointAllele();
        Assert.assertEquals(breakpointAllele.leftAlignedLeftBreakpoint, new SimpleInterval("1", 200, 200));
        Assert.assertEquals(breakpointAllele.leftAlignedRightBreakpoint, new SimpleInterval("1", 600, 600));
        Assert.assertTrue(breakpointAllele.fiveToThree);
        Assert.assertFalse(breakpointAllele.threeToFive);
        Assert.assertEquals(breakpointAllele.homology, "ACACA");
    }

}