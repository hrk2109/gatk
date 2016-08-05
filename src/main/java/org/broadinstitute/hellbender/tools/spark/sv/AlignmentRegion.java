package org.broadinstitute.hellbender.tools.spark.sv;

import com.github.lindenb.jbwa.jni.AlnRgn;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.TextCigarCodec;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.List;
import java.util.Objects;

class AlignmentRegion {

    final String contigId;
    final String breakpointId;
    final Cigar cigar;
    final boolean forwardStrand;
    final SimpleInterval referenceInterval;
    final int mqual;
    final int startInAssembledContig;
    final int endInAssembledContig;
    final int assembledContigLength;
    final int mismatches;

    public AlignmentRegion(final String breakpointId, final String contigId, final AlnRgn alnRgn) {
        this.contigId = contigId;
        this.breakpointId = breakpointId;
        this.forwardStrand = alnRgn.getStrand() == '+';
        final Cigar alignmentCigar = TextCigarCodec.decode(alnRgn.getCigar());
        this.cigar = forwardStrand ? alignmentCigar : CigarUtils.invertCigar(alignmentCigar);
        this.referenceInterval = new SimpleInterval(alnRgn.getChrom(), (int) alnRgn.getPos() + 1, (int) (alnRgn.getPos() + 1 + cigar.getReferenceLength()));
        this.mqual = alnRgn.getMQual();
        this.assembledContigLength = cigar.getReadLength();
        this.startInAssembledContig = startOfAlignmentInContig(cigar);
        this.endInAssembledContig = endOfAlignmentInContig(assembledContigLength, cigar);
        this.mismatches = alnRgn.getNm();
    }

    public AlignmentRegion(final String breakpointId, final String contigId, final Cigar cigar, final boolean forwardStrand, final SimpleInterval referenceInterval, final int mqual, final int startInAssembledContig, final int endInAssembledContig, final int mismatches) {
        this.contigId = contigId;
        this.breakpointId = breakpointId;
        this.cigar = cigar;
        this.forwardStrand = forwardStrand;
        this.referenceInterval = referenceInterval;
        this.mqual = mqual;
        this.startInAssembledContig = startInAssembledContig;
        this.endInAssembledContig = endInAssembledContig;
        this.assembledContigLength = cigar.getReadLength();
        this.mismatches = mismatches;
    }

    public AlignmentRegion(final GATKRead read) {
        this.breakpointId = null;
        this.contigId = read.getName();
        this.forwardStrand = ! read.isReverseStrand();
        this.cigar = forwardStrand ? read.getCigar() : CigarUtils.invertCigar(read.getCigar());
        this.referenceInterval = new SimpleInterval(read);
        this.assembledContigLength = cigar.getReadLength() + getHardClipping(cigar);
        this.startInAssembledContig = startOfAlignmentInContig(cigar);
        this.endInAssembledContig = endOfAlignmentInContig(assembledContigLength, cigar);
        this.mqual = read.getMappingQuality();
        if (read.hasAttribute("NM")) {
            this.mismatches = read.getAttributeAsInteger("NM");
        } else {
            this.mismatches = 0;
        }
    }

    public int overlapOnContig(final AlignmentRegion other) {
        return Math.max(0, Math.min(endInAssembledContig, other.endInAssembledContig) - Math.max(startInAssembledContig, other.startInAssembledContig));
    }

    private static int getHardClipping(final Cigar cigar) {
        final List<CigarElement> cigarElements = cigar.getCigarElements();
        return (cigarElements.get(0).getOperator() == CigarOperator.HARD_CLIP ? cigarElements.get(0).getLength() : 0) +
                (cigarElements.get(cigarElements.size() - 1).getOperator() == CigarOperator.HARD_CLIP ? cigarElements.get(cigarElements.size() - 1).getLength() : 0);
    }

    private static int startOfAlignmentInContig(final Cigar cigar) {
        return getClippedBases(true, cigar) + 1;
    }

    private static int endOfAlignmentInContig(final int assembledContigLength, final Cigar cigar) {
        return assembledContigLength - getClippedBases(false, cigar);
    }

    private static int getClippedBases(final boolean fromStart, final Cigar cigar) {
        int posInContig = 0;
        int j = fromStart ? 0 : cigar.getCigarElements().size() - 1;
        final int offset = fromStart ? 1 : -1;
        CigarElement ce = cigar.getCigarElement(j);
        while (ce.getOperator().isClipping()) {
            posInContig += ce.getLength();
            j += offset;
            ce = cigar.getCigarElement(j);
        }
        return posInContig;
    }

    @Override
    public String toString() {
        return breakpointId +
                "\t" +
                contigId +
                "\t" +
                referenceInterval.getContig() +
                "\t" +
                referenceInterval.getStart() +
                "\t" +
                referenceInterval.getEnd() +
                "\t" +
                (forwardStrand ? "+" : "-") +
                "\t" +
                cigar.toString() +
                "\t" +
                mqual +
                "\t" +
                startInAssembledContig +
                "\t" +
                endInAssembledContig +
                "\t" +
                mismatches;
    }

    public static AlignmentRegion fromString(final String[] fields) {
        final String breakpointId = fields[0];
        final String contigId = fields[1];
        final String refContig = fields[2];
        final Integer refStart = Integer.valueOf(fields[3]);
        final Integer refEnd = Integer.valueOf(fields[4]);
        final SimpleInterval refInterval = new SimpleInterval(refContig, refStart, refEnd);
        final boolean refStrand = ("+".equals(fields[5]));
        final Cigar cigar = TextCigarCodec.decode(fields[6]);
        final int mqual = Integer.valueOf(fields[7]);
        final int contigStart = Integer.valueOf(fields[8]);
        final int contigEnd = Integer.valueOf(fields[9]);
        final int mismatches = Integer.valueOf(fields[10]);
        return new AlignmentRegion(breakpointId, contigId, cigar, refStrand, refInterval, mqual, contigStart, contigEnd, mismatches);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlignmentRegion that = (AlignmentRegion) o;
        return forwardStrand == that.forwardStrand &&
                mqual == that.mqual &&
                startInAssembledContig == that.startInAssembledContig &&
                endInAssembledContig == that.endInAssembledContig &&
                assembledContigLength == that.assembledContigLength &&
                mismatches == that.mismatches &&
                Objects.equals(contigId, that.contigId) &&
                Objects.equals(breakpointId, that.breakpointId) &&
                Objects.equals(cigar, that.cigar) &&
                Objects.equals(referenceInterval, that.referenceInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contigId, breakpointId, cigar, forwardStrand, referenceInterval, mqual, startInAssembledContig, endInAssembledContig, assembledContigLength, mismatches);
    }

    public String toPackedString() {
        return "" + startInAssembledContig + "-" + endInAssembledContig + ":" + referenceInterval.getContig() + ',' + referenceInterval.getStart() + ',' + (forwardStrand ? '+' : '-') + ',' + TextCigarCodec.encode(cigar) + ',' + mqual + ',' + mismatches;
    }
}
