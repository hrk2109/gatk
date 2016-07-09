package org.broadinstitute.hellbender.engine.filters;

import htsjdk.samtools.Cigar;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * Standard ReadFilters
 */
public final class ReadFilterLibrary {

    private ReadFilterLibrary(){ /*no instance*/ }

    /**
     * local classes for static read filters
     */
    public static class AllowAllReads extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){return true;}}

    //Note: do not call getCigar to avoid creation of new Cigar objects
    public static class CigarIsSupported extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return ! CigarUtils.containsNOperator(read.getCigarElements());}}

    public static class GoodCigar extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test (final GATKRead read) {
            return CigarUtils.isGood(read.getCigar());}}

    public static class HasMatchingBasesAndQuals extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return read.getLength() == read.getBaseQualityCount();}}

    public static class HasReadGroup extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return read.getReadGroup() != null;}}

    public static class Mapped extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return !read.isUnmapped();}}

    public static class MappingQualityAvailable extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return read.getMappingQuality() != QualityUtils.MAPPING_QUALITY_UNAVAILABLE;}}

    public static class MappingQualityNotZero extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return read.getMappingQuality() != 0;}}

    /**
     * Reads that either have a mate that maps to the same contig, or don't have a mapped mate.
     */
    public static class MateOnSameContigOrNoMappedMate extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return ! read.isPaired() ||
                    read.mateIsUnmapped() ||
                    (! read.isUnmapped() && read.getContig().equals(read.getMateContig()));}}

    /**
     * Reads that have a mapped mate and both mate and read are on different same strands (ie the usual situation).
     */
    public static class MateDifferentStrand extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return read.isPaired() &&
                    ! read.isUnmapped() &&
                    ! read.mateIsUnmapped() &&
                    read.mateIsReverseStrand() != read.isReverseStrand();}}

    public static class NonZeroReferenceLengthAlignment extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test (final GATKRead read) {
            return read.getCigarElements()
                    .stream()
                    .anyMatch(c -> c.getOperator().consumesReferenceBases() && c.getLength() > 0);
        }}

    public static class NotDuplicate extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return ! read.isDuplicate();}}

    public static class PassesVendorQualityCheck extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return ! read.failsVendorQualityCheck();}}

    public static class PrimaryAlignment extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return ! read.isSecondaryAlignment();}}

    //Note: do not call getCigar to avoid creation of new Cigar objects
    public static class ReadLengthEqualsCigarLength extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test (final GATKRead read) {
            return read.isUnmapped() ||
                    read.getLength() == Cigar.getReadLength(read.getCigarElements());}}

    public static class SeqIsStored extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){
            return read.getLength() > 0;}}

    public static class ValidAlignmentStart extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read){return read.isUnmapped() || read.getStart() > 0;}}

    // Alignment doesn't align to a negative number of bases in the reference.
    //Note: to match gatk3 we must keep reads that align to zero bases in the reference.
    public static class ValidAlignmentEnd extends ReadFilter {
        private static final long serialVersionUID = 1L;
        @Override public boolean test(final GATKRead read) {
            return read.isUnmapped() || (read.getEnd() - read.getStart() + 1) >= 0;}}

    /**
     * Static, stateless read filter instances
     */
    public static final AllowAllReads ALLOW_ALL_READS = new AllowAllReads();
    public static final CigarIsSupported CIGAR_IS_SUPPORTED = new CigarIsSupported();
    public static final GoodCigar GOOD_CIGAR = new GoodCigar();
    public static final HasReadGroup HAS_READ_GROUP = new HasReadGroup();
    public static final HasMatchingBasesAndQuals HAS_MATCHING_BASES_AND_QUALS = new HasMatchingBasesAndQuals();
    public static final Mapped MAPPED = new Mapped();
    public static final MappingQualityAvailable MAPPING_QUALITY_AVAILABLE = new MappingQualityAvailable();
    public static final MappingQualityNotZero MAPPING_QUALITY_NOT_ZERO   = new MappingQualityNotZero();
    public static final MateOnSameContigOrNoMappedMate MATE_ON_SAME_CONTIG_OR_NO_MAPPED_MATE = new MateOnSameContigOrNoMappedMate();
    public static final MateDifferentStrand MATE_DIFFERENT_STRAND = new MateDifferentStrand();
    public static final NotDuplicate NOT_DUPLICATE = new NotDuplicate();
    public static final NonZeroReferenceLengthAlignment NON_ZERO_REFERENCE_LENGTH_ALIGNMENT = new NonZeroReferenceLengthAlignment();
    public static final PassesVendorQualityCheck PASSES_VENDOR_QUALITY_CHECK = new PassesVendorQualityCheck();
    public static final PrimaryAlignment PRIMARY_ALIGNMENT = new PrimaryAlignment();
    public static final ReadLengthEqualsCigarLength READLENGTH_EQUALS_CIGARLENGTH = new ReadLengthEqualsCigarLength();
    public static final SeqIsStored SEQ_IS_STORED = new SeqIsStored();
    public static final ValidAlignmentStart VALID_ALIGNMENT_START = new ValidAlignmentStart();
    public static final ValidAlignmentEnd VALID_ALIGNMENT_END = new ValidAlignmentEnd();

}
