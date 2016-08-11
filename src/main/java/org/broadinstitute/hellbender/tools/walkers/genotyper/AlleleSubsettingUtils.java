package org.broadinstitute.hellbender.tools.walkers.genotyper;

import htsjdk.variant.variantcontext.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.collections.Permutation;
import org.broadinstitute.hellbender.utils.genotyper.IndexedAlleleList;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utilities class containing methods for restricting {@link VariantContext} and {@link GenotypesContext} objects to a
 * reduced set of alleles, as well as for choosing the best set of alleles to keep and for cleaning up annotations and
 * genotypes after subsetting.
 *
 * @author David Benjamin &lt;davidben@broadinstitute.org&gt;
 */
public final class AlleleSubsettingUtils {
    private AlleleSubsettingUtils() {}  // prevent instantiation
    private static final int PL_INDEX_OF_HOM_REF = 0;
    public static final int NUM_OF_STRANDS = 2; // forward and reverse strands

    private static final GenotypeLikelihoodCalculators GL_CALCS = new GenotypeLikelihoodCalculators();


    /**
     * Create the new GenotypesContext with the subsetted PLs and ADs
     *
     * @param originalGs               the original GenotypesContext
     * @param originalAlleles          the original alleles
     * @param allelesToKeep            the subset of alleles to use with the new Genotypes
     * @param assignmentMethod         assignment strategy for the (subsetted) PLs
     * @return                         a new non-null GenotypesContext
     */
    public static GenotypesContext subsetAlleles(final GenotypesContext originalGs, final int defaultPloidy,
                                                 final List<Allele> originalAlleles,
                                                 final List<Allele> allelesToKeep,
                                                 final GenotypeAssignmentMethod assignmentMethod) {
        Utils.nonNull(originalGs, "original GenotypesContext must not be null");
        Utils.nonNull(allelesToKeep, "allelesToKeep is null");
        Utils.nonEmpty(allelesToKeep, "must keep at least one allele");
        Utils.validateArg(allelesToKeep.get(0).isReference(), "First allele must be the reference allele");

        final GenotypesContext newGTs = GenotypesContext.create(originalGs.size());
        final Permutation<Allele> allelePermutation = new IndexedAlleleList<>(originalAlleles).permutation(new IndexedAlleleList<>(allelesToKeep));

        final Map<Integer, int[]> subsettedLikelihoodIndicesByPloidy = new TreeMap<>();
        for (final Genotype g : originalGs) {
            final int ploidy = g.getPloidy() > 0 ? g.getPloidy() : defaultPloidy;
            if (!subsettedLikelihoodIndicesByPloidy.containsKey(ploidy)) {
                subsettedLikelihoodIndicesByPloidy.put(ploidy, subsettedPLIndices(ploidy, originalAlleles, allelesToKeep));
            }
            final int[] subsettedLikelihoodIndices = subsettedLikelihoodIndicesByPloidy.get(ploidy);

            final int expectedNumLikelihoods = GenotypeLikelihoods.numLikelihoods(originalAlleles.size(), ploidy);
            // create the new likelihoods array from the alleles we are allowed to use
            double[] newLikelihoods = null;
            if (g.hasLikelihoods()) {
                final double[] originalLikelihoods = g.getLikelihoods().getAsVector();
                newLikelihoods = originalLikelihoods.length == expectedNumLikelihoods ?
                        MathUtils.normalizeFromLog10(Arrays.stream(subsettedLikelihoodIndices)
                                .mapToDouble(idx -> originalLikelihoods[idx]).toArray(), false, true) : null;
            }

            final boolean useNewLikelihoods = newLikelihoods != null && GATKVariantContextUtils.isInformative(newLikelihoods);
            final GenotypeBuilder gb = useNewLikelihoods ? new GenotypeBuilder(g).PL(newLikelihoods) : new GenotypeBuilder(g).noPL();

            GATKVariantContextUtils.makeGenotypeCall(g.getPloidy(), gb, assignmentMethod, newLikelihoods, allelesToKeep);

            // restrict SAC to the new allele subset
            if (g.hasExtendedAttribute(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY)) {
                final int[] newSACs = subsetSACAlleles(g, allelesToKeep, originalAlleles);
                if (newSACs != null)
                    gb.attribute(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY, newSACs);
            }

            // restrict AD to the new allele subset
            if(g.hasAD()) {
                final int[] oldAD = g.getAD();
                final int[] newAD = IntStream.range(0, allelesToKeep.size()).map(n -> oldAD[allelePermutation.fromIndex(n)]).toArray();
                gb.AD(newAD);
            }
            newGTs.add(gb.make());
        }
        return newGTs;
    }

    /**
     * From a given genotype, extract a given subset of alleles and return the new SACs
     * @param g                             genotype to subset
     * @param allelesToUse                  alleles to subset
     * @param originalAlleles               the original alleles
     * @return                              the subsetted SACs
     */
    private static int[] subsetSACAlleles(final Genotype g, final List<Allele> allelesToUse, final List<Allele> originalAlleles){

        if ( !g.hasExtendedAttribute(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY) )
            return null;

        // we need to determine which of the alternate alleles (and hence the likelihoods) to use and carry forward
        final int numOriginalAltAlleles = originalAlleles.size() - 1;
        final int numNewAltAlleles = allelesToUse.size() - 1;
        final List<Integer> sacIndicesToUse = numOriginalAltAlleles == numNewAltAlleles ? null : determineSACIndicesToUse(originalAlleles, allelesToUse);

        return makeNewSACs(g, sacIndicesToUse);
    }

    /**
     * Make a new SAC array from the a subset of the genotype's original SAC
     *
     * @param g               the genotype
     * @param sacIndicesToUse the indices in the SAC to use given the allelesToUse (@see #determineSACIndicesToUse())
     * @return subset of SACs from the original genotype, the original SACs if sacIndicesToUse is null
     */
    private static int[] makeNewSACs(final Genotype g, final List<Integer> sacIndicesToUse) {

        final int[] oldSACs  = getSACs(g);

        if (sacIndicesToUse == null) {
            return oldSACs;
        } else {
            final int[] newSACs = new int[sacIndicesToUse.size()];
            int newIndex = 0;
            for (final int oldIndex : sacIndicesToUse) {
                newSACs[newIndex++] = oldSACs[oldIndex];
            }
            return newSACs;
        }
    }

    /**
     * Get the genotype SACs
     *
     * @param g the genotype
     * @return an arrays of SACs
     * @throws GATKException if the type of the SACs is unexpected
     */
    private static int[] getSACs(final Genotype g) {

        if ( !g.hasExtendedAttribute(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY) )
            throw new IllegalArgumentException("Genotype must have SAC");

        if ( g.getExtendedAttributes().get(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY).getClass().equals(String.class) ) {
            final String SACsString = (String) g.getExtendedAttributes().get(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY);
            String[] stringSACs = SACsString.split(",");
            final int[] intSACs = new int[stringSACs.length];
            int i = 0;
            for (String sac : stringSACs)
                intSACs[i++] = Integer.parseInt(sac);

            return intSACs;
        }
        else if ( g.getExtendedAttributes().get(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY).getClass().equals(int[].class) )
            return (int[]) g.getExtendedAttributes().get(GATKVCFConstants.STRAND_COUNT_BY_SAMPLE_KEY);
        else
            throw new GATKException("Unexpected SAC type");
    }

    /**
     * Find the strand allele count indices to use for a selected set of alleles
     *
     * @param originalAlleles the original alleles
     * @param allelesToUse the subset of alleles to use
     * @return a list of SAC indices to use or null if none
     */
    private static List<Integer> determineSACIndicesToUse(final List<Allele> originalAlleles, final List<Allele> allelesToUse) {

        // the bitset representing the allele indices we want to keep
        final BitSet alleleIndicesToUse = getAlleleIndexBitset(originalAlleles, allelesToUse);

        // an optimization: if we are supposed to use all (or none in the case of a ref call) of the alleles,
        // then we can keep the SACs as is; otherwise, we determine which ones to keep
        if (alleleIndicesToUse.cardinality() == alleleIndicesToUse.size()) {
            return null;
        }

        return getSACIndices(alleleIndicesToUse);
    }

    /**
     * Given the original alleles and a list of alleles from that VC to keep,
     * returns a bitset representing which allele indices should be kept
     *
     * @param originalAlleles the original alleles
     * @param allelesToKeep   the list of alleles to keep
     * @return non-null bitset
     */
    private static BitSet getAlleleIndexBitset(final List<Allele> originalAlleles, final List<Allele> allelesToKeep) {
        final int numOriginalAlleles = originalAlleles.size();
        final BitSet alleleIndicesToKeep = new BitSet(numOriginalAlleles);

        // the reference allele is still used
        alleleIndicesToKeep.set(0);
        for ( int i = 0; i < numOriginalAlleles; i++ ) {
            if ( originalAlleles.get(i).isNonReference() ) {
                if (allelesToKeep.contains(originalAlleles.get(i))) {
                    alleleIndicesToKeep.set(i);
                }
            }
        }

        return alleleIndicesToKeep;
    }

    /**
     * Get the actual strand aleele counts indices to use given the corresponding allele indices
     *
     * @param alleleIndicesToUse    the bitset representing the alleles to use (@see #getAlleleIndexBitset)
     * @return a non-null List
     */
    private static List<Integer> getSACIndices(final BitSet alleleIndicesToUse) {

        if (alleleIndicesToUse.isEmpty()) throw new IllegalArgumentException("cannot have no alleles to use");

        final List<Integer> result = new ArrayList<>(NUM_OF_STRANDS * alleleIndicesToUse.size());

        for (int SACindex = 0; SACindex < alleleIndicesToUse.size(); SACindex++) {
            if (alleleIndicesToUse.get(SACindex)) {
                result.add(NUM_OF_STRANDS * SACindex);
                result.add(NUM_OF_STRANDS * SACindex + 1);
            }
        }

        return result;
    }

    /**
     * Returns the new set of alleles to use based on a likelihood score: alleles' scores are the sum of their counts in
     * sample genotypes, weighted by the confidence in the genotype calls.
     *
     * @param vc target variant context.
     * @param numAltAllelesToKeep number of alleles to keep.
     * @return the list of alleles to keep, including the reference
     */
    public static List<Allele> calculateMostLikelyAlleles(final VariantContext vc, final int defaultPloidy,
                                                          final int numAltAllelesToKeep) {
        Utils.nonNull(vc, "vc is null");
        final int nonRefAltAlleleIndex = GATKVariantContextUtils.indexOfAltAllele(vc, GATKVCFConstants.NON_REF_SYMBOLIC_ALLELE, false);
        final int[] properAltAlleleIndices = IntStream.range(1, vc.getNAlleles()).filter(n -> n != nonRefAltAlleleIndex).toArray();

        if (numAltAllelesToKeep >= properAltAlleleIndices.length) {
            return vc.getAlleles();
        }

        final double[] likelihoodSums = new double[vc.getNAlleles()];
        for ( final Genotype genotype : vc.getGenotypes().iterateInSampleNameOrder() ) {
            final double[] gls = genotype.getLikelihoods().getAsVector();
            if (gls == null || !GATKVariantContextUtils.isInformative(gls)) {
                continue;
            }
            final int indexOfMostLikelyGenotype = MathUtils.maxElementIndex(gls);
            final double GLDiffBetweenRefAndBest = gls[indexOfMostLikelyGenotype] - gls[PL_INDEX_OF_HOM_REF];
            final int ploidy = genotype.getPloidy() > 0 ? genotype.getPloidy() : defaultPloidy;

            final int[] alleleCounts = new GenotypeLikelihoodCalculators()
                    .getInstance(ploidy, vc.getNAlleles()).genotypeAlleleCountsAt(indexOfMostLikelyGenotype)
                    .alleleCountsByIndex(vc.getNAlleles() - 1);

            for (int allele = 1; allele < alleleCounts.length; allele++) {
                likelihoodSums[allele] += alleleCounts[allele] * GLDiffBetweenRefAndBest;
            }
        }

        final List<Double> properAltAlleleLikelihoodSums = Arrays.stream(properAltAlleleIndices)
                .mapToObj(n -> likelihoodSums[n]).collect(Collectors.toList());
        Collections.sort(properAltAlleleLikelihoodSums, Collections.reverseOrder());
        final double likelihoodSumThreshold = properAltAlleleLikelihoodSums.get(numAltAllelesToKeep);
        return IntStream.range(0, vc.getNAlleles()) //keep ref, non-ref, and alts above the threshold
                .filter(n -> n == 0 || n == nonRefAltAlleleIndex || likelihoodSums[n] > likelihoodSumThreshold)
                .mapToObj(n -> vc.getAlternateAllele(n-1)).collect(Collectors.toList());
    }

    /**
     * Given a list of original alleles and a subset of new alleles to retain, find the array of old PL indices that correspond
     * to new PL indices i.e. result[7] = old PL index of genotype containing same alleles as the new genotype with PL index 7.
     *
     * This method is written in terms f indices rather than subsetting PLs directly in order to produce output that can be
     * recycled from sample to sample, provided that the ploidy is the same.
     *
     * @param ploidy                Ploidy (number of chromosomes describing PL's)
     * @param originalAlleles       List of original alleles
     * @param newAlleles            New alleles -- must be a subset of {@code originalAlleles}
     * @return                      old PL indices of new genotypes
     */
    private static int[] subsettedPLIndices(final int ploidy, final List<Allele> originalAlleles, final List<Allele> newAlleles) {
        final int[] result = new int[GenotypeLikelihoods.numLikelihoods(newAlleles.size(), ploidy)];
        final Permutation<Allele> allelePermutation = new IndexedAlleleList<>(originalAlleles).permutation(new IndexedAlleleList<>(newAlleles));

        final GenotypeLikelihoodCalculator glCalc = GL_CALCS.getInstance(ploidy, originalAlleles.size());
        for (int oldPLIndex = 0; oldPLIndex < glCalc.genotypeCount(); oldPLIndex++) {
            final GenotypeAlleleCounts oldAlleleCounts = glCalc.genotypeAlleleCountsAt(oldPLIndex);

            final boolean containsOnlyNewAlleles = IntStream.range(0, oldAlleleCounts.distinctAlleleCount())
                    .map(oldAlleleCounts::alleleIndexAt).allMatch(allelePermutation::isKept);

            if (containsOnlyNewAlleles) {
                // make an array in the format described in {@link GenotypeAlleleCounts}:
                // [(new) index of first allele, count of first allele, (new) index of second allele, count of second allele. . .]
                final int[] newAlleleCounts = IntStream.range(0, newAlleles.size()).flatMap(newAlleleIndex ->
                        IntStream.of(newAlleleIndex, oldAlleleCounts.alleleCountFor(allelePermutation.fromIndex(newAlleleIndex)))).toArray();

                final int newPLIndex = glCalc.alleleCountsToIndex(newAlleleCounts);
                result[newPLIndex] = oldPLIndex;
            }
        }
        return  result;
    }
}
