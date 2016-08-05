package org.broadinstitute.hellbender.tools.spark.sv;

import com.github.lindenb.jbwa.jni.AlnRgn;
import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.utils.bwa.BWANativeLibrary;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.broadinstitute.hellbender.tools.spark.sv.RunSGAViaProcessBuilderOnSpark.ContigsCollection;
import static org.broadinstitute.hellbender.tools.spark.sv.RunSGAViaProcessBuilderOnSpark.ContigsCollection.ContigID;
import static org.broadinstitute.hellbender.tools.spark.sv.RunSGAViaProcessBuilderOnSpark.ContigsCollection.ContigSequence;

public class ContigAligner implements Closeable {

    static String referencePath;

    final BwaIndex index;
    final BwaMem bwaMem;

    private static final Logger log = LogManager.getLogger(ContigAligner.class);

    static {
        BWANativeLibrary.load();
    }

    public ContigAligner(final String referenceFilePath) throws IOException {
        referencePath = referenceFilePath;

        try {
            index = new BwaIndex(LocalizedReference.INSTANCE);
            log.info("Created BWA index");
        } catch (final IOException e) {
            throw new GATKException("Unable to load reference", e);
        }
        bwaMem = new BwaMem(index);
        log.info("Created BWA MEM");
    }

    public List<AlignmentRegion> alignContigs(String breakpointId, final ContigsCollection contigsCollection) {
        final List<AlignmentRegion> alignedContigs = new ArrayList<>();
        try {
            for(final Tuple2<ContigID, ContigSequence> contigInfo : contigsCollection.getContents()) {
                final String contigId = contigInfo._1.toString();
                final byte[] sequence = contigInfo._2.toString().getBytes();
                final AlnRgn[] alnRgns = bwaAlignSequence(bwaMem, contigId, sequence);

                log.info("alnRgns : " + (alnRgns == null ? "null" : alnRgns.length));
                // filter out secondary alignments, convert to AlignmentRegion objects and sort by alignment start pos
                final List<AlignmentRegion> alignmentRegionList = Arrays.stream(alnRgns)
                        .filter(a -> a.getSecondary() < 0)
                        .map(a -> new AlignmentRegion(breakpointId, contigId, a))
                        .sorted(Comparator.comparing(a -> a.startInAssembledContig))
                        .collect(arrayListCollector(alnRgns.length));
                 alignedContigs.addAll(alignmentRegionList);
            }
        } catch (final IOException e) {
            throw new GATKException("could not execute BWA");
        }

        return alignedContigs;
    }

    @VisibleForTesting
    public static List<AssembledBreakpoint> getAssembledBreakpointsFromAlignmentRegions(final byte[] sequence, final List<AlignmentRegion> alignmentRegionList, final Integer minAlignLength) {
        final List<AssembledBreakpoint> results = new ArrayList<>(alignmentRegionList.size() - 1);
        final Iterator<AlignmentRegion> iterator = alignmentRegionList.iterator();
        final List<String> insertionAlignmentRegions = new ArrayList<>();
        if ( iterator.hasNext() ) {
            AlignmentRegion current = iterator.next();
            while (treatAlignmentRegionAsInsertion(current) && iterator.hasNext()) {
                current = iterator.next();
            }
            while ( iterator.hasNext() ) {
                final AlignmentRegion next = iterator.next();
                if (currentAlignmentRegionIsTooSmall(current, next, minAlignLength)) {
                    continue;
                }

                if (treatNextAlignmentRegionInPairAsInsertion(current, next, minAlignLength)) {
                    if (iterator.hasNext()) {
                        insertionAlignmentRegions.add(next.toPackedString());
                        // todo: track alignments of skipped regions for classification as duplications, mei's etc.
                        continue;
                    } else {
                        break;
                    }
                }

                final AlignmentRegion previous = current;
                current = next;

                final byte[] sequenceCopy = Arrays.copyOf(sequence, sequence.length);

                String homology = "";
                if (previous.endInAssembledContig >= current.startInAssembledContig) {
                    final byte[] homologyBytes = Arrays.copyOfRange(sequenceCopy, current.startInAssembledContig - 1, previous.endInAssembledContig);
                    if (previous.referenceInterval.getStart() > current.referenceInterval.getStart()) {
                        SequenceUtil.reverseComplement(homologyBytes, 0, homologyBytes.length);
                    }
                    homology = new String(homologyBytes);
                }

                String insertedSequence = "";
                if (previous.endInAssembledContig < current.startInAssembledContig - 1) {

                    final int insertionStart;
                    final int insertionEnd;

                    insertionStart = previous.endInAssembledContig + 1;
                    insertionEnd = current.startInAssembledContig - 1;

                    final byte[] insertedSequenceBytes = Arrays.copyOfRange(sequenceCopy, insertionStart - 1, insertionEnd);
                    if (previous.referenceInterval.getStart() > current.referenceInterval.getStart()) {
                        SequenceUtil.reverseComplement(insertedSequenceBytes, 0, insertedSequenceBytes.length);
                    }
                    insertedSequence = new String(insertedSequenceBytes);
                }
                final AssembledBreakpoint assembledBreakpoint = new AssembledBreakpoint(current.contigId, previous, current, insertedSequence, homology, insertionAlignmentRegions);

                results.add(assembledBreakpoint);
            }
        }
        return results;
    }

    private static boolean currentAlignmentRegionIsTooSmall(final AlignmentRegion current, final AlignmentRegion next, final Integer minAlignLength) {
        return current.referenceInterval.size() - current.overlapOnContig(next) < minAlignLength;
    }

    protected static boolean treatNextAlignmentRegionInPairAsInsertion(AlignmentRegion current, AlignmentRegion next, final Integer minAlignLength) {
        return treatAlignmentRegionAsInsertion(next) ||
                (next.referenceInterval.size() - current.overlapOnContig(next) < minAlignLength) ||
                current.referenceInterval.contains(next.referenceInterval) ||
                next.referenceInterval.contains(current.referenceInterval);
    }

    private static boolean treatAlignmentRegionAsInsertion(final AlignmentRegion next) {
        return next.mqual < 60;
    }

    private Collector<AlignmentRegion, ?, ArrayList<AlignmentRegion>> arrayListCollector(final int size) {
        return Collectors.toCollection( () -> new ArrayList<>(size));
    }

    /**
     * Wrap a contig sequence in a ShortRead object and pass it to BWA to align
     */
    private AlnRgn[] bwaAlignSequence(final BwaMem bwaMem, final String contigId, final byte[] sequence) throws IOException {
        final ShortRead contigShortRead = new ShortRead(contigId, sequence, qualSequence(sequence.length));
        log.debug("Calling bwaMem.align");
        return bwaMem.align(contigShortRead);
    }

    /**
     * Generate a bogus base quality sequence to pass in for the aligned contig (since the jBWA API requires that reads have qualities)
     */
    private byte[] qualSequence(final int length) {
        final byte[] quals = new byte[length];
        Arrays.fill(quals, (byte)'A');
        return quals;
    }

    public void close() {
        log.info("closing BWA mem and index");
        bwaMem.dispose();
        index.close();
    }

    private static class LocalizedReference {
        static File INSTANCE;

        static {
            try {
                INSTANCE = BucketUtils.isHadoopUrl(ContigAligner.referencePath) ? BwaSparkEngine.localizeReferenceAndBwaIndexFiles(ContigAligner.referencePath) : new File(ContigAligner.referencePath);
            } catch (final IOException e) {
                throw new GATKException("unable to localize reference", e);
            }
        }
    }

}
