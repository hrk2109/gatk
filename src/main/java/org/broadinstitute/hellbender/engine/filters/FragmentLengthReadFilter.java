package org.broadinstitute.hellbender.engine.filters;

import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.Serializable;

/**
 * Keep reads that are within a given max fragment length.
 */
public final class FragmentLengthReadFilter extends ReadFilter implements Serializable  {
    private static final long serialVersionUID = 1l;

    @Argument(fullName = "maxFragmentLength",
            shortName = "maxFragment",
            doc = "Keep only read pairs with fragment length at most equal to the given value",
            optional = false)
    public Integer maxFragmentLength; // no default value, since the command line parser will think its set

    @Override
    public boolean test( final GATKRead read ) {
        if ( ! read.isPaired() ) {
            return true;
        }
        //Note fragment length is negative if mate maps to lower position than read so we take absolute value.
        return Math.abs(read.getFragmentLength()) <= maxFragmentLength;
    }
}
