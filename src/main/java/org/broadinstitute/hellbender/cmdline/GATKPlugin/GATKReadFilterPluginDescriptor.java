package org.broadinstitute.hellbender.cmdline.GATKPlugin;

import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineParser;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.filters.CountingReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A GATKCommandLinePluginDescriptor for the ReadFilter class
 */
public class GATKReadFilterPluginDescriptor extends GATKCommandLinePluginDescriptor<ReadFilter> {

    @Argument(fullName = StandardArgumentDefinitions.READ_FILTER_LONG_NAME,
            shortName = StandardArgumentDefinitions.READ_FILTER_SHORT_NAME,
            doc="Read filters to be applied before analysis", optional=true)
    public final List<String> readFilterNames = new ArrayList<>(); // preserve order

    final String disableReadFilterArgName = "disableReadFilter";
    @Argument(fullName = disableReadFilterArgName,
            shortName = "df",
            doc="Read filters to be disabled before analysis", optional=true)
    public final Set<String> disableFilters = new HashSet<>();

    // Map of read filter class (simple) names to the corresponding plugin instance
    public Map<String, ReadFilter> readFilters = new HashMap<>();

    /////////////////////////////////////////////////////////
    // GATKCommandLinePluginDescriptor implementation methods

    @Override
    public Class<?> getPluginClass() {return org.broadinstitute.hellbender.engine.filters.ReadFilter.class;}

    @Override
    public String getPackageName() {return "org.broadinstitute.hellbender.engine.filters";};

    @Override
    public Predicate<Class<?>> getClassFilter() {
        //TODO should we use an opt-in annotation instead of runtime filtering ?
        return c -> {
            // don't use the ReadFilter base class, or the CountingReadFilter, or the unit tests
            return !c.getName().equals(this.getPluginClass().getName()) &&
                    !c.getName().startsWith(this.getPluginClass().getName() + "$") &&
                    !c.getName().startsWith(CountingReadFilter.class.getName()) &&
                    !c.getName().contains("UnitTest$");
        };
    }

    // Instantiate a new ReadFilter derived object and save it in the list
    @Override
    public Object addInstance(final Class<?> pluggableClass)
            throws IllegalAccessException, InstantiationException {
        final ReadFilter readFilter = (ReadFilter) pluggableClass.newInstance();
        // use getSimpleName; the classes are all in the same package
        readFilters.put(pluggableClass.getSimpleName(), readFilter);
        return readFilter;
    }

    // Verify that this target plugin class's dependent value has been specified
    @Override
    public boolean isDependentArgumentAllowed(Class<?> targetPluginClass) {
        return readFilterNames.contains(targetPluginClass.getSimpleName());
    }

    /**
     * Pass back the list of ReadFilter instances that were actually seen on the
     * command line in the same order they were specified
     */
    @Override
    public void getInstances(final Consumer<Collection<ReadFilter>> consumer) {
        // add the instances in the order they were specified on the command line
        final ArrayList<ReadFilter> filters = new ArrayList<>(readFilters.size());
        readFilterNames.forEach(s -> filters.add(getReadFilterForName(s)));
        consumer.accept(filters);
    }

    // Find the instance of the given read filter in the instance list.
    private ReadFilter getReadFilterForName(final String filterName) {
        ReadFilter rf = null;
        for (Map.Entry<String, ReadFilter> entry : readFilters.entrySet()) {
            if (entry.getKey().endsWith(filterName)) {
                rf = entry.getValue();
                break;
            }
        }
        return rf;
    }

    // Return the allowable values for readFilterNames/disableReadFilter
    @Override
    public Set<String> getAllowedStringValues(final String longArgName) {
        if (longArgName.equals(StandardArgumentDefinitions.READ_FILTER_LONG_NAME) ||
                longArgName.equals(disableReadFilterArgName)) {
            return readFilters.keySet();
        }
        throw new IllegalArgumentException("Allowed values request for unrecognized string argument: " + longArgName);
    }

    /**
     * Validate the list of arguments and reduce the list of read filters to those
     * actually seen on the command line. This is called by the command line parser
     * after all arguments have been parsed.
     */
    @Override
    public void validateArguments() {
        // first, validate that no filter is disabled *and* enabled
        disableFilters.forEach(
            s -> {
                if (readFilterNames.contains(s)) {
                    throw new UserException.CommandLineException(
                            "Read filter: " + s + " is both enabled and disabled");
                }
            });
        readFilterNames.forEach(
            s -> {
                if (disableFilters.contains(s)) {
                    throw new UserException.CommandLineException(
                            "Read filter: " + s + " is both enabled and disabled");
                }
            });

        // now validate that each filter specified is valid (has a corresponding instance)
        Map<String, ReadFilter> requestedReadFilters = new HashMap<>();
        readFilterNames.forEach(s -> {
            ReadFilter trf = readFilters.get(s);
            if (null == trf) {
                throw new UserException.CommandLineException("Unrecognized read filter name: " + s);
            }
            else {
                requestedReadFilters.put(s, trf);
            }
        });
        readFilters = requestedReadFilters;
    }

    /////////////////////////////////////////////////////////
    // ReadFilter plugin-specific helper methods

    /**
     * Determine if a particular ReadFilter was disabled on the command line.
     * @param filterName
     * @return true if the name appears the list of disabled filters
     */
    public boolean isDisabledFilter(final String filterName) {
        return disableFilters.contains(filterName);
    }

    /**
     * Merge the default filters with the users's command line read filter requests, then initialize
     * the resulting filters.
     *
     * @param defaultFilters - default filters for the tool context
     * @param samHeader - a SAMFileHeader to initialize read filter instances
     * @param wrapperFunction - a wrapper function for each read filter; used to wrap read filters in
     *                        CountingReadFilters
     * @param mergeAccumulator - identity filter for base accumulator in list reduction (this is the default
     *                         filter returned if no filters are left after reducing the merged filter list)
     * @param <T> extends ReadFilter, type returned by the wrapperFunction
     * @return
     */
    public <T extends ReadFilter> T getMergedReadFilters(
            final List<ReadFilter> defaultFilters,
            final SAMFileHeader samHeader,
            final Function<ReadFilter, T> wrapperFunction,
            final BinaryOperator<T> mergeFunction,
            final T mergeAccumulator) {

        Utils.nonNull(defaultFilters);
        Utils.nonNull(samHeader);
        Utils.nonNull(wrapperFunction);
        Utils.nonNull(mergeFunction);
        Utils.nonNull(mergeAccumulator);

        // start with the tool's default filters and remove any that were disabled on the command line
        final List<ReadFilter> userSpecifiedReadFilters = defaultFilters
                .stream()
                .filter(f -> !isDisabledFilter(f.getClass().getSimpleName()))
                .collect(Collectors.toList());

        // now add in any additional filters enabled on the command line in the order in which
        // they were specified
        getInstances(orderedList -> orderedList.forEach(f -> userSpecifiedReadFilters.add(f)));

        // give each remaining filter a header, then map/wrap and reduce using the merge function
        userSpecifiedReadFilters.forEach(f -> f.setHeader(samHeader));
        return userSpecifiedReadFilters.stream().map(wrapperFunction).reduce(mergeAccumulator, mergeFunction);
    }

}
