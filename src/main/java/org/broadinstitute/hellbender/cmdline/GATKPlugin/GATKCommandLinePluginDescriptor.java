package org.broadinstitute.hellbender.cmdline.GATKPlugin;

import org.broadinstitute.hellbender.exceptions.UserException;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A base class for descriptor for plugins that can be dynamically discovered by the
 * command line parser and specified as command line arguments.
 *
 * Descriptors (sub)classes:
 *
 * - must live in the org.broadinstitute.hellbender.cmdline.GATKPlugin package
 * - must have a no-arg constructor
 * - should have at least one @Argument used to accumulate the user-specified instances
 *   of the plugin seen on the command line. Allowed values for this argument are determined
 *   by the descriptor, but are usually the simple class names of the discovered plugin
 *   subclasses.
 *
 * Each descriptor passed to the command line parser is instantiated before argument
 * parsing starts, and is queried to find the command line plugin classes that should be
 * discovered dynamically.
 *
 * Plugin classes:
 *
 * - should subclass a common base class (the name of which is returned by the descriptor)
 * - should all live in the same package (to allow them to be unambiguously specified
 *   by the class' simple name
 * - should contain @Arguments for any values they wish to collect. @Arguments may be
 *   optional or required. If required, the arguments are in effect "provisionally
 *   required" in that they are contingent on the specific plugin being specified on
 *   the command line; they will only be marked by the command line parser as missing
 *   if the they have not been specified on the command line, and the plugin class
 *   containing the plugin argument *has* been specified on the command line (as
 *   determined by the command line parser via a call to isDependentArgumentAllowed).
 *
 * NOTE: plugin class @Arguments should not have initial values, as the command line
 * parser will interpret these as having been set (and will validate them accordingly
 * and throw if the dependent argument has not been set).
 *
 * The methods for each descriptor are called in the following order:
 *
 *  getPluginClass()/getPackageName() - once when argument parsing begins (if the descriptor
 *  has been passed to the command line parser as a target descriptor)
 *
 *  getClassFilter() - once for each plugin subclass found
 *  addInstance() - once for each plugin subclass that isn't filtered out by getClassFilter
 *  validateDependentArgumentAllowed  - once for each plugin argument value that has been
 *  specified on the command line for a plugin that is controlled by this descriptor
 *
 *  validateArguments() - once when argument parsing is complete
 *  getInstances() - whenever the pluggable class consumer wants the resulting plugin instances
 */
public abstract class GATKCommandLinePluginDescriptor<T> {

    /**
     * Base class to be used as command line plugin. Subclasses of this class will be
     * command line accessible.
     */
    public abstract Class<?> getPluginClass();

    /**
     * Package names from which to load the command line plugin classes
      */
    public abstract String getPackageName();

    //TODO should we use an opt-in annotation instead of runtime filtering ?
    /**
     * Give this descriptor a chance to filter out any classes it doesn't
     * want to be discoverable.
     * @return false if the class shouldn't be used; otherwise true
     */
    public Predicate<Class<?>> getClassFilter() { return c -> true;}

    /**
     * Instantiate and return a new instance of the specified plugin class.
     * It is the descriptor's job to maintain a list of these instances
     * so they can later be retrieved by getInstances().
     *
     * @param pluggableClass a discovered class is a subclass of the base class specified
     *                       by getPluginClass
     * @return the instantiated object that will be used by the command line parser
     * as an argument source
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public abstract Object addInstance(Class<?> pluggableClass)
            throws IllegalAccessException, InstantiationException;

    /**
     * Called by the command line parser when an argument value from the class
     * targetPluginClass has been seen on the command line. Returns true if the
     * argument is allowed (ie., an instance of the dependent parent plugin class
     * was actually seen), otherwise false.
     *
     * @param targetPluginClass
     * @return true if the plugin for this class was specified on the command line
     */
    public abstract boolean isDependentArgumentAllowed(Class<?> targetPluginClass);

    /**
     * It is the descriptor's job to contain an argument list which will be populated
     * by the command line parser with the name of each plugin specified on the command line.
     *
     * This method is called after all command line arguments have been processed to allow
     * the descriptor to validate the plugin arguments specified, and to reduce the list of
     * plugin instances to those actually seen on the command line.
     *
     * Implementations of this method should validate that all of the values that have been
     * specified on the command line have a corresponding plugin instance (this will detect
     * a user-specified value for which there is no corresponding plugin class).
     * @throws UserException.CommandLineException if a plugin value has been specified that
     * has no corresponding plugin instance (i.e., the plugin class corresponding to the name
     * was not discovered)
     */
    public abstract void validateArguments() throws UserException.CommandLineException;

    /**
     * Given a consumer of a list of plugin objects of type T, call the consumer's
     * accept method with the list actual instances accumulated by the plugin (those
     * actually specified on the command line). This gives clients of the plugin
     * objects a chance to retrieve the actual list.
     *
     * @param consumer function that accepts a collection of plugin instances
     */
    public abstract void getInstances(Consumer<Collection<T>> consumer);

}
