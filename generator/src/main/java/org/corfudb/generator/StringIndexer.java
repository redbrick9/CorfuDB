package org.corfudb.generator;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTable.Index;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class StringIndexer implements CorfuTable.IndexRegistry<String, String> {

    public static final CorfuTable.IndexName BY_VALUE = () -> "BY_VALUE";
    public static final CorfuTable.IndexName BY_FIRST_CHAR = () -> "BY_FIRST_LETTER";

    private static final CorfuTable.Index<String, String, ? extends Comparable<?>> BY_VALUE_INDEX =
            new CorfuTable.Index<>(
                    BY_VALUE,
                    (CorfuTable.IndexFunction<String, String, String>) (key, val) -> val);

    private static final CorfuTable.Index<String, String, ? extends Comparable<?>> BY_FIRST_CHAR_INDEX =
            new CorfuTable.Index<>(
                    BY_FIRST_CHAR,
                    (CorfuTable.IndexFunction<String, String, String>) (key, val) ->
                            Character.toString(val.charAt(0)));

    @Override
    public Iterator<Index<String, String, ? extends Comparable<?>>> iterator() {
        return Stream.of(BY_VALUE_INDEX, BY_FIRST_CHAR_INDEX).iterator();
    }

    @Override
    public Optional<CorfuTable.Index<String, String, ? extends Comparable<?>>> get(CorfuTable.IndexName name) {
        String indexName = (name != null)? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            return Optional.of(BY_VALUE_INDEX);

        } else if (BY_FIRST_CHAR.get().equals(indexName)) {
            return Optional.of(BY_FIRST_CHAR_INDEX);
        } else {
            return Optional.empty();
        }
    }
}