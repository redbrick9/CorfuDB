package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.util.auditor.Event;

/**
 * Created by Sam Behnam on 2/15/18.
 */
public class IsEmptyOperation extends Operation {
    public IsEmptyOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        return getConfiguration()
                .getMap(event.getMapId())
                .isEmpty();
    }
}
