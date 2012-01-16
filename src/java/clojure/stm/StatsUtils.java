package clojure.stm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Utils for statistics
 * 
 * @author dennis(killme2008@gmail.com)
 * @date 2012-1-16
 * 
 */
public class StatsUtils {

    static enum StatsItem {
        /**
         * stats items for transaction
         */
        GET_WRITE_LOCK_FAIL,
        INTERRUPTED,
        CHANGE_COMMITTED,
        BARGE_FAIL,
        NOT_RUNNING,
        GET_FAULT,
        TOTAL_TIMES,
        TOTAL_COST,
        /**
         * Stats items for reference
         */
        DEREF,
        SET,
        ALTER,
        ENSURE,
        COMMUTE,
    }


    public static void statsReason(
            final ConcurrentHashMap<String/* form */, ConcurrentHashMap<StatsUtils.StatsItem, AtomicLong>> counter,
            final String form, final StatsUtils.StatsItem reason, final long delta) {
        ConcurrentHashMap<StatsUtils.StatsItem, AtomicLong> subMap = counter.get(form);
        if (subMap == null) {
            subMap = new ConcurrentHashMap<StatsUtils.StatsItem, AtomicLong>();
            final ConcurrentHashMap<StatsUtils.StatsItem, AtomicLong> oldMap = counter.putIfAbsent(form, subMap);
            if (oldMap != null) {
                subMap = oldMap;
            }
        }

        AtomicLong atom = subMap.get(reason);
        if (atom == null) {
            atom = new AtomicLong(0);
            final AtomicLong old = subMap.putIfAbsent(reason, atom);
            if (old != null) {
                atom = old;
            }
        }
        atom.addAndGet(delta);
    }
}
