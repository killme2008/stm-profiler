/**
 *   Copyright (c) Rich Hickey. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

/* rich Jul 26, 2007 */

package clojure.stm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.PersistentHashMap;
import clojure.lang.RT;
import clojure.lang.Util;


@SuppressWarnings({ "SynchronizeOnNonFinalField" })
public class LockingTransaction {

    public static final int RETRY_LIMIT = 10000;
    public static final int LOCK_WAIT_MSECS = 100;
    public static final long BARGE_WAIT_NANOS = 10 * 1000000;
    // public static int COMMUTE_RETRY_LIMIT = 10;

    private String form;
    static final int RUNNING = 0;
    static final int COMMITTING = 1;
    static final int RETRY = 2;
    static final int KILLED = 3;
    static final int COMMITTED = 4;


    public LockingTransaction(final String form) {
        super();
        this.form = form;
    }

    public final static ConcurrentHashMap<String/* form */, ConcurrentHashMap<StatsItem, AtomicLong>> counter =
            new ConcurrentHashMap<String, ConcurrentHashMap<StatsItem, AtomicLong>>();
    final static ThreadLocal<LockingTransaction> transaction = new ThreadLocal<LockingTransaction>();


    public static IPersistentMap getSTMStats() {
        IPersistentMap rt = PersistentHashMap.EMPTY;
        for (final Map.Entry<String, ConcurrentHashMap<StatsItem, AtomicLong>> entry1 : counter.entrySet()) {
            final String form = entry1.getKey();
            final ConcurrentHashMap<StatsItem, AtomicLong> subMap = entry1.getValue();
            IPersistentMap subRt = PersistentHashMap.EMPTY;
            long totalRetry = 0;
            for (final Map.Entry<StatsItem, AtomicLong> entry2 : subMap.entrySet()) {
                final StatsItem statsItem = entry2.getKey();
                final String item = statsItem.name();
                final long value = entry2.getValue().get();
                if (statsItem != StatsItem.TOTAL_COST && statsItem != StatsItem.TOTAL_TIMES) {
                    totalRetry += value;
                }
                subRt = subRt.assoc(item, value);
            }
            final long totalTimes = subRt.valAt("TOTAL_TIMES") == null ? 0 : (Long) subRt.valAt("TOTAL_TIMES");
            final long totalCost = subRt.valAt("TOTAL_COST") == null ? 0 : (Long) subRt.valAt("TOTAL_COST");
            if (totalTimes > 0) {
                subRt = subRt.assoc("AVERAGE_COST", Math.round((double) totalCost / totalTimes));
                subRt = subRt.assoc("AVERAGE_RETRY", Math.round((double) totalRetry / totalTimes));
            }
            rt = rt.assoc(form, subRt);
        }
        return rt;
    }

    static class RetryEx extends Error {
        public StatsItem reason;


        public RetryEx(final StatsItem reason) {
            super();
            this.reason = reason;
        }

    }

    static class AbortException extends Exception {
    }

    public static class Info {
        final AtomicInteger status;
        final long startPoint;
        final CountDownLatch latch;


        public Info(final int status, final long startPoint) {
            this.status = new AtomicInteger(status);
            this.startPoint = startPoint;
            this.latch = new CountDownLatch(1);
        }


        public boolean running() {
            final int s = this.status.get();
            return s == RUNNING || s == COMMITTING;
        }
    }

    static class CFn {
        final IFn fn;
        final ISeq args;


        public CFn(final IFn fn, final ISeq args) {
            this.fn = fn;
            this.args = args;
        }
    }

    // total order on transactions
    // transactions will consume a point for init, for each retry, and on commit
    // if writing
    final private static AtomicLong lastPoint = new AtomicLong();


    void getReadPoint() {
        this.readPoint = lastPoint.incrementAndGet();
    }


    long getCommitPoint() {
        return lastPoint.incrementAndGet();
    }


    void stop(final int status) {
        if (this.info != null) {
            synchronized (this.info) {
                this.info.status.set(status);
                this.info.latch.countDown();
            }
            this.info = null;
            this.vals.clear();
            this.sets.clear();
            this.commutes.clear();
            // actions.clear();
        }
    }

    enum StatsItem {
        GET_WRITE_LOCK_FAIL,
        INTERRUPTED,
        CHANGE_COMMITTED,
        BARGE_FAIL,
        NOT_RUNNING,
        GET_FAULT,
        TOTAL_TIMES,
        TOTAL_COST
    }

    final RetryEx getWriteLockFail = new RetryEx(StatsItem.GET_WRITE_LOCK_FAIL);
    final RetryEx interrupted = new RetryEx(StatsItem.INTERRUPTED);
    final RetryEx changeComitted = new RetryEx(StatsItem.CHANGE_COMMITTED);
    final RetryEx bargeFail = new RetryEx(StatsItem.BARGE_FAIL);
    final RetryEx notRunning = new RetryEx(StatsItem.NOT_RUNNING);
    final RetryEx getFault = new RetryEx(StatsItem.GET_FAULT);

    Info info;
    long readPoint;
    long startPoint;
    long startTime;
    // final RetryEx retryex = new RetryEx();
    final ArrayList<Agent.Action> actions = new ArrayList<Agent.Action>();
    final HashMap<Ref, Object> vals = new HashMap<Ref, Object>();
    final HashSet<Ref> sets = new HashSet<Ref>();
    final TreeMap<Ref, ArrayList<CFn>> commutes = new TreeMap<Ref, ArrayList<CFn>>();

    final HashSet<Ref> ensures = new HashSet<Ref>(); // all hold readLock


    void tryWriteLock(final Ref ref) {
        try {
            if (!ref.lock.writeLock().tryLock(LOCK_WAIT_MSECS, TimeUnit.MILLISECONDS)) {
                throw this.getWriteLockFail;
            }
        }
        catch (final InterruptedException e) {
            throw this.interrupted;
        }
    }


    // returns the most recent val
    Object lock(final Ref ref) {
        // can't upgrade readLock, so release it
        this.releaseIfEnsured(ref);

        boolean unlocked = true;
        try {
            this.tryWriteLock(ref);
            unlocked = false;

            if (ref.tvals != null && ref.tvals.point > this.readPoint) {
                throw this.changeComitted;
            }
            final Info refinfo = ref.tinfo;

            // write lock conflict
            if (refinfo != null && refinfo != this.info && refinfo.running()) {
                if (!this.barge(refinfo)) {
                    ref.lock.writeLock().unlock();
                    unlocked = true;
                    return this.blockAndBail(refinfo);
                }
            }
            ref.tinfo = this.info;
            return ref.tvals == null ? null : ref.tvals.val;
        }
        finally {
            if (!unlocked) {
                ref.lock.writeLock().unlock();
            }
        }
    }


    private Object blockAndBail(final Info refinfo) {
        // stop prior to blocking
        this.stop(RETRY);
        try {
            refinfo.latch.await(LOCK_WAIT_MSECS, TimeUnit.MILLISECONDS);
        }
        catch (final InterruptedException e) {
            // ignore
        }
        throw this.bargeFail;
    }


    private void releaseIfEnsured(final Ref ref) {
        if (this.ensures.contains(ref)) {
            this.ensures.remove(ref);
            ref.lock.readLock().unlock();
        }
    }


    void abort() throws AbortException {
        this.stop(KILLED);
        throw new AbortException();
    }


    private boolean bargeTimeElapsed() {
        return System.nanoTime() - this.startTime > BARGE_WAIT_NANOS;
    }


    private boolean barge(final Info refinfo) {
        boolean barged = false;
        // if this transaction is older
        // try to abort the other
        if (this.bargeTimeElapsed() && this.startPoint < refinfo.startPoint) {
            barged = refinfo.status.compareAndSet(RUNNING, KILLED);
            if (barged) {
                refinfo.latch.countDown();
            }
        }
        return barged;
    }


    static LockingTransaction getEx() {
        final LockingTransaction t = transaction.get();
        if (t == null || t.info == null) {
            throw new IllegalStateException("No transaction running");
        }
        return t;
    }


    static public boolean isRunning() {
        return getRunning() != null;
    }


    static LockingTransaction getRunning() {
        final LockingTransaction t = transaction.get();
        if (t == null || t.info == null) {
            return null;
        }
        return t;
    }


    static public Object runInTransaction(final String form, final Callable fn) throws Exception {
        // System.out.println(form);
        LockingTransaction t = transaction.get();
        if (t == null) {
            transaction.set(t = new LockingTransaction(form));
        }
        t.form = form;

        if (t.info != null) {
            return fn.call();
        }

        return t.run(fn);
    }

    static class Notify {
        final public Ref ref;
        final public Object oldval;
        final public Object newval;


        Notify(final Ref ref, final Object oldval, final Object newval) {
            this.ref = ref;
            this.oldval = oldval;
            this.newval = newval;
        }
    }


    Object run(final Callable fn) throws Exception {
        boolean done = false;
        Object ret = null;
        final ArrayList<Ref> locked = new ArrayList<Ref>();
        final ArrayList<Notify> notify = new ArrayList<Notify>();
        this.statsReason(StatsItem.TOTAL_TIMES, 1);
        final long start = System.nanoTime();
        for (int i = 0; !done && i < RETRY_LIMIT; i++) {
            try {
                this.getReadPoint();
                if (i == 0) {
                    this.startPoint = this.readPoint;
                    this.startTime = System.nanoTime();
                }
                this.info = new Info(RUNNING, this.startPoint);
                ret = fn.call();
                // make sure no one has killed us before this point, and can't
                // from now on
                if (this.info.status.compareAndSet(RUNNING, COMMITTING)) {
                    for (final Map.Entry<Ref, ArrayList<CFn>> e : this.commutes.entrySet()) {
                        final Ref ref = e.getKey();
                        if (this.sets.contains(ref)) {
                            continue;
                        }

                        final boolean wasEnsured = this.ensures.contains(ref);
                        // can't upgrade readLock, so release it
                        this.releaseIfEnsured(ref);
                        this.tryWriteLock(ref);
                        locked.add(ref);
                        if (wasEnsured && ref.tvals != null && ref.tvals.point > this.readPoint) {
                            throw this.changeComitted;
                        }

                        final Info refinfo = ref.tinfo;
                        if (refinfo != null && refinfo != this.info && refinfo.running()) {
                            if (!this.barge(refinfo)) {
                                throw this.bargeFail;
                            }
                        }
                        final Object val = ref.tvals == null ? null : ref.tvals.val;
                        this.vals.put(ref, val);
                        for (final CFn f : e.getValue()) {
                            this.vals.put(ref, f.fn.applyTo(RT.cons(this.vals.get(ref), f.args)));
                        }
                    }
                    for (final Ref ref : this.sets) {
                        this.tryWriteLock(ref);
                        locked.add(ref);
                    }

                    // validate and enqueue notifications
                    for (final Map.Entry<Ref, Object> e : this.vals.entrySet()) {
                        final Ref ref = e.getKey();
                        ref.validate(ref.getValidator(), e.getValue());
                    }

                    // at this point, all values calced, all refs to be written
                    // locked
                    // no more client code to be called
                    final long msecs = System.currentTimeMillis();
                    final long commitPoint = this.getCommitPoint();
                    for (final Map.Entry<Ref, Object> e : this.vals.entrySet()) {
                        final Ref ref = e.getKey();
                        final Object oldval = ref.tvals == null ? null : ref.tvals.val;
                        final Object newval = e.getValue();
                        final int hcount = ref.histCount();

                        if (ref.tvals == null) {
                            ref.tvals = new Ref.TVal(newval, commitPoint, msecs);
                        }
                        else if (ref.faults.get() > 0 && hcount < ref.maxHistory || hcount < ref.minHistory) {
                            ref.tvals = new Ref.TVal(newval, commitPoint, msecs, ref.tvals);
                            ref.faults.set(0);
                        }
                        else {
                            ref.tvals = ref.tvals.next;
                            ref.tvals.val = newval;
                            ref.tvals.point = commitPoint;
                            ref.tvals.msecs = msecs;
                        }
                        if (ref.getWatches().count() > 0) {
                            notify.add(new Notify(ref, oldval, newval));
                        }
                    }

                    done = true;
                    this.info.status.set(COMMITTED);
                }
            }
            catch (final RetryEx retry) {
                this.statsRetry(retry);
                // eat this so we retry rather than fall out
            }
            finally {
                for (int k = locked.size() - 1; k >= 0; --k) {
                    locked.get(k).lock.writeLock().unlock();
                }
                locked.clear();
                for (final Ref r : this.ensures) {
                    r.lock.readLock().unlock();
                }
                this.ensures.clear();
                this.stop(done ? COMMITTED : RETRY);
                try {
                    if (done) // re-dispatch out of transaction
                    {
                        for (final Notify n : notify) {
                            n.ref.notifyWatches(n.oldval, n.newval);
                        }
                        for (final Agent.Action action : this.actions) {
                            Agent.dispatchAction(action);
                        }
                    }
                }
                finally {
                    notify.clear();
                    this.actions.clear();
                }
            }
        }
        final long end = System.nanoTime();
        this.statsReason(StatsItem.TOTAL_COST, (end - start) / 1000000);
        if (!done) {
            throw Util.runtimeException("Transaction failed after reaching retry limit");
        }
        return ret;
    }


    private void statsRetry(final RetryEx retry) {
        this.statsReason(retry.reason, 1);
    }


    private void statsReason(final StatsItem reason, final long delta) {
        ConcurrentHashMap<StatsItem, AtomicLong> subMap = counter.get(this.form);
        if (subMap == null) {
            subMap = new ConcurrentHashMap<LockingTransaction.StatsItem, AtomicLong>();
            final ConcurrentHashMap<StatsItem, AtomicLong> oldMap = counter.putIfAbsent(this.form, subMap);
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


    public void enqueue(final Agent.Action action) {
        this.actions.add(action);
    }


    Object doGet(final Ref ref) {
        if (!this.info.running()) {
            throw this.notRunning;
        }
        if (this.vals.containsKey(ref)) {
            return this.vals.get(ref);
        }
        try {
            ref.lock.readLock().lock();
            if (ref.tvals == null) {
                throw new IllegalStateException(ref.toString() + " is unbound.");
            }
            Ref.TVal ver = ref.tvals;
            do {
                if (ver.point <= this.readPoint) {
                    return ver.val;
                }
            } while ((ver = ver.prior) != ref.tvals);
        }
        finally {
            ref.lock.readLock().unlock();
        }
        // no version of val precedes the read point
        ref.faults.incrementAndGet();
        throw this.getFault;

    }


    Object doSet(final Ref ref, final Object val) {
        if (!this.info.running()) {
            throw this.notRunning;
        }
        if (this.commutes.containsKey(ref)) {
            throw new IllegalStateException("Can't set after commute");
        }
        if (!this.sets.contains(ref)) {
            this.sets.add(ref);
            this.lock(ref);
        }
        this.vals.put(ref, val);
        return val;
    }


    void doEnsure(final Ref ref) {
        if (!this.info.running()) {
            throw this.notRunning;
        }
        if (this.ensures.contains(ref)) {
            return;
        }
        ref.lock.readLock().lock();

        // someone completed a write after our snapshot
        if (ref.tvals != null && ref.tvals.point > this.readPoint) {
            ref.lock.readLock().unlock();
            throw this.changeComitted;
        }

        final Info refinfo = ref.tinfo;

        // writer exists
        if (refinfo != null && refinfo.running()) {
            ref.lock.readLock().unlock();

            if (refinfo != this.info) // not us, ensure is doomed
            {
                this.blockAndBail(refinfo);
            }
        }
        else {
            this.ensures.add(ref);
        }
    }


    Object doCommute(final Ref ref, final IFn fn, final ISeq args) {
        if (!this.info.running()) {
            throw this.notRunning;
        }
        if (!this.vals.containsKey(ref)) {
            Object val = null;
            try {
                ref.lock.readLock().lock();
                val = ref.tvals == null ? null : ref.tvals.val;
            }
            finally {
                ref.lock.readLock().unlock();
            }
            this.vals.put(ref, val);
        }
        ArrayList<CFn> fns = this.commutes.get(ref);
        if (fns == null) {
            this.commutes.put(ref, fns = new ArrayList<CFn>());
        }
        fns.add(new CFn(fn, args));
        final Object ret = fn.applyTo(RT.cons(this.vals.get(ref), args));
        this.vals.put(ref, ret);
        return ret;
    }

    /*
     * //for test static CyclicBarrier barrier; static ArrayList<Ref> items;
     * 
     * public static void main(String[] args){ try { if(args.length != 4)
     * System.
     * err.println("Usage: LockingTransaction nthreads nitems niters ninstances"
     * ); int nthreads = Integer.parseInt(args[0]); int nitems =
     * Integer.parseInt(args[1]); int niters = Integer.parseInt(args[2]); int
     * ninstances = Integer.parseInt(args[3]);
     * 
     * if(items == null) { ArrayList<Ref> temp = new ArrayList(nitems); for(int
     * i = 0; i < nitems; i++) temp.add(new Ref(0)); items = temp; }
     * 
     * class Incr extends AFn{ public Object invoke(Object arg1) { Integer i =
     * (Integer) arg1; return i + 1; }
     * 
     * public Obj withMeta(IPersistentMap meta){ throw new
     * UnsupportedOperationException();
     * 
     * } }
     * 
     * class Commuter extends AFn implements Callable{ int niters; List<Ref>
     * items; Incr incr;
     * 
     * 
     * public Commuter(int niters, List<Ref> items){ this.niters = niters;
     * this.items = items; this.incr = new Incr(); }
     * 
     * public Object call() { long nanos = 0; for(int i = 0; i < niters; i++) {
     * long start = System.nanoTime();
     * LockingTransaction.runInTransaction(this); nanos += System.nanoTime() -
     * start; } return nanos; }
     * 
     * public Object invoke() { for(Ref tref : items) {
     * LockingTransaction.getEx().doCommute(tref, incr); } return null; }
     * 
     * public Obj withMeta(IPersistentMap meta){ throw new
     * UnsupportedOperationException();
     * 
     * } }
     * 
     * class Incrementer extends AFn implements Callable{ int niters; List<Ref>
     * items;
     * 
     * 
     * public Incrementer(int niters, List<Ref> items){ this.niters = niters;
     * this.items = items; }
     * 
     * public Object call() { long nanos = 0; for(int i = 0; i < niters; i++) {
     * long start = System.nanoTime();
     * LockingTransaction.runInTransaction(this); nanos += System.nanoTime() -
     * start; } return nanos; }
     * 
     * public Object invoke() { for(Ref tref : items) {
     * //Transaction.get().doTouch(tref); // LockingTransaction t =
     * LockingTransaction.getEx(); // int val = (Integer) t.doGet(tref); //
     * t.doSet(tref, val + 1); int val = (Integer) tref.get(); tref.set(val +
     * 1); } return null; }
     * 
     * public Obj withMeta(IPersistentMap meta){ throw new
     * UnsupportedOperationException();
     * 
     * } }
     * 
     * ArrayList<Callable<Long>> tasks = new ArrayList(nthreads); for(int i = 0;
     * i < nthreads; i++) { ArrayList<Ref> si; synchronized(items) { si =
     * (ArrayList<Ref>) items.clone(); } Collections.shuffle(si); tasks.add(new
     * Incrementer(niters, si)); //tasks.add(new Commuter(niters, si)); }
     * ExecutorService e = Executors.newFixedThreadPool(nthreads);
     * 
     * if(barrier == null) barrier = new CyclicBarrier(ninstances);
     * System.out.println("waiting for other instances..."); barrier.await();
     * System.out.println("starting"); long start = System.nanoTime();
     * List<Future<Long>> results = e.invokeAll(tasks); long estimatedTime =
     * System.nanoTime() - start;
     * System.out.printf("nthreads: %d, nitems: %d, niters: %d, time: %d%n",
     * nthreads, nitems, niters, estimatedTime / 1000000); e.shutdown();
     * for(Future<Long> result : results) { System.out.printf("%d, ",
     * result.get() / 1000000); } System.out.println();
     * System.out.println("waiting for other instances..."); barrier.await();
     * synchronized(items) { for(Ref item : items) { System.out.printf("%d, ",
     * (Integer) item.currentVal()); } } System.out.println("\ndone");
     * System.out.flush(); } catch(Exception ex) { ex.printStackTrace(); } }
     */
}
