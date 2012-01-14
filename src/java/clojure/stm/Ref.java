/**
 *   Copyright (c) Rich Hickey. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

/* rich Jul 25, 2007 */

package clojure.stm;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IRef;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Util;


public class Ref extends ARef implements IFn, Comparable<Ref>, IRef {
    @Override
    public int compareTo(final Ref ref) {
        if (this.id == ref.id) {
            return 0;
        }
        else if (this.id < ref.id) {
            return -1;
        }
        else {
            return 1;
        }
    }


    public int getMinHistory() {
        return this.minHistory;
    }


    public Ref setMinHistory(final int minHistory) {
        this.minHistory = minHistory;
        return this;
    }


    public int getMaxHistory() {
        return this.maxHistory;
    }


    public Ref setMaxHistory(final int maxHistory) {
        this.maxHistory = maxHistory;
        return this;
    }

    public static class TVal {
        Object val;
        long point;
        long msecs;
        TVal prior;
        TVal next;


        TVal(final Object val, final long point, final long msecs, final TVal prior) {
            this.val = val;
            this.point = point;
            this.msecs = msecs;
            this.prior = prior;
            this.next = prior.next;
            this.prior.next = this;
            this.next.prior = this;
        }


        TVal(final Object val, final long point, final long msecs) {
            this.val = val;
            this.point = point;
            this.msecs = msecs;
            this.next = this;
            this.prior = this;
        }

    }

    TVal tvals;
    final AtomicInteger faults;
    final ReentrantReadWriteLock lock;
    LockingTransaction.Info tinfo;
    // IFn validator;
    final long id;

    volatile int minHistory = 0;
    volatile int maxHistory = 10;

    static final AtomicLong ids = new AtomicLong();


    public Ref(final Object initVal) {
        this(initVal, null);
    }


    public Ref(final Object initVal, final IPersistentMap meta) {
        super(meta);
        this.id = ids.getAndIncrement();
        this.faults = new AtomicInteger();
        this.lock = new ReentrantReadWriteLock();
        this.tvals = new TVal(initVal, 0, System.currentTimeMillis());
    }


    // the latest val

    // ok out of transaction
    Object currentVal() {
        try {
            this.lock.readLock().lock();
            if (this.tvals != null) {
                return this.tvals.val;
            }
            throw new IllegalStateException(this.toString() + " is unbound.");
        }
        finally {
            this.lock.readLock().unlock();
        }
    }


    // *

    @Override
    public Object deref() {
        final LockingTransaction t = LockingTransaction.getRunning();
        if (t == null) {
            return this.currentVal();
        }
        return t.doGet(this);
    }


    // void validate(IFn vf, Object val){
    // try{
    // if(vf != null && !RT.booleanCast(vf.invoke(val)))
    // throw new IllegalStateException("Invalid ref state");
    // }
    // catch(RuntimeException re)
    // {
    // throw re;
    // }
    // catch(Exception e)
    // {
    // throw new IllegalStateException("Invalid ref state", e);
    // }
    // }
    //
    // public void setValidator(IFn vf){
    // try
    // {
    // lock.writeLock().lock();
    // validate(vf,currentVal());
    // validator = vf;
    // }
    // finally
    // {
    // lock.writeLock().unlock();
    // }
    // }
    //
    // public IFn getValidator(){
    // try
    // {
    // lock.readLock().lock();
    // return validator;
    // }
    // finally
    // {
    // lock.readLock().unlock();
    // }
    // }

    public Object set(final Object val) {
        return LockingTransaction.getEx().doSet(this, val);
    }


    public Object commute(final IFn fn, final ISeq args) {
        return LockingTransaction.getEx().doCommute(this, fn, args);
    }


    public Object alter(final IFn fn, final ISeq args) {
        final LockingTransaction t = LockingTransaction.getEx();
        return t.doSet(this, fn.applyTo(RT.cons(t.doGet(this), args)));
    }


    public void touch() {
        LockingTransaction.getEx().doEnsure(this);
    }


    // */
    boolean isBound() {
        try {
            this.lock.readLock().lock();
            return this.tvals != null;
        }
        finally {
            this.lock.readLock().unlock();
        }
    }


    public void trimHistory() {
        try {
            this.lock.writeLock().lock();
            if (this.tvals != null) {
                this.tvals.next = this.tvals;
                this.tvals.prior = this.tvals;
            }
        }
        finally {
            this.lock.writeLock().unlock();
        }
    }


    public int getHistoryCount() {
        try {
            this.lock.writeLock().lock();
            return this.histCount();
        }
        finally {
            this.lock.writeLock().unlock();
        }
    }


    int histCount() {
        if (this.tvals == null) {
            return 0;
        }
        else {
            int count = 0;
            for (TVal tv = this.tvals.next; tv != this.tvals; tv = tv.next) {
                count++;
            }
            return count;
        }
    }


    final public IFn fn() {
        return (IFn) this.deref();
    }


    @Override
    public Object call() {
        return this.invoke();
    }


    @Override
    public void run() {
        try {
            this.invoke();
        }
        catch (final Exception e) {
            throw Util.runtimeException(e);
        }
    }


    @Override
    public Object invoke() {
        return this.fn().invoke();
    }


    @Override
    public Object invoke(final Object arg1) {
        return this.fn().invoke(arg1);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2) {
        return this.fn().invoke(arg1, arg2);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3) {
        return this.fn().invoke(arg1, arg2, arg3);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4) {
        return this.fn().invoke(arg1, arg2, arg3, arg4);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14) {
        return this.fn()
            .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
            final Object arg16) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
            final Object arg16, final Object arg17) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
            final Object arg16, final Object arg17, final Object arg18) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17, arg18);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
            final Object arg16, final Object arg17, final Object arg18, final Object arg19) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17, arg18, arg19);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
            final Object arg16, final Object arg17, final Object arg18, final Object arg19, final Object arg20) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17, arg18, arg19, arg20);
    }


    @Override
    public Object invoke(final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
            final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
            final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
            final Object arg16, final Object arg17, final Object arg18, final Object arg19, final Object arg20,
            final Object... args) {
        return this.fn().invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17, arg18, arg19, arg20, args);
    }


    @Override
    public Object applyTo(final ISeq arglist) {
        return AFn.applyToHelper(this, arglist);
    }

}
