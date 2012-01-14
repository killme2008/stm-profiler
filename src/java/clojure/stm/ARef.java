/**
 *   Copyright (c) Rich Hickey. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

/* rich Jan 1, 2009 */

package clojure.stm;

import java.util.Map;

import clojure.lang.AReference;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IRef;
import clojure.lang.ISeq;
import clojure.lang.PersistentHashMap;
import clojure.lang.RT;
import clojure.lang.Util;


public abstract class ARef extends AReference implements IRef {
    protected volatile IFn validator = null;
    private volatile IPersistentMap watches = PersistentHashMap.EMPTY;


    public ARef() {
        super();
    }


    public ARef(final IPersistentMap meta) {
        super(meta);
    }


    void validate(final IFn vf, final Object val) {
        try {
            if (vf != null && !RT.booleanCast(vf.invoke(val))) {
                throw new IllegalStateException("Invalid reference state");
            }
        }
        catch (final RuntimeException re) {
            throw re;
        }
        catch (final Exception e) {
            throw new IllegalStateException("Invalid reference state", e);
        }
    }


    void validate(final Object val) {
        this.validate(this.validator, val);
    }


    public void setValidator(final IFn vf) {
        try {
            this.validate(vf, this.deref());
        }
        catch (final Exception e) {
            throw Util.runtimeException(e);
        }
        this.validator = vf;
    }


    public IFn getValidator() {
        return this.validator;
    }


    public IPersistentMap getWatches() {
        return this.watches;
    }


    synchronized public IRef addWatch(final Object key, final IFn callback) {
        this.watches = this.watches.assoc(key, callback);
        return this;
    }


    synchronized public IRef removeWatch(final Object key) {
        try {
            this.watches = this.watches.without(key);
        }
        catch (final Exception e) {
            throw Util.runtimeException(e);
        }

        return this;
    }


    public void notifyWatches(final Object oldval, final Object newval) {
        final IPersistentMap ws = this.watches;
        if (ws.count() > 0) {
            for (ISeq s = ws.seq(); s != null; s = s.next()) {
                final Map.Entry e = (Map.Entry) s.first();
                final IFn fn = (IFn) e.getValue();
                try {
                    if (fn != null) {
                        fn.invoke(e.getKey(), this, oldval, newval);
                    }
                }
                catch (final Exception e1) {
                    throw Util.runtimeException(e1);
                }
            }
        }
    }
}
