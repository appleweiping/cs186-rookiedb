package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        if (readonly) {
            throw new UnsupportedOperationException("Cannot acquire on a readonly context");
        }
        // Acquiring NL is meaningless — callers should release instead.
        if (lockType == LockType.NL) {
            throw new InvalidLockException("Cannot acquire an NL lock; use release instead");
        }
        // The parent must hold a lock that permits this child lock; otherwise
        // acquiring here would leave the lock manager in an invalid state.
        if (parent != null) {
            LockType parentType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentType, lockType)) {
                throw new InvalidLockException(
                        "Parent lock " + parentType + " cannot be parent of " + lockType);
            }
        }
        lockman.acquire(transaction, name, lockType);
        // Record that the parent now has one more locked child.
        if (parent != null) {
            parent.incrementNumChildLocks(transaction);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Cannot release on a readonly context");
        }
        // Cannot release a lock while the transaction still holds locks on
        // children of this context (would violate multigranularity).
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException(
                    "Cannot release " + name + " while it has child locks");
        }
        lockman.release(transaction, name);
        if (parent != null) {
            parent.decrementNumChildLocks(transaction);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Cannot promote on a readonly context");
        }
        LockType current = getExplicitLockType(transaction);
        if (current == LockType.NL) {
            throw new NoLockHeldException("No lock held on " + name);
        }
        if (current == newLockType) {
            throw new DuplicateLockRequestException(
                    "Already holds a " + newLockType + " lock on " + name);
        }

        // A transaction cannot hold SIX beneath a SIX ancestor (it would be
        // redundant and violates the "no S/IS under SIX" invariant).
        if (newLockType == LockType.SIX && hasSIXAncestor(transaction)) {
            throw new InvalidLockException(
                    "Cannot promote to SIX with a SIX ancestor");
        }

        if (newLockType == LockType.SIX &&
                (current == LockType.IS || current == LockType.IX || current == LockType.S)) {
            // Special SIX promotion: atomically release all S/IS descendant
            // locks (they're subsumed by SIX) along with the current lock.
            List<ResourceName> sisDescendants = sisDescendants(transaction);
            List<ResourceName> releaseNames = new ArrayList<>(sisDescendants);
            releaseNames.add(name); // release-and-reacquire the lock on `name`
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
            // Update child-lock bookkeeping for each released descendant.
            for (ResourceName descendant : sisDescendants) {
                LockContext ctx = fromResourceName(lockman, descendant);
                if (ctx.parent != null) {
                    ctx.parent.decrementNumChildLocks(transaction);
                }
            }
        } else {
            // Ordinary promotion; validity is enforced by LockManager.promote.
            if (!LockType.substitutable(newLockType, current)) {
                throw new InvalidLockException(
                        newLockType + " is not a valid promotion of " + current);
            }
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("Cannot escalate on a readonly context");
        }
        LockType current = getExplicitLockType(transaction);
        if (current == LockType.NL) {
            throw new NoLockHeldException("No lock held on " + name);
        }

        // Gather this context's lock plus all descendant locks held by the
        // transaction. Decide whether we need S or X at this level: X if any
        // lock in the subtree requires write access (X/IX/SIX), else S.
        List<ResourceName> toRelease = new ArrayList<>();
        boolean needsExclusive = current == LockType.X || current == LockType.IX
                || current == LockType.SIX;

        for (Lock lock : lockman.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name)) {
                toRelease.add(lock.name);
                if (lock.lockType == LockType.X || lock.lockType == LockType.IX
                        || lock.lockType == LockType.SIX) {
                    needsExclusive = true;
                }
            }
        }

        LockType newType = needsExclusive ? LockType.X : LockType.S;

        // If we already hold exactly the escalated lock and have no descendant
        // locks, escalation is a no-op (avoid a redundant mutating call).
        if (toRelease.isEmpty() && current == newType) {
            return;
        }

        // Release descendants and this lock, re-acquiring the coarser lock in a
        // single atomic operation.
        toRelease.add(name);
        lockman.acquireAndRelease(transaction, name, newType, toRelease);

        // Update child-lock counts: every released descendant lock removed a
        // child from its parent context.
        for (ResourceName released : toRelease) {
            if (released.equals(name)) continue;
            LockContext ctx = fromResourceName(lockman, released);
            if (ctx.parent != null) {
                ctx.parent.decrementNumChildLocks(transaction);
            }
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // The lock explicitly held at this exact resource level.
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // An explicit lock here takes precedence.
        LockType explicit = getExplicitLockType(transaction);
        if (explicit != LockType.NL) return explicit;
        if (parent == null) return LockType.NL;

        // Otherwise the effective lock is implied by an ancestor. An ancestor's
        // S or X grants the same at this level; SIX grants an implicit S here.
        // Intent locks (IS/IX) grant nothing concrete at this level.
        LockType parentEffective = parent.getEffectiveLockType(transaction);
        if (parentEffective == LockType.S || parentEffective == LockType.X) {
            return parentEffective;
        }
        if (parentEffective == LockType.SIX) {
            return LockType.S;
        }
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // Walk up the ancestry looking for an explicit SIX lock.
        LockContext ctx = parent;
        while (ctx != null) {
            if (ctx.getExplicitLockType(transaction) == LockType.SIX) return true;
            ctx = ctx.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // All S/IS locks the transaction holds on descendants of this context.
        List<ResourceName> result = new ArrayList<>();
        for (Lock lock : lockman.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name) &&
                    (lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                result.add(lock.name);
            }
        }
        return result;
    }

    /** Increments the count of locked children for `transaction` on this context. */
    private void incrementNumChildLocks(TransactionContext transaction) {
        long t = transaction.getTransNum();
        numChildLocks.put(t, numChildLocks.getOrDefault(t, 0) + 1);
    }

    /** Decrements the count of locked children for `transaction` on this context. */
    private void decrementNumChildLocks(TransactionContext transaction) {
        long t = transaction.getTransNum();
        numChildLocks.put(t, Math.max(0, numChildLocks.getOrDefault(t, 0) - 1));
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

