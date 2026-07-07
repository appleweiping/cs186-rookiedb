package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // Nothing to do for NL, or if the lock we effectively hold already
        // covers the requested access.
        if (requestType == LockType.NL) return;
        if (LockType.substitutable(effectiveLockType, requestType)) return;

        // Ensure every ancestor holds the appropriate intent lock (IS for an S
        // request, IX for an X request) before touching this level.
        LockType requiredParent = LockType.parentLock(requestType);
        ensureAncestorLocks(parentContext, transaction, requiredParent);

        if (explicitLockType == LockType.NL) {
            // Nothing here yet: acquire the requested lock directly.
            lockContext.acquire(transaction, requestType);
        } else if (requestType == LockType.S && explicitLockType == LockType.IX) {
            // We hold IX but need S at this level: SIX gives us both.
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            // We hold an intent lock (IS/IX/SIX) but need a concrete S/X here:
            // escalate to gather descendant locks into a single S or X lock.
            lockContext.escalate(transaction);
            // Escalation may have produced S when we need X; promote if needed.
            if (!LockType.substitutable(lockContext.getExplicitLockType(transaction), requestType)) {
                lockContext.promote(transaction, requestType);
            }
        } else {
            // We hold a plain S (and need X): promote to X.
            lockContext.promote(transaction, requestType);
        }
    }

    /**
     * Ensures that `transaction` holds at least `requiredType` (an intent lock,
     * IS or IX) on `context` and, recursively, the appropriate intent locks on
     * all of its ancestors. Acquires or promotes as needed.
     */
    private static void ensureAncestorLocks(LockContext context,
                                            TransactionContext transaction,
                                            LockType requiredType) {
        if (context == null || requiredType == LockType.NL) return;

        // First make sure our parent has the intent lock it needs for us.
        ensureAncestorLocks(context.parentContext(), transaction,
                LockType.parentLock(requiredType));

        LockType current = context.getExplicitLockType(transaction);
        if (LockType.substitutable(current, requiredType)) {
            // Already sufficient (e.g. we hold IX and only need IS).
            return;
        }
        if (current == LockType.NL) {
            context.acquire(transaction, requiredType);
        } else if (current == LockType.S && requiredType == LockType.IX) {
            // Holding S but need IX for a descendant write: SIX covers both.
            context.promote(transaction, LockType.SIX);
        } else {
            context.promote(transaction, requiredType);
        }
    }
}
