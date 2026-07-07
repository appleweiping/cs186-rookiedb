package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // Standard multigranularity compatibility matrix. NL is compatible with
        // everything. The rest follow from what each lock permits:
        //   - IS conflicts only with X.
        //   - IX conflicts with S, SIX, and X (any lock that assumes a stable
        //     view or exclusive access to descendants).
        //   - S conflicts with IX, SIX, and X.
        //   - SIX conflicts with everything except IS (and NL).
        //   - X conflicts with everything except NL.
        if (a == NL || b == NL) return true;
        switch (a) {
            case IS:  return b != X;
            case IX:  return b == IS || b == IX;
            case S:   return b == IS || b == S;
            case SIX: return b == IS;
            case X:   return false;
            default:  throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // A child holding NL needs nothing from its parent.
        if (childLockType == NL) return true;
        // To hold an actual (S/IS) lock on a child, the parent must hold at
        // least an IS. To hold an X-flavored (X/IX/SIX) lock on a child, the
        // parent must hold an IX or SIX.
        switch (parentLockType) {
            case IS:  return childLockType == IS || childLockType == S;
            case IX:  return true;   // IX permits any child lock
            case SIX: return childLockType == X || childLockType == IX || childLockType == SIX;
            default:  return false;  // S, X, NL cannot be parents of a real lock
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // `substitute` can stand in for `required` if it grants at least all the
        // capabilities `required` does.
        switch (required) {
            case NL:  return true;                       // anything covers NL
            case IS:  return substitute != NL;           // any real lock covers IS
            case IX:  return substitute == IX || substitute == SIX || substitute == X;
            case S:   return substitute == S || substitute == SIX || substitute == X;
            // SIX = S + IX. Only SIX (or X, which permits everything on this
            // node) can substitute; note X does not allow IX-style child intent,
            // but on the node itself X is at least as permissive as SIX.
            case SIX: return substitute == SIX || substitute == X;
            case X:   return substitute == X;
            default:  throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

