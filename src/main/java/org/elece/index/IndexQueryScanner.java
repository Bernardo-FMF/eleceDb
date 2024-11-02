package org.elece.index;

import org.elece.exception.btree.BTreeException;

import java.util.Iterator;
import java.util.Set;

public interface IndexQueryScanner<K extends Comparable<K>, V> {
    Iterator<V> getGreaterThan(K k, Set<K> kExclusions) throws BTreeException;

    Iterator<V> getGreaterThanEqual(K k, Set<K> kExclusions) throws BTreeException;

    Iterator<V> getLessThan(K k, Set<K> kExclusions) throws BTreeException;

    Iterator<V> getLessThanEqual(K k, Set<K> kExclusions) throws BTreeException;

    Iterator<V> getBetweenRange(K k1, K k2, Set<K> kExclusions) throws BTreeException;
}
