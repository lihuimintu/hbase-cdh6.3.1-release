/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.skiplist.core;

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
abstract class AbstractCCSMap<K, V> implements ICCSMap<K, V> {

  static final int EQ = 1;
  static final int LT = 2;
  static final int GT = 0; // Actually checked as !LT

  protected final ICCSList<K> ccsl;
  protected final IAllocatorHandler allocatorHandler;
  protected final Class<K> keyClass;

  private transient Values<V> values;
  private transient KeySet<K> keySet;
  private transient EntrySet<K, V> entrySet;

  public AbstractCCSMap(AllocatorHandlerRegister.AllocatorHandlerBuilder builder,
      INodeComparator<K> comparator, Class<K> keyClass, SchemaEnum schema) {
    this.allocatorHandler = AllocatorHandlerRegister.getAllocatorHandler();
    if (this.allocatorHandler == null) {
      throw new IllegalStateException("not any allocatorHandler.");
    }
    this.keyClass = keyClass;
    this.ccsl = new CompactedConcurrentSkipList<>(builder, comparator, schema);
  }

  public AbstractCCSMap(ICCSList<K> subCCSL, Class<K> keyClass) {
    this.allocatorHandler = AllocatorHandlerRegister.getAllocatorHandler();
    if (this.allocatorHandler == null) {
      throw new IllegalStateException("not any allocatorHandler.");
    }
    this.keyClass = keyClass;
    this.ccsl = subCCSL;
  }

  @Override
  public V get(Object key) {
    try {
      long node = this.ccsl.get(keyClass.cast(key));
      if (node != NIL_NODE_ID) {
        return doGetValue(node);
      }
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
    return null;
  }

  @Override
  public V remove(Object key) {
    try {
      long node = this.ccsl.remove(keyClass.cast(key));
      if (node != NIL_NODE_ID) {
        return doGetValue(node);
      }
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
    return null;
  }

  @Override
  public Entry<K, V> lowerEntry(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, LT);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetEntry(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public K lowerKey(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, LT);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetKey(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> floorEntry(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, LT | EQ);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetEntry(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public K floorKey(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, LT | EQ);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetKey(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> ceilingEntry(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, GT|EQ);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetEntry(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public K ceilingKey(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, GT|EQ);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetKey(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> higherEntry(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, GT);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetEntry(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public K higherKey(K key) {
    try {
      long nodeId = this.ccsl.findNear(key, GT);
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetKey(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> firstEntry() {
    try {
      long nodeId = this.ccsl.findFirst();
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetEntry(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> lastEntry() {
    try {
      long nodeId = this.ccsl.findLast();
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetEntry(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> pollFirstEntry() {
    try {
      long nodeId = this.ccsl.findFirst();
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      Entry<K, V> snapshot = doGetEntry(nodeId);
      this.ccsl.remove(nodeId);
      return snapshot;
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Entry<K, V> pollLastEntry() {
    try {
      long nodeId = this.ccsl.findLast();
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      Entry<K, V> snapshot = doGetEntry(nodeId);
      this.ccsl.remove(nodeId);
      return snapshot;
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public NavigableMap<K, V> descendingMap() {
    throw new UnsupportedOperationException("CCSMap Not implemented");
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    throw new UnsupportedOperationException("CCSMap Not implemented");
  }

  @Override
  public NavigableSet<K> descendingKeySet() {
    throw new UnsupportedOperationException("CCSMap Not implemented");
  }

  @Override
  public Comparator<? super K> comparator() {
    throw new UnsupportedOperationException("CCSMap Not implemented");
  }

  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  @Override
  public SortedMap<K, V> headMap(K toKey) {
    return headMap(toKey, false);
  }

  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }

  @Override
  public K firstKey() {
    try {
      long nodeId = this.ccsl.findFirst();
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetKey(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public K lastKey() {
    try {
      long nodeId = this.ccsl.findLast();
      if (nodeId == NIL_NODE_ID) {
        return null;
      }
      return doGetKey(nodeId);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int size() {
    return this.ccsl.getSize();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("CCSMap Not implemented");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("CCSMap Not implemented");
  }

  @Override
  public Set<K> keySet() {
    KeySet<K> ks = keySet;
    if (ks != null) {
      return ks;
    }
    keySet = new KeySet<>(this, ccsl);
    return keySet;
  }

  @Override
  public Collection<V> values() {
    Values<V> vs = values;
    if (vs != null) {
      return vs;
    }
    values = new Values<>(this, ccsl);
    return values;
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    EntrySet<K, V> es = entrySet;
    if (es != null) {
      return es;
    }
    entrySet = new EntrySet<>(this, ccsl);
    return entrySet;
  }

  @Override
  public void close() {
    this.ccsl.close();
  }

  @Override
  public CompactedConcurrentSkipList.Stat getCcslStat() {
    return this.ccsl.getCcslStat();
  }

  static final class KeySet<E> extends AbstractSet<E> {
    final AbstractCCSMap<E, ?> m;
    final ICCSList ccsl;

    KeySet(AbstractCCSMap<E, ?> m, ICCSList ccsl) {
      this.m = m;
      this.ccsl = ccsl;
    }

    @Override
    public Iterator<E> iterator() {
      try {
        return m.new KeyIterator(ccsl);
      } catch (CCSMapException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public int size() {
      return m.size();
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      return m.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
      return m.remove(o) != null;
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Set)) {
        return false;
      }
      Collection<?> c = (Collection<?>) o;
      try {
        return containsAll(c) && c.containsAll(this);
      } catch (ClassCastException unused) {
        return false;
      } catch (NullPointerException unused) {
        return false;
      }
    }

    @Override
    public int hashCode() {
      int h = 17;
      Iterator<E> i = iterator();
      while (i.hasNext()) {
        E obj = i.next();
        if (obj != null) {
          h = h * 31 + obj.hashCode();
        }
      }
      return h;
    }

    @Override
    public Object[] toArray() {
      return toList(this).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }

    //default Spliterator
  }

  static final class Values<E> extends AbstractCollection<E> {
    final AbstractCCSMap<?, E> m;
    final ICCSList ccsl;

    Values(AbstractCCSMap<?,E> m, ICCSList ccsl) {
      this.m = m;
      this.ccsl = ccsl;
    }

    @Override
    public Iterator<E> iterator() {
      try {
        return m.new ValueIterator(ccsl);
      } catch (CCSMapException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public int size() {
      return m.size();
    }

    @Override
    public boolean contains(Object o) {
      return m.containsValue(o);
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public Object[] toArray() {
      return toList(this).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
    //default Spliterator
  }

  static final class EntrySet<K1, V1> extends AbstractSet<Entry<K1, V1>> {
    final AbstractCCSMap<K1, V1> m;
    final ICCSList ccsl;

    EntrySet(AbstractCCSMap<K1,V1> m, ICCSList ccsl) {
      this.m = m;
      this.ccsl = ccsl;
    }

    @Override
    public Iterator<Entry<K1, V1>> iterator() {
      try {
        return m.new EntryIterator(ccsl);
      } catch (CCSMapException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      V1 v = m.get(e.getKey());
      return v != null && v.equals(e.getValue());
    }

    @Override
    public boolean remove(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      return m.remove(e.getKey(), e.getValue());
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public int size() {
      return m.size();
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Set)) {
        return false;
      }
      Collection<?> c = (Collection<?>) o;
      try {
        return containsAll(c) && c.containsAll(this);
      } catch (ClassCastException unused) {
        return false;
      } catch (NullPointerException unused) {
        return false;
      }
    }

    @Override
    public int hashCode() {
      int h = 17;
      Iterator<Entry<K1, V1>> i = iterator();
      while (i.hasNext()) {
        Entry<K1, V1> obj = i.next();
        if (obj != null) {
          h = h * 31 + obj.hashCode();
        }
      }
      return h;
    }

    @Override
    public Object[] toArray() {
      return toList(this).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
    //default Spliterator
  }

  abstract static class AbstractIterator<T> implements Iterator<T> {
    private final IIterCCSList iter;

    /**
     * Initializes ascending iterator for entire range.
     */
    AbstractIterator(ICCSList ccsl) throws CCSMapException {
      iter = ccsl.nodeIdIter();
    }

    @Override
    public final boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public T next() {
      try {
        long nextId = iter.next();
        return getObject(nextId);
      } catch (SerdeException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void remove() {
      this.iter.remove();
    }

    abstract T getObject(long nextId) throws SerdeException;
  }

  class KeyIterator extends AbstractIterator<K> {

    /**
     * Initializes ascending iterator for entire range.
     *
     * @param ccsl ccslist
     */
    KeyIterator(ICCSList ccsl) throws CCSMapException {
      super(ccsl);
    }

    @Override
    K getObject(long nodeId) throws SerdeException {
      return doGetKey(nodeId);
    }
  }

  class ValueIterator extends AbstractIterator<V> {

    /**
     * Initializes ascending iterator for entire range.
     *
     * @param ccsl ccslist
     */
    ValueIterator(ICCSList ccsl) throws CCSMapException {
      super(ccsl);
    }

    @Override
    V getObject(long nodeId) throws SerdeException {
      return doGetValue(nodeId);
    }
  }

  class EntryIterator extends AbstractIterator<Map.Entry<K, V>> {

    /**
     * Initializes ascending iterator for entire range.
     *
     * @param ccsl ccslist
     */
    EntryIterator(ICCSList ccsl) throws CCSMapException {
      super(ccsl);
    }

    @Override
    Map.Entry<K, V> getObject(long nodeId) throws SerdeException {
      return doGetEntry(nodeId);
    }
  }

  private static <E> List<E> toList(Collection<E> c) {
    // Using size() here would be a pessimization.
    ArrayList<E> list = new ArrayList<>();
    for (E e : c) {
      list.add(e);
    }
    return list;
  }

  abstract Map.Entry<K, V> doGetEntry(long nodeId) throws SerdeException;

  abstract K doGetKey(long nodeId) throws SerdeException;

  abstract V doGetValue(long nodeId) throws SerdeException;

}
