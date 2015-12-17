package com.spotify.scio.values

import java.lang.{Long => JLong}

import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.spotify.scio.ScioContext
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliValueSampler, PoissonValueSampler}
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}

import scala.reflect.ClassTag

// scalastyle:off number.of.methods
/**
 * Extra functions available on SCollections of (key, value) pairs through an implicit conversion.
 * @groupname cogroup CoGroup Operations
 * @groupname join Join Operations
 * @groupname per_key Per Key Aggregations
 * @groupname transform Transformations
 * @groupname Ungrouped Other Members
 */
class PairSCollectionFunctions[K, V](val self: SCollection[(K, V)])
                                    (implicit ctKey: ClassTag[K], ctValue: ClassTag[V]) {

  import TupleFunctions._

  private val context: ScioContext = self.context

  private def toKvTransform = ParDo.of(Functions.mapFn[(K, V), KV[K, V]](kv => KV.of(kv._1, kv._2)))

  private[scio] def toKV: SCollection[KV[K, V]] = {
    val o = self.applyInternal(toKvTransform).setCoder(self.getKvCoder[K, V])
    context.wrap(o)
  }

  private def applyPerKey[UI: ClassTag, UO: ClassTag](t: PTransform[PCollection[KV[K, V]], PCollection[KV[K, UI]]],
                                                      f: KV[K, UI] => (K, UO))
  : SCollection[(K, UO)] = {
    val o = self.applyInternal(new PTransform[PCollection[(K, V)], PCollection[(K, UO)]]() {
      override def apply(input: PCollection[(K, V)]): PCollection[(K, UO)] =
        input
          .apply("TupleToKv", toKvTransform)
          .setCoder(self.getKvCoder[K, V])
          .apply(t)
          .apply("KvToTuple", ParDo.of(Functions.mapFn[KV[K, UI], (K, UO)](f)))
          .setCoder(self.getCoder[(K, UO)])
    })
    context.wrap(o)
  }

  // =======================================================================
  // CoGroups
  // =======================================================================

  /**
   * For each key k in `this` or `that`, return a resulting SCollection that contains a tuple with
   * the list of values for that key in `this` as well as `that`.
   * @group cogroup
   */
  def coGroup[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] =
    MultiJoin.coGroup(self, that)

  /**
   * For each key k in `this` or `that1` or `that2`, return a resulting SCollection that contains
   * a tuple with the list of values for that key in `this`, `that1` and `that2`.
   * @group cogroup
   */
  def coGroup[W1: ClassTag, W2: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    MultiJoin.coGroup(self, that1, that2)

  /**
   * For each key k in `this` or `that1` or `that2` or `that3`, return a resulting SCollection
   * that contains a tuple with the list of values for that key in `this`, `that1`, `that2` and
   * `that3`.
   * @group cogroup
   */
  def coGroup[W1: ClassTag, W2: ClassTag, W3: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    MultiJoin.coGroup(self, that1, that2, that3)

  /**
   * Alias for cogroup.
   * @group cogroup
   */
  def groupWith[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] =
    this.coGroup(that)

  /**
   * Alias for cogroup.
   * @group cogroup
   */
  def groupWith[W1: ClassTag, W2: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    this.coGroup(that1, that2)

  /**
   * Alias for cogroup.
   * @group cogroup
   */
  def groupWith[W1: ClassTag, W2: ClassTag, W3: ClassTag]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    this.coGroup(that1, that2, that3)

  // =======================================================================
  // Joins
  // =======================================================================

  /**
   * Perform a full outer join of `this` and `that`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), Some(w))) for w in `that`,
   * or the pair (k, (Some(v), None)) if no elements in `that` have key k. Similarly, for each
   * element (k, w) in `that`, the resulting SCollection will either contain all pairs (k,
   * (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements in
   * `this` have key k. Uses the given Partitioner to partition the output SCollection.
   * @group join
   */
  def fullOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Option[V], Option[W]))] =
    MultiJoin.outer(self, that)

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `this` and
   * `that`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in
   * `this` and (k, v2) is in `that`. Uses the given Partitioner to partition the output RDD.
   * @group join
   */
  def join[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, W))] =
    MultiJoin(self, that)

  /**
   * Perform a left outer join of `this` and `that`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (v, Some(w))) for w in `that`, or the
   * pair (k, (v, None)) if no elements in `that` have key k. Uses the given Partitioner to
   * partition the output SCollection.
   * @group join
   */
  def leftOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] =
    MultiJoin.left(self, that)

  /**
   * Perform a right outer join of `this` and `that`. For each element (k, w) in `that`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output SCollection.
   * @group join
   */
  def rightOuterJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Option[V], W))] =
    this.coGroup(that).flatMap { t =>
      for {
        v <- if (t._2._1.isEmpty) Iterable(None) else t._2._1.map(Option(_))
        w <- t._2._2
      } yield (t._1, (v, w))
    }

  // =======================================================================
  // Transformations
  // =======================================================================

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this
   * SCollection, V. Thus, we need one operation for merging a V into a U and one operation for
   * merging two U's. To avoid memory allocation, both of these functions are allowed to modify
   * and return their first argument instead of creating a new U.
   * @group per_key
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): SCollection[(K, U)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(zeroValue)(seqOp, combOp)), kvToTuple[K, U])

  /**
   * Aggregate the values of each key with [[com.twitter.algebird.Aggregator Aggregator]]. First
   * each value V is mapped to A, then we reduce with a semigroup of A, then finally we present
   * the results as U. This could be more powerful and better optimized in some cases.
   * @group per_key
   */
  def aggregateByKey[A: ClassTag, U: ClassTag](aggregator: Aggregator[V, A, U]): SCollection[(K, U)] =
    this.mapValues(aggregator.prepare).sumByKey(aggregator.semigroup).mapValues(aggregator.present)

  /**
   * For each key, compute the values' data distribution using approximate `N`-tiles.
   * @return a new SCollection whose values are Iterables of the approximate `N`-tiles of
   * the elements.
   * @group per_key
   */
  def approxQuantilesByKey(numQuantiles: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(ApproximateQuantiles.perKey(numQuantiles, ord), kvListToTuple[K, V])

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an SCollection[(K, V)] into a result of type SCollection[(K, C)], for a
   * "combined type" C Note that V and C can be different -- for example, one might group an
   * SCollection of type (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three
   * functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two C's into a single one.
   * @group per_key
   */
  def combineByKey[C: ClassTag](createCombiner: V => C)
                               (mergeValue: (C, V) => C)
                               (mergeCombiners: (C, C) => C): SCollection[(K, C)] =
    this.applyPerKey(Combine.perKey(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)), kvToTuple[K, C])

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param sampleSize the number of entries in the statisticalsample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`.
   * @group per_key
   */
  def countApproxDistinctByKey(sampleSize: Int): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](sampleSize), kvToTuple[K, JLong])
      .asInstanceOf[SCollection[(K, Long)]]

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`.
   * @group per_key
   */
  def countApproxDistinctByKey(maximumEstimationError: Double = 0.02): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](maximumEstimationError), kvToTuple[K, JLong])
      .asInstanceOf[SCollection[(K, Long)]]

  /**
   * Count the number of elements for each key.
   * @return a new SCollection of (key, count) pairs
   * @group per_key
   */
  def countByKey(): SCollection[(K, Long)] = this.keys.countByValue()

  /**
   * Pass each value in the key-value pair SCollection through a flatMap function without changing
   * the keys.
   * @group transform
   */
  def flatMapValues[U: ClassTag](f: V => TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMap(kv => f(kv._2).map(v => (kv._1, v)))

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   * @group per_key
   */
  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(zeroValue)(op, op)), kvToTuple[K, V])

  /**
   * Fold by key with [[com.twitter.algebird.Monoid Monoid]], which defines the associative
   * function and "zero value" for V. This could be more powerful and better optimized in some
   * cases.
   * @group per_key
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(mon)), kvToTuple[K, V])

  /**
   * Group the values for each key in the SCollection into a single sequence. The ordering of
   * elements within each group is not guaranteed, and may even differ each time the resulting
   * SCollection is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using
   * [[PairSCollectionFunctions.aggregateByKey[U]* PairSCollectionFunctions.aggregateByKey]] or
   * [[PairSCollectionFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for
   * any key in memory. If a key has too many values, it can result in an OutOfMemoryError.
   * @group per_key
   */
  def groupByKey(): SCollection[(K, Iterable[V])] =
    this.applyPerKey(GroupByKey.create[K, V](), kvIterableToTuple[K, V])

  /**
   * Return an SCollection with the keys of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Keys
  def keys: SCollection[K] = self.map(_._1)

  /**
   * Pass each value in the key-value pair SCollection through a map function without changing the
   * keys.
   * @group transform
   */
  def mapValues[U: ClassTag](f: V => U): SCollection[(K, U)] = self.map(kv => (kv._1, f(kv._2)))

  /**
   * Return the max of values for each key as defined by the implicit Ordering[T].
   * @return a new SCollection of (key, maximum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def maxByKey(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.max)

  /**
   * Return the min of values for each key as defined by the implicit Ordering[T].
   * @return a new SCollection of (key, minimum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def minByKey(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.min)

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   * @group per_key
   */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(op)), kvToTuple[K, V])

  /**
   * Return a sampled subset of values for each key of this SCollection.
   * @return a new SCollection of (key, sampled values) pairs
   * @group per_key
   */
  def sampleByKey(sampleSize: Int): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Sample.fixedSizePerKey[K, V](sampleSize), kvIterableToTuple[K, V])

  /**
   * Return a subset of this SCollection sampled by key (via stratified sampling).
   *
   * Create a sample of this SCollection using variable sampling rates for different keys as
   * specified by `fractions`, a key to sampling rate map, via simple random sampling with one
   * pass over the SCollection, to produce a sample of size that's approximately equal to the sum
   * of math.ceil(numItems * samplingRate) over all key values.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @return SCollection containing the sampled subset
   * @group per_key
   */
  def sampleByKey(withReplacement: Boolean, fractions: Map[K, Double]): SCollection[(K, V)] = {
    if (withReplacement) {
      self.parDo(new PoissonValueSampler[K, V](fractions))
    } else {
      self.parDo(new BernoulliValueSampler[K, V](fractions))
    }
  }

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in `other`.
   * @group per_key
   */
  def subtractByKey[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, V)] =
    this.coGroup(that).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) t._2._1.map((t._1, _)) else  Seq.empty
    }

  /**
   * Reduce by key with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful
   * and better optimized in some cases.
   * @group per_key
   */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(sg)), kvToTuple[K, V])

  /**
   * Swap the keys with the values.
   * @group transform
   */
  // Scala lambda is simpler than transforms.KvSwap
  def swap: SCollection[(V, K)] = self.map(kv => (kv._2, kv._1))

  /**
   * Return the top k (largest) values for each key from this SCollection as defined by the
   * specified implicit Ordering[T].
   * @return a new SCollection of (key, top k) pairs
   * @group per_key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Top.perKey[K, V, Ordering[V]](num, ord), kvListToTuple[K, V])

  /**
   * Return an SCollection with the values of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Values
  def values: SCollection[V] = self.map(_._2)

  /* Hash operations */

  /**
   * Perform an inner join by replicating `that` to all workers. The right side should be tiny and
   * fit in memory.
   * @group transform
   */
  def hashJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, W))] = {
    val side = that.asMultiMapSideInput
    self.withSideInputs(side).flatMap[(K, (V, W))] { (kv, s) =>
      s(side).getOrElse(kv._1, Iterable()).toSeq.map(w => (kv._1, (kv._2, w)))
    }.toSCollection
  }

  /**
   * Perform a left outer join by replicating `that` to all workers. The right side should be tiny
   * and fit in memory.
   * @group transform
   */
  def hashLeftJoin[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] = {
    val side = that.asMultiMapSideInput
    self.withSideInputs(side).flatMap[(K, (V, Option[W]))] { (kv, s) =>
      val (k, v) = kv
      val m = s(side)
      if (m.contains(k)) m(k).map(w => (k, (v, Some(w)))) else Seq((k, (v, None)))
    }.toSCollection
  }

  // =======================================================================
  // Side input operations
  // =======================================================================

  /**
   * Convert this SCollection of (key, value) to a SideInput of Map[key, value], to be used with
   * [[SCollection.withSideInputs]]. It is required that each key of the input be associated with
   * a single value.
   */
  def asMapSideInput: SideInput[Map[K, V]] = new MapSideInput[K, V](self.toKV.applyInternal(View.asMap()))

  /**
   * Convert this SCollection of (key, value) to a SideInput of Map[key, Iterable[value]], to be
   * used with [[SCollection.withSideInputs]].
   */
  def asMultiMapSideInput: SideInput[Map[K, Iterable[V]]] =
    new MultiMapSideInput[K, V](self.toKV.applyInternal(View.asMultimap()))

}
// scalastyle:on number.of.methods
