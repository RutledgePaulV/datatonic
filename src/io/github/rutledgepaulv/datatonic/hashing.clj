(ns io.github.rutledgepaulv.datatonic.hashing
  (:import (clojure.lang IPersistentMap IPersistentSet Keyword MapEntry PersistentTreeMap PersistentTreeSet Sequential)
           (com.google.common.hash Funnel HashCode HashFunction Hashing PrimitiveSink)
           (java.nio.charset Charset)
           (java.util HexFormat)))


(defprotocol Hashable
  (hash* [x ^PrimitiveSink sink context]))

(def utf-8
  (Charset/forName "UTF-8"))

(defn hash-object ^HashCode
  ([x] (hash-object (Hashing/murmur3_128) x))
  ([^HashFunction hash-function x]
   (.hashObject
     hash-function x
     (reify Funnel
       (funnel [this from into]
         (hash* from into {:hash-function hash-function}))))))

(defn hex [hash]
  (.formatHex (HexFormat/of) (.asBytes hash)))

(extend-protocol Hashable
  IPersistentMap
  (hash* [this ^PrimitiveSink sink {:keys [hash-function]}]
    (.putBytes sink (.asBytes (Hashing/combineUnordered (map (partial hash-object hash-function) this)))))

  IPersistentSet
  (hash* [this ^PrimitiveSink sink {:keys [hash-function]}]
    (.putBytes sink (.asBytes (Hashing/combineUnordered (map (partial hash-object hash-function) this)))))

  PersistentTreeMap
  (hash* [this ^PrimitiveSink sink {:keys [hash-function]}]
    (.putBytes sink (.asBytes (Hashing/combineOrdered (map (partial hash-object hash-function) this)))))

  PersistentTreeSet
  (hash* [this ^PrimitiveSink sink {:keys [hash-function]}]
    (.putBytes sink (.asBytes (Hashing/combineOrdered (map (partial hash-object hash-function) this)))))

  Sequential
  (hash* [this ^PrimitiveSink sink {:keys [hash-function]}]
    (.putBytes sink (.asBytes (Hashing/combineOrdered (map (partial hash-object hash-function) this)))))

  Keyword
  (hash* [this ^PrimitiveSink sink _]
    (if (qualified-keyword? this)
      (.putString sink (str (namespace this) "/" (name this)) utf-8)
      (.putString sink (name this) utf-8)))

  Boolean
  (hash* [this ^PrimitiveSink sink _]
    (.putBoolean sink this))

  Short
  (hash* [this ^PrimitiveSink sink _]
    (.putShort sink this))

  Integer
  (hash* [this ^PrimitiveSink sink _]
    (.putInt sink this))

  Long
  (hash* [this ^PrimitiveSink sink _]
    (.putLong sink this))

  Byte
  (hash* [this ^PrimitiveSink sink _]
    (.putByte sink this))

  String
  (hash* [this ^PrimitiveSink sink _]
    (.putString sink this utf-8))

  MapEntry
  (hash* [this ^PrimitiveSink sink {:keys [hash-function]}]
    (let [key-hash (hash-object hash-function (key this))
          val-hash (hash-object hash-function (val this))]
      (.putBytes sink (.asBytes key-hash))
      (.putString sink "___MAP_ENTRY_SEPARATOR___" utf-8)
      (.putBytes sink (.asBytes val-hash)))))


(comment

  (hex (hash-object {:a #{2 3} :4 10}))

  )
