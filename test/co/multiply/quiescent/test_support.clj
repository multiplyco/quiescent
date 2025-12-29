(ns co.multiply.quiescent.test-support
  (:require
    [co.multiply.quiescent :as q]))


(defn platform-thread-fixture
  "Fixture that allows dereferencing Tasks on platform threads.
   Tests run on platform threads, but Quiescent's MachineLatch
   normally throws when awaited from non-virtual threads.

   Use with: (use-fixtures :once platform-thread-fixture)"
  [f]
  (q/throw-on-platform-park! false)
  (try (f)
       (finally
         (q/throw-on-platform-park! true))))
