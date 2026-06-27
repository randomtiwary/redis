start_server {
    tags {"timeseries"}
} {
    test {TS.SET and TS.GET basics} {
        r del ts1
        assert_equal {OK} [r ts.set ts1 1000 1.5]
        assert_equal {1.5} [r ts.get ts1 1000]
    }

    test {TS.SET with labels and TS.GET returns value and labels} {
        r del ts2
        assert_equal {OK} [r ts.set ts2 2000 3.25 {host=a region=us}]
        set res [r ts.get ts2 2000]
        assert_equal {3.25} [lindex $res 0]
        assert_equal {host=a region=us} [lindex $res 1]
    }

    test {TS.SET overwrites existing timestamp} {
        r del ts3
        r ts.set ts3 10 1.0
        r ts.set ts3 10 2.0 label-x
        set res [r ts.get ts3 10]
        assert_equal 2 [lindex $res 0]
        assert_equal {label-x} [lindex $res 1]
    }

    test {TS.GET missing key or timestamp returns null} {
        r del ts4
        assert_equal {} [r ts.get ts4 1]
        r ts.set ts4 1 9.0
        assert_equal {} [r ts.get ts4 2]
    }

    test {TS.SET maintains sort order for out-of-order timestamps} {
        r del ts5
        r ts.set ts5 300 3.0
        r ts.set ts5 100 1.0
        r ts.set ts5 200 2.0
        assert_equal 1 [r ts.get ts5 100]
        assert_equal 2 [r ts.get ts5 200]
        assert_equal 3 [r ts.get ts5 300]
    }

    test {TS.RANGE aggregations max min sum avg} {
        r del ts6
        r ts.set ts6 1 10.0
        r ts.set ts6 2 20.0
        r ts.set ts6 3 30.0
        r ts.set ts6 4 40.0
        # range [2,4] => 20,30,40
        assert_equal 40 [r ts.range ts6 2 4 max]
        assert_equal 20 [r ts.range ts6 2 4 min]
        assert_equal 90 [r ts.range ts6 2 4 sum]
        assert_equal 30 [r ts.range ts6 2 4 avg]
    }

    test {TS.RANGE empty range returns null} {
        r del ts7
        r ts.set ts7 5 1.0
        assert_equal {} [r ts.range ts7 1 3 sum]
        assert_equal {} [r ts.range missing 1 3 avg]
    }

    test {TS.RANGE rejects unknown aggregation} {
        r del ts8
        r ts.set ts8 1 1.0
        catch {r ts.range ts8 1 1 foo} err
        assert_match {*unknown aggregation*} $err
    }

    test {TS commands wrong type} {
        r del ts9
        r set ts9 string
        catch {r ts.set ts9 1 1.0} err
        assert_match {*WRONGTYPE*} $err
        catch {r ts.get ts9 1} err
        assert_match {*WRONGTYPE*} $err
        catch {r ts.range ts9 1 2 sum} err
        assert_match {*WRONGTYPE*} $err
    }

    test {TYPE reports timeseries} {
        r del ts10
        r ts.set ts10 1 1.0
        assert_equal {timeseries} [r type ts10]
        assert_equal {timeseries} [r object encoding ts10]
    }

    test {DUMP/RESTORE preserves timeseries with gorilla timestamps} {
        r del ts11
        r ts.set ts11 1000 1.0 a
        r ts.set ts11 1001 2.0 b
        r ts.set ts11 1002 3.0 c
        set payload [r dump ts11]
        r del ts11
        r restore ts11 0 $payload
        assert_equal {1 a} [r ts.get ts11 1000]
        assert_equal {2 b} [r ts.get ts11 1001]
        assert_equal {3 c} [r ts.get ts11 1002]
        assert_equal 6 [r ts.range ts11 1000 1002 sum]
    }

    test {COPY preserves timeseries} {
        r del ts12 ts12copy
        r ts.set ts12 50 5.5 lab
        r copy ts12 ts12copy
        assert_equal {5.5 lab} [r ts.get ts12copy 50]
    }

    test {Regular interval timestamps compress well (gorilla)} {
        r del ts13
        # 100 samples at 60s intervals — Gorilla encodes DoD=0 after the first delta
        for {set i 0} {$i < 100} {incr i} {
            set ts [expr {1000000 + $i * 60}]
            r ts.set ts13 $ts [expr {10.0 + $i}]
        }
        assert_equal 10 [r ts.get ts13 1000000]
        set last [expr {1000000 + 99 * 60}]
        assert_equal 109 [r ts.get ts13 $last]
        # avg of 10..109 = 59.5
        assert_equal 59.5 [r ts.range ts13 1000000 $last avg]
    }
}
