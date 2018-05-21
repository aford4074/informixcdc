import sys, traceback

from _informixcdc import *

from _informixcdc import InformixCdc as _InformixCdc

class InformixCdc(_InformixCdc):
    __doc__ = _InformixCdc.__doc__

def memleak_tests():
    from time import sleep

    cdc = InformixCdc('informix_1', timeout=10, max_records=256)
    print 'connect()', cdc.connect('informix', 'informix')
    print 'is_connected', cdc.is_connected
    print 'session_id', cdc.session_id
    print 'enable()', cdc.enable("stocks:informix.cdc_test", "field1,field2")
    print 'activate()', cdc.activate()

    from collections import defaultdict
    from gc import get_objects
    before = defaultdict(int)
    for i in get_objects():
        before[type(i)] += 1

    x = 0

    for record in cdc:
        #print record.__repr__()
        x += 1
        if x % 100000 == 0:
            after = defaultdict(int)
            for i in get_objects():
                after[type(i)] += 1
            print [(k, after[k] - before[k]) for k in after if after[k] - before[k]]
            print locals()
            print globals()
            break

def main():
    cdc = InformixCdc('informix_1', timeout=5, max_records=2)

    print 'connect()', cdc.connect('informix', 'informix')
    print 'is_connected', cdc.is_connected
    print 'session_id', cdc.session_id
    print 'enable()', cdc.enable("stocks:informix.cdc_test", "field1,field2")
    print 'enable()', cdc.enable("stocks:informix.cdc_test2", "a,b")
    print 'enable()', cdc.enable("informixcdc_test:informix.informixcdc_test",
                                 "cdc_serial8, cdc_bigint_low, cdc_bigint_high,"
                                 "cdc_char, cdc_date, cdc_datetime, cdc_decimal_low,"
                                 "cdc_decimal_high, cdc_float_low, cdc_float_high,"
                                 "cdc_integer_low, cdc_integer_high, cdc_smallfloat_low,"
                                 "cdc_smallfloat_high, cdc_smallint_low, cdc_smallint_high,"
                                 "cdc_varchar, cdc_lvarchar")
    print 'activate()', cdc.activate()

    try:
        for record in cdc:
            print record.__repr__()
    except:
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60

if __name__ == '__main__':
    main()
